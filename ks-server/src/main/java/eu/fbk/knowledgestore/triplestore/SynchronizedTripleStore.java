package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.runtime.Synchronizer;

/**
 * A {@code TripleStore} wrapper that synchronizes and enforces a proper access to a wrapped
 * {@code TripleStore}.
 * <p>
 * This wrapper provides the following guarantees with respect to external access to the wrapped
 * {@link TripleStore}:
 * <ul>
 * <li>transaction are started and committed according to the synchronization strategy enforced by
 * a supplied {@link Synchronizer};</li>
 * <li>at most one thread at a time can access the wrapped {@code TripleStore} and its
 * {@code TripleTransaction}s, with the only exception of {@link TripleStore#close()} and
 * {@link TripleTransaction#end(boolean)} which may be called concurrently with other active
 * operations;</li>
 * <li>access to the wrapped {@code TripleStore} and its {@code TripleTransaction} is enforced to
 * occur strictly in adherence with the lifecycle defined for {@link Component}s (
 * {@code IllegalStateException}s are returned to the caller otherwise);</li>
 * <li>method {@link #reset()} of wrapped {@code TripleStore} is called with no transactions
 * active (this implies waiting for completion of pending transactions);</li>
 * <li>before a {@code TripleTransaction} is ended, all the iterations previously returned and
 * still open to be forcedly closed;</li>
 * <li>before the {@code TripleStore} is closed, pending {@code TripleTransaction} are forcedly
 * ended with a rollback.</li>
 * </ul>
 * </p>
 */
public class SynchronizedTripleStore extends ForwardingTripleStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedTripleStore.class);

    private static final int NEW = 0;

    private static final int INITIALIZED = 1;

    private static final int CLOSED = 2;

    private final TripleStore delegate;

    private final Synchronizer synchronizer;

    private final List<TripleTransaction> transactions;

    private final AtomicInteger state;

    /**
     * Creates a new instance for the wrapped {@code TripleStore} and the {@code Synchronizer}
     * specification string supplied.
     *
     * @param delegate
     *            the wrapped {@code DataStore}
     * @param synchronizerSpec
     *            the synchronizer specification string (see {@link Synchronizer})
     */
    public SynchronizedTripleStore(final TripleStore delegate, final String synchronizerSpec) {
        this(delegate, Synchronizer.create(synchronizerSpec));
    }

    /**
     * Creates a new instance for the wrapped {@code TripleStore} and {@code Synchronizer}
     * specified.
     *
     * @param delegate
     *            the wrapped {@code TripleStore}
     * @param synchronizer
     *            the synchronizer responsible to regulate the access to the wrapped
     *            {@code TripleStore}
     */
    public SynchronizedTripleStore(final TripleStore delegate, final Synchronizer synchronizer) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.synchronizer = Preconditions.checkNotNull(synchronizer);
        this.transactions = Lists.newArrayList();
        this.state = new AtomicInteger(NEW);
        LOGGER.debug("{} configured, synchronizer={}", getClass().getSimpleName(), synchronizer);
    }

    @Override
    protected TripleStore delegate() {
        return this.delegate;
    }

    private void checkState(final int expected) {
        final int state = this.state.get();
        if (state != expected) {
            throw new IllegalStateException("TripleStore "
                    + (state == NEW ? "not initialized"
                            : state == INITIALIZED ? "already initialized" : "already closed"));
        }
    }

    @Override
    public synchronized void init() throws IOException {
        checkState(NEW);
        super.init();
        this.state.set(INITIALIZED);
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws IOException {
        checkState(INITIALIZED);
        this.synchronizer.beginTransaction(readOnly);
        TripleTransaction transaction = null;
        try {
            synchronized (this) {
                checkState(INITIALIZED);
                transaction = delegate().begin(readOnly);
                if (Thread.interrupted()) {
                    transaction.end(false);
                    throw new IllegalStateException("Interrupted");
                }
                transaction = new SynchronizedTripleTransaction(transaction, readOnly);
                synchronized (this.transactions) {
                    this.transactions.add(transaction);
                }
            }
        } finally {
            if (transaction == null) {
                this.synchronizer.endTransaction(readOnly);
            }
        }
        return transaction;
    }

    @Override
    public void reset() throws IOException {
        checkState(INITIALIZED);
        this.synchronizer.beginExclusive();
        try {
            synchronized (this) {
                checkState(INITIALIZED);
                delegate().reset();
            }
        } finally {
            this.synchronizer.endExclusive();
        }
    }

    @Override
    public void close() {
        if (!this.state.compareAndSet(INITIALIZED, CLOSED)
                && !this.state.compareAndSet(NEW, CLOSED)) {
            return;
        }
        List<TripleTransaction> transactionsToEnd;
        synchronized (this.transactions) {
            transactionsToEnd = Lists.newArrayList(this.transactions);
        }
        try {
            for (final TripleTransaction transaction : transactionsToEnd) {
                try {
                    LOGGER.warn("Forcing rollback of tx " + transaction
                            + " due to closure of TripleStore");
                    transaction.end(false);
                } catch (final Throwable ex) {
                    LOGGER.error("Exception caught while ending tx " + transaction
                            + " (rollback assumed): " + ex.getMessage(), ex);
                }
            }
        } finally {
            super.close();
        }
    }

    private final class SynchronizedTripleTransaction extends ForwardingTripleTransaction {

        private final TripleTransaction delegate;

        private final List<WeakReference<CloseableIteration<?, ?>>> iterations;

        private final boolean readOnly;

        private final AtomicBoolean ended;

        SynchronizedTripleTransaction(final TripleTransaction delegate, final boolean readOnly) {
            this.delegate = delegate;
            this.iterations = Lists.newArrayList();
            this.readOnly = readOnly;
            this.ended = new AtomicBoolean(false);
        }

        @Override
        protected TripleTransaction delegate() {
            return this.delegate;
        }

        private <T extends CloseableIteration<?, ?>> T registerIteration(
                @Nullable final T iteration) {
            synchronized (this.iterations) {
                if (iteration == null) {
                    return null;
                } else if (this.ended.get() || Thread.interrupted()) {
                    Util.closeQuietly(iteration);
                    throw new IllegalStateException("Closed / interrupted");
                } else {
                    final int size = this.iterations.size();
                    for (int i = size - 1; i >= 0; --i) {
                        if (this.iterations.get(i).get() == null) {
                            this.iterations.remove(i);
                        }
                    }
                    this.iterations.add(new WeakReference<CloseableIteration<?, ?>>(iteration));
                }
            }
            return iteration;
        }

        private void closeIterations() {
            synchronized (this.iterations) {
                final int size = this.iterations.size();
                for (int i = size - 1; i >= 0; --i) {
                    Util.closeQuietly(this.iterations.remove(i).get());
                }
            }
        }

        private void checkState() {
            if (this.ended.get()) {
                throw new IllegalStateException("DataTransaction already ended");
            }
            if (Thread.interrupted()) {
                throw new IllegalStateException("Interrupted");
            }
        }

        @Override
        public synchronized CloseableIteration<? extends Statement, ? extends Exception> get(
                @Nullable final Resource subject, @Nullable final URI predicate,
                @Nullable final Value object, @Nullable final Resource context)
                throws IOException, IllegalStateException {
            checkState();
            return registerIteration(super.get(subject, predicate, object, context));
        }

        @Override
        public synchronized CloseableIteration<BindingSet, QueryEvaluationException> query(
                final SelectQuery query, @Nullable final BindingSet bindings,
                @Nullable final Long timeout) throws IOException, UnsupportedOperationException {
            checkState();
            return registerIteration(super.query(query, bindings, timeout));
        }

        @Override
        public synchronized void infer(@Nullable final Handler<? super Statement> handler)
                throws IOException, IllegalStateException {
            checkState();
            super.infer(handler);
        }

        @Override
        public synchronized void add(final Iterable<? extends Statement> stream)
                throws IOException, IllegalStateException {
            checkState();
            super.add(stream);
        }

        @Override
        public synchronized void remove(final Iterable<? extends Statement> stream)
                throws IOException, IllegalStateException {
            checkState();
            super.remove(stream);
        }

        @Override
        public void end(final boolean commit) throws IOException {
            if (!this.ended.compareAndSet(false, true)) {
                return;
            }
            closeIterations();
            SynchronizedTripleStore.this.synchronizer.beginCommit();
            try {
                super.end(commit);
            } finally {
                SynchronizedTripleStore.this.synchronizer.endCommit();
                SynchronizedTripleStore.this.synchronizer.endTransaction(this.readOnly);
                synchronized (SynchronizedTripleStore.this.transactions) {
                    SynchronizedTripleStore.this.transactions.remove(this);
                }
            }
        }

    }

}
