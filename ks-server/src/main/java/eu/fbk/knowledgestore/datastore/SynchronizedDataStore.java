package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.runtime.Synchronizer;

/**
 * A {@code DataStore} wrapper that synchronizes and enforces a proper access to an another
 * {@code DataStore}.
 * <p>
 * This wrapper provides the following guarantees with respect to external access to the wrapped
 * {@link DataStore}:
 * <ul>
 * <li>transaction are started and committed according to the synchronization strategy enforced by
 * a supplied {@link Synchronizer};</li>
 * <li>at most one thread at a time can access the wrapped {@code DataStore} and its
 * {@code DataTransaction}s, with the only exception of {@link DataStore#close()} and
 * {@link DataTransaction#end(boolean)} which may be called concurrently with other active
 * operations;</li>
 * <li>access to the wrapped {@code DataStore} and its {@code DataTransaction} is enforced to
 * occur strictly in adherence with the lifecycle defined for {@link Component}s (
 * {@code IllegalStateException}s are thrown to the caller otherwise);</li>
 * <li>before a {@code DataTransaction} is ended, all the streams previously returned and still
 * open to be forcedly closed;</li>
 * <li>before the {@code DataStore} is closed, pending {@code DataTransaction} are forcedly ended
 * with a rollback;</li>
 * </ul>
 * </p>
 */
public final class SynchronizedDataStore extends ForwardingDataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedDataStore.class);

    private static final int NEW = 0;

    private static final int INITIALIZED = 1;

    private static final int CLOSED = 2;

    private final DataStore delegate;

    private final Synchronizer synchronizer;

    private final Set<DataTransaction> transactions; // used also as lock object

    private final AtomicInteger state;

    /**
     * Creates a new instance for the wrapped {@code DataStore} and the {@code Synchronizer}
     * specification string supplied.
     * 
     * @param delegate
     *            the wrapped {@code DataStore}
     * @param synchronizerSpec
     *            the synchronizer specification string (see {@link Synchronizer})
     */
    public SynchronizedDataStore(final DataStore delegate, final String synchronizerSpec) {
        this(delegate, Synchronizer.create(synchronizerSpec));
    }

    /**
     * Creates a new instance for the wrapped {@code DataStore} and {@code Synchronizer}
     * specified.
     * 
     * @param delegate
     *            the wrapped {@code DataStore}
     * @param synchronizer
     *            the synchronizer responsible to regulate the access to the wrapped
     *            {@code DataStore}
     */
    public SynchronizedDataStore(final DataStore delegate, final Synchronizer synchronizer) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.synchronizer = Preconditions.checkNotNull(synchronizer);
        this.transactions = Sets.newHashSet();
        this.state = new AtomicInteger(NEW);
        LOGGER.debug("{} configured, synchronizer=", getClass().getSimpleName(), synchronizer);
    }

    @Override
    protected DataStore delegate() {
        return this.delegate;
    }

    private void checkState(final int expected) {
        final int state = this.state.get();
        if (state != expected) {
            throw new IllegalStateException("DataStore "
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
    public DataTransaction begin(final boolean readOnly) throws IOException, IllegalStateException {
        checkState(INITIALIZED);
        this.synchronizer.beginTransaction(readOnly);
        DataTransaction transaction = null;
        try {
            synchronized (this) {
                checkState(INITIALIZED);
                transaction = delegate().begin(readOnly);
                transaction = new SynchronizedDataTransaction(transaction, readOnly);
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
    public void close() {
        if (!this.state.compareAndSet(INITIALIZED, CLOSED)
                && !this.state.compareAndSet(NEW, CLOSED)) {
            return;
        }
        List<DataTransaction> transactionsToEnd;
        synchronized (this.transactions) {
            transactionsToEnd = Lists.newArrayList(this.transactions);
        }
        try {
            for (final DataTransaction transaction : transactionsToEnd) {
                try {
                    LOGGER.warn("Forcing rollback of DataTransaction " + transaction
                            + "due to closure of DataStore");
                    transaction.end(false);
                } catch (final Throwable ex) {
                    LOGGER.error("Exception caught while ending DataTransaction " + transaction
                            + "(rollback assumed): " + ex.getMessage(), ex);
                }
            }
        } finally {
            super.close();
        }
    }

    private final class SynchronizedDataTransaction extends ForwardingDataTransaction {

        private final DataTransaction delegate;

        private final List<WeakReference<Stream<?>>> streams;

        private final boolean readOnly;

        private final AtomicBoolean ended;

        SynchronizedDataTransaction(final DataTransaction delegate, final boolean readOnly) {
            this.delegate = Preconditions.checkNotNull(delegate);
            this.streams = Lists.newArrayList();
            this.readOnly = readOnly;
            this.ended = new AtomicBoolean(false);
        }

        @Override
        protected DataTransaction delegate() {
            return this.delegate;
        }

        private <T extends Stream<?>> T registerStream(@Nullable final T stream) {
            synchronized (this.streams) {
                if (stream == null) {
                    return null;
                } else if (this.ended.get()) {
                    Util.closeQuietly(stream);
                } else {
                    final int size = this.streams.size();
                    for (int i = size - 1; i >= 0; --i) {
                        if (this.streams.get(i).get() == null) {
                            this.streams.remove(i);
                        }
                    }
                    this.streams.add(new WeakReference<Stream<?>>(stream));
                }
            }
            return stream;
        }

        private void closeStreams() {
            synchronized (this.streams) {
                final int size = this.streams.size();
                for (int i = size - 1; i >= 0; --i) {
                    Util.closeQuietly(this.streams.remove(i).get());
                }
            }
        }

        private void checkState() {
            if (this.ended.get()) {
                throw new IllegalStateException("DataTransaction already ended");
            }
        }

        private void checkWritable() {
            if (this.readOnly) {
                throw new IllegalStateException("DataTransaction is read-only");
            }
        }

        @Override
        public synchronized Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
                @Nullable final Set<? extends URI> properties) throws IOException,
                IllegalArgumentException, IllegalStateException {
            checkState();
            return registerStream(super.lookup(type, ids, properties));
        }

        @Override
        public synchronized Stream<Record> retrieve(final URI type,
                @Nullable final XPath condition, @Nullable final Set<? extends URI> properties)
                throws IOException, IllegalArgumentException, IllegalStateException {
            checkState();
            return registerStream(super.retrieve(type, condition, properties));
        }

        @Override
        public synchronized long count(final URI type, @Nullable final XPath condition)
                throws IOException, IllegalArgumentException, IllegalStateException {
            checkState();
            return super.count(type, condition);
        }

        @Override
        public Stream<Record> match(final Map<URI, XPath> conditions,
                final Map<URI, Set<URI>> ids, final Map<URI, Set<URI>> properties)
                throws IOException, IllegalStateException {
            checkState();
            return registerStream(super.match(conditions, ids, properties));
        }

        @Override
        public void store(final URI type, final Record record) throws IOException,
                IllegalStateException {
            checkState();
            checkWritable();
            super.store(type, record);
        }

        @Override
        public void delete(final URI type, final URI id) throws IOException, IllegalStateException {
            checkState();
            checkWritable();
            super.delete(type, id);
        }

        @Override
        public void end(final boolean commit) throws IOException, IllegalStateException {
            if (!this.ended.compareAndSet(false, true)) {
                return;
            }
            closeStreams();
            SynchronizedDataStore.this.synchronizer.beginCommit();
            try {
                super.end(commit);
            } finally {
                SynchronizedDataStore.this.synchronizer.endCommit();
                SynchronizedDataStore.this.synchronizer.endTransaction(this.readOnly);
                synchronized (SynchronizedDataStore.this.transactions) {
                    SynchronizedDataStore.this.transactions.remove(this);
                }
            }
        }

    }

}
