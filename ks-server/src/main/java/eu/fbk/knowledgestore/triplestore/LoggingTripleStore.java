package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.IterationWrapper;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Stream;

/**
 * A {@code TripleStore} wrapper that log calls to the operations of a wrapped {@code TripleStore}
 * and their execution times.
 * <p>
 * This wrapper intercepts calls to an underlying {@code TripleStore} and to the
 * {@code TripleTransaction}s it creates, and logs request information and execution times via
 * SLF4J (level DEBUG, logger named after this class). The overhead introduced by this wrapper
 * when logging is disabled is negligible.
 * </p>
 */
public final class LoggingTripleStore extends ForwardingTripleStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingTripleStore.class);

    private final TripleStore delegate;

    /**
     * Creates a new instance for the wrapped {@code TripleStore} specified.
     *
     * @param delegate
     *            the wrapped {@code TripleStore}
     */
    public LoggingTripleStore(final TripleStore delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
        LOGGER.debug("{} configured", getClass().getSimpleName());
    }

    @Override
    protected TripleStore delegate() {
        return this.delegate;
    }

    @Override
    public void init() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.init();
            LOGGER.debug("{} - initialized in {} ms", this, System.currentTimeMillis() - ts);
        } else {
            super.init();
        }
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            final TripleTransaction transaction = new LoggingTripleTransaction(
                    super.begin(readOnly));
            LOGGER.debug("{} - started in {} mode in {} ms", transaction, readOnly ? "read-only"
                    : "read-write", System.currentTimeMillis() - ts);
            return transaction;
        } else {
            return super.begin(readOnly);
        }
    }

    @Override
    public void reset() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.reset();
            LOGGER.debug("{} - reset done in {} ms", this, System.currentTimeMillis() - ts);
        } else {
            super.reset();
        }
    }

    @Override
    public void close() {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.close();
            LOGGER.debug("{} - closed in {} ms", this, System.currentTimeMillis() - ts);
        } else {
            super.close();
        }
    }

    private static final class LoggingTripleTransaction extends ForwardingTripleTransaction {

        private final TripleTransaction delegate;

        LoggingTripleTransaction(final TripleTransaction delegate) {
            this.delegate = Preconditions.checkNotNull(delegate);
        }

        @Override
        protected TripleTransaction delegate() {
            return this.delegate;
        }

        private String format(@Nullable final Value value) {
            return value == null ? "*" : Data.toString(value, Data.getNamespaceMap());
        }

        @Nullable
        private <T, E extends Exception> CloseableIteration<T, E> logClose(
                @Nullable final CloseableIteration<T, E> iteration, final String name,
                final long ts) {
            return iteration == null ? null : new IterationWrapper<T, E>(iteration) {

                @Override
                protected void handleClose() throws E {
                    try {
                        super.handleClose();
                    } finally {
                        LOGGER.debug("{} - {} closed after {} ms", LoggingTripleTransaction.this,
                                name, System.currentTimeMillis() - ts);
                    }
                }

            };
        }

        @Override
        public CloseableIteration<? extends Statement, ? extends Exception> get(
                @Nullable final Resource subject, @Nullable final URI predicate,
                @Nullable final Value object, @Nullable final Resource context)
                throws IOException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final String name = "get() statement iteration for <" + format(subject) + ", "
                        + format(predicate) + ", " + format(object) + ", " + format(context) + ">";
                final long ts = System.currentTimeMillis();
                CloseableIteration<? extends Statement, ? extends Exception> result;
                result = logClose(super.get(subject, predicate, object, context), name, ts);
                LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis()
                        - ts);
                return result;
            } else {
                return super.get(subject, predicate, object, context);
            }
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> query(
                final SelectQuery query, @Nullable final BindingSet bindings, final Long timeout)
                throws IOException, UnsupportedOperationException {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Evaluating query ({} bindings, {} timeout):\n{}",
                        bindings == null ? 0 : bindings.size(), timeout, query);
                final String name = "query() result iteration";
                final long ts = System.currentTimeMillis();
                CloseableIteration<BindingSet, QueryEvaluationException> result;
                result = logClose(super.query(query, bindings, timeout), name, ts);
                LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis()
                        - ts);
                return result;
            } else {
                return super.query(query, bindings, timeout);
            }
        }

        @Override
        public void infer(@Nullable final Handler<? super Statement> handler) throws IOException,
                IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} - start materializing inferences");
                final long ts = System.currentTimeMillis();
                super.infer(handler);
                LOGGER.debug("{} - inferences materialized in {} ms", this,
                        System.currentTimeMillis() - ts);
            } else {
                super.infer(handler);
            }
        }

        @Override
        public void add(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final AtomicLong count = new AtomicLong();
                final AtomicBoolean eof = new AtomicBoolean();
                final Stream<Statement> stream = Stream.create(statements).track(count, eof);
                final long ts = System.currentTimeMillis();
                super.remove(stream);
                LOGGER.debug("{} - {} statements removed in {} ms{}", this, count,
                        System.currentTimeMillis() - ts, eof.get() ? ", EOF" : "");
            } else {
                super.add(statements);
            }

        }

        @Override
        public void remove(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final AtomicLong count = new AtomicLong();
                final AtomicBoolean eof = new AtomicBoolean();
                final Stream<Statement> stream = Stream.create(statements).track(count, eof);
                final long ts = System.currentTimeMillis();
                super.remove(stream);
                LOGGER.debug("{} - {} statements removed in {} ms{}", this, count,
                        System.currentTimeMillis() - ts, eof.get() ? ", EOF" : "");
            } else {
                super.remove(statements);
            }
        }

        @Override
        public void end(final boolean commit) throws IOException {

            if (LOGGER.isDebugEnabled()) {
                final long ts = System.currentTimeMillis();
                super.end(commit);
                LOGGER.debug("{} - {} done in {} ms", this, commit ? "commit" : "rollback",
                        System.currentTimeMillis() - ts);
            } else {
                super.end(commit);
            }
        }

    }

}
