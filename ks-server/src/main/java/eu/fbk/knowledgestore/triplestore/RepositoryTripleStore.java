package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;
import info.aduna.iteration.IterationWrapper;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

public final class RepositoryTripleStore implements TripleStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryTripleStore.class);

    private final Repository repository;

    public RepositoryTripleStore(final Sail sail) {
        this(new SailRepository(sail));
    }

    public RepositoryTripleStore(final Repository repository) {
        this.repository = Preconditions.checkNotNull(repository);
        LOGGER.info("RepositoryTripleStore configured, backend={}", repository.getClass()
                .getSimpleName());
    }

    @Override
    public void init() throws IOException {
        try {
            this.repository.initialize();
        } catch (final Throwable ex) {
            throw new IOException("Could not initialize Sesame repository");
        }
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws IOException {
        return new RepositoryTripleTransaction(readOnly);
    }

    @Override
    public void reset() throws IOException {
        RepositoryConnection connection = null;
        try {
            connection = this.repository.getConnection();
            connection.clear();
            connection.clearNamespaces();
            LOGGER.info("Sesame repository successfully resetted");
        } catch (final RepositoryException ex) {
            throw new IOException("Could not reset Sesame repository", ex);
        } finally {
            Util.closeQuietly(connection);
        }
    }

    @Override
    public void close() {
        try {
            this.repository.shutDown();
        } catch (final RepositoryException ex) {
            LOGGER.error("Failed to shutdown Sesame repository", ex);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private class RepositoryTripleTransaction implements TripleTransaction {

        private final RepositoryConnection connection;

        private final boolean readOnly;

        private final long ts;

        private boolean dirty;

        RepositoryTripleTransaction(final boolean readOnly) throws IOException {

            final long ts = System.currentTimeMillis();
            final RepositoryConnection connection;
            try {
                connection = RepositoryTripleStore.this.repository.getConnection();
            } catch (final RepositoryException ex) {
                throw new IOException("Could not connect to Sesame repository", ex);
            }

            this.connection = connection;
            this.readOnly = readOnly;
            this.ts = ts;
            this.dirty = false;

            try {
                connection.begin();
            } catch (final Throwable ex) {
                Util.closeQuietly(connection);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " started in " + (readOnly ? "read-only" : "read-write")
                        + " mode, " + (System.currentTimeMillis() - ts) + " ms");
            }
        }

        private void checkWritable() {
            if (this.readOnly) {
                throw new IllegalStateException(
                        "Write operation not allowed on read-only transaction");
            }
        }

        @Nullable
        private <T, E extends Exception> CloseableIteration<T, E> logClose(
                @Nullable final CloseableIteration<T, E> iteration) {
            if (iteration == null || !LOGGER.isDebugEnabled()) {
                return iteration;
            }
            final long ts = System.currentTimeMillis();
            return new IterationWrapper<T, E>(iteration) {

                @Override
                protected void handleClose() throws E {
                    try {
                        super.handleClose();
                    } finally {
                        LOGGER.debug("Repository iteration closed after {} ms",
                                System.currentTimeMillis() - ts);
                    }
                }

            };
        }

        @Override
        public CloseableIteration<? extends Statement, ? extends Exception> get(
                final Resource subject, final URI predicate, final Value object,
                final Resource context) throws IOException, IllegalStateException {

            try {
                final long ts = System.currentTimeMillis();
                final CloseableIteration<? extends Statement, ? extends Exception> result;
                if (subject == null || predicate == null || object == null || context == null) {
                    result = logClose(this.connection.getStatements(subject, predicate, object,
                            false, context));
                    LOGGER.debug("getStatements() iteration obtained in {} ms",
                            System.currentTimeMillis() - ts);
                } else {
                    Iterator<Statement> iterator;
                    if (this.connection.hasStatement(subject, predicate, object, true, context)) {
                        iterator = Iterators.emptyIterator();
                    } else {
                        iterator = Iterators
                                .<Statement>singletonIterator(new ContextStatementImpl(subject,
                                        predicate, object, context));
                    }
                    result = new CloseableIteratorIteration<Statement, RuntimeException>(iterator);
                    LOGGER.debug("hasStatement() evaluated in {} ms", System.currentTimeMillis()
                            - ts);
                }
                return result;

            } catch (final RepositoryException ex) {
                throw new IOException("Error while retrieving matching statements", ex);
            }
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> query(
                final SelectQuery query, final BindingSet bindings, @Nullable final Long timeout)
                throws IOException, UnsupportedOperationException, IllegalStateException {

            LOGGER.debug("Evaluating query:\n{}", query.getString());

            final TupleQuery tupleQuery;
            try {
                tupleQuery = this.connection.prepareTupleQuery(QueryLanguage.SPARQL,
                        query.getString());

            } catch (final RepositoryException ex) {
                throw new IOException("Failed to prepare SPARQL tuple query:\n" + query, ex);

            } catch (final MalformedQueryException ex) {
                // should not happen, as SelectQuery can only be created with valid queries
                throw new UnsupportedOperationException(
                        "SPARQL query rejected as malformed by Sesame repository:\n" + query, ex);
            }

            if (bindings != null) {
                for (final Binding binding : bindings) {
                    tupleQuery.setBinding(binding.getName(), binding.getValue());
                }
            }

            if (timeout != null) {
                // Note: we pass the value in ms, although the spec says seconds. However, at
                // least for Virtuoso it seems that the value passed is interpreted as a ms value
                tupleQuery.setMaxQueryTime(timeout.intValue());
            }

            final long ts = System.currentTimeMillis();
            try {
                // execute the query
                final CloseableIteration<BindingSet, QueryEvaluationException> result;
                result = logClose(tupleQuery.evaluate());
                LOGGER.debug("Query result iteration obtained in {} ms",
                        System.currentTimeMillis() - ts);
                return result;

            } catch (final QueryEvaluationException ex) {
                // return all the information available, so to help debugging
                final StringBuilder builder = new StringBuilder();
                boolean emitQuery = false;
                builder.append("Query evaluation failed after ")
                        .append(System.currentTimeMillis() - ts).append(" ms");
                if (ex.getMessage() != null) {
                    builder.append("\n").append(ex.getMessage());
                    emitQuery = !ex.getMessage().contains(query.getString());
                }
                if (emitQuery) {
                    builder.append("\nFailed query:\n\n").append(query);
                }
                throw new IOException(builder.toString(), ex);
            }
        }

        @Override
        public void infer(final Handler<? super Statement> handler) throws IOException,
                IllegalStateException {

            checkWritable();

            // No inference done at this level (to be implemented in a decorator).
            if (handler != null) {
                try {
                    handler.handle(null);
                } catch (final Throwable ex) {
                    Throwables.propagateIfPossible(ex, IOException.class);
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void add(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {

            Preconditions.checkNotNull(statements);
            checkWritable();

            try {
                this.dirty = true;
                this.connection.add(statements);
            } catch (final RepositoryException ex) {
                throw new DataCorruptedException("Error while adding statements", ex);
            }
        }

        @Override
        public void remove(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {

            Preconditions.checkNotNull(statements);
            checkWritable();

            try {
                this.dirty = true;
                this.connection.remove(statements);
            } catch (final RepositoryException ex) {
                throw new DataCorruptedException("Error while removing statements", ex);
            }
        }

        @Override
        public void end(final boolean commit) throws DataCorruptedException, IOException,
                IllegalStateException {

            final long ts = System.currentTimeMillis();
            boolean committed = false;

            try {
                if (this.dirty) {
                    if (commit) {
                        try {
                            this.connection.commit();
                            committed = true;

                        } catch (final Throwable ex) {
                            try {
                                this.connection.rollback();
                                LOGGER.debug("{} rolled back after commit failure", this);

                            } catch (final RepositoryException ex2) {
                                throw new DataCorruptedException(
                                        "Failed to rollback transaction after commit failure", ex);
                            }
                            throw new IOException(
                                    "Failed to commit transaction (rollback forced)", ex);
                        }
                    } else {
                        try {
                            this.connection.rollback();
                        } catch (final Throwable ex) {
                            throw new DataCorruptedException("Failed to rollback transaction", ex);
                        }
                    }
                }
            } finally {
                try {
                    this.connection.close();
                } catch (final RepositoryException ex) {
                    LOGGER.error("Failed to close connection", ex);
                } finally {
                    if (LOGGER.isDebugEnabled()) {
                        final long now = System.currentTimeMillis();
                        LOGGER.debug("{} {} and closed in {} ms, tx duration {} ms", this,
                                committed ? "committed" : "rolled back", now - ts, now - this.ts);
                    }
                }
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

    }

}
