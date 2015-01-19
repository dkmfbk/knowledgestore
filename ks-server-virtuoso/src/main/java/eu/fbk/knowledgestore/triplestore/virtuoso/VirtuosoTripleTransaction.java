package eu.fbk.knowledgestore.triplestore.virtuoso;

import java.io.IOException;
import java.sql.SQLException;
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
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;
import info.aduna.iteration.IterationWrapper;

import virtuoso.sesame2.driver.VirtuosoRepositoryConnection;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.triplestore.SelectQuery;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;

final class VirtuosoTripleTransaction implements TripleTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoTripleTransaction.class);

    private final VirtuosoTripleStore store;

    private final VirtuosoRepositoryConnection connection;

    private final boolean readOnly;

    private final long ts;

    VirtuosoTripleTransaction(final VirtuosoTripleStore store, final boolean readOnly)
            throws IOException {

        assert store != null;

        // try to connect to Virtuoso - under the hoods, a (pooled) JDBC connection is obtained
        // note that there is no special read-only mode in Virtuoso
        final long ts = System.currentTimeMillis();
        final VirtuosoRepositoryConnection connection;
        try {
            connection = (VirtuosoRepositoryConnection) store.getVirtuoso().getConnection();
        } catch (final RepositoryException ex) {
            throw new IOException("Could not connect to Virtuoso", ex);
        }

        this.store = store;
        this.connection = connection;
        this.readOnly = readOnly;
        this.ts = ts;

        try {
            connection.begin();
            connection.getQuadStoreConnection().prepareCall("log_enable(2)").execute();
        } catch (final Throwable ex) {
            try {
                connection.close();
            } catch (final RepositoryException ex2) {
                LOGGER.error("Cannot close connection after begin() failure", ex);
            }
            throw new IOException("Cannot setup read-only transaction", ex);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(this + " started in " + (readOnly ? "read-only" : "read-write")
                    + " mode, " + (System.currentTimeMillis() - ts) + " ms");
        }
    }

    private void checkWritable() {
        if (this.readOnly) {
            throw new IllegalStateException("Write operation not allowed on read-only transaction");
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
                    LOGGER.debug("Virtuoso iteration closed after {} ms",
                            System.currentTimeMillis() - ts);
                }
            }

        };
    }

    @Override
    public CloseableIteration<? extends Statement, ? extends Exception> get(
            @Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, @Nullable final Resource context) throws IOException,
            IllegalStateException {

        try {
            final long ts = System.currentTimeMillis();
            final CloseableIteration<? extends Statement, ? extends Exception> result;
            if (subject == null || predicate == null || object == null || context == null) {
                result = logClose(this.connection.getStatements(subject, predicate, object, false,
                        context));
                LOGGER.debug("Virtuoso getStatements() iteration obtained in {} ms",
                        System.currentTimeMillis() - ts);
            } else {
                Iterator<Statement> iterator;
                if (this.connection.hasStatement(subject, predicate, object, false, context)) {
                    iterator = Iterators.emptyIterator();
                } else {
                    iterator = Iterators.<Statement>singletonIterator(new ContextStatementImpl(
                            subject, predicate, object, context));
                }
                result = new CloseableIteratorIteration<Statement, RuntimeException>(iterator);
                LOGGER.debug("Virtuoso hasStatement() evaluated in {} ms",
                        System.currentTimeMillis() - ts);
            }
            return result;
        } catch (final RepositoryException re) {
            throw new IOException("Error while checking statement.", re);
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> query(final SelectQuery query,
            @Nullable final BindingSet bindings, @Nullable final Long timeout)
            throws DataCorruptedException, IOException, UnsupportedOperationException {

        LOGGER.debug("Evaluating query:\n{}", query);

        final TupleQuery tupleQuery;
        try {
            tupleQuery = this.connection
                    .prepareTupleQuery(QueryLanguage.SPARQL, query.getString());

        } catch (final RepositoryException ex) {
            throw new IOException("Failed to prepare SPARQL tuple query:\n" + query, ex);

        } catch (final MalformedQueryException ex) {
            // should not happen, as SelectQuery can only be created with valid queries
            throw new UnsupportedOperationException(
                    "SPARQL query rejected as malformed by Virtuoso:\n" + query, ex);
        }

        if (bindings != null) {
            for (final Binding binding : bindings) {
                tupleQuery.setBinding(binding.getName(), binding.getValue());
            }
        }

        // note: it seems Virtuoso totally ignores the timeout
        if (timeout != null) {
            tupleQuery.setMaxQueryTime(timeout.intValue() / 1000);
        }

        try {
            final long ts = System.currentTimeMillis();
            final CloseableIteration<BindingSet, QueryEvaluationException> result;
            result = logClose(tupleQuery.evaluate());
            LOGGER.debug("Virtuoso iteration obtained in {} ms", System.currentTimeMillis() - ts);
            return result;
        } catch (final QueryEvaluationException ex) {
            throw new IOException("Failed to execute query:\n" + query, ex);
        }
    }

    @Override
    public void infer(@Nullable final Handler<? super Statement> handler) throws IOException,
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

    /**
     * Adds the specified RDF statement to the triple store. Virtuoso may buffer the operation,
     * performing it when more opportune and in any case ensuring that the same effects are
     * produced as obtainable by directly executing the operation.
     *
     * @param statement
     *            the RDF statement to add
     * @throws DataCorruptedException
     *             in case a non-recoverable data corruption situation is detected
     * @throws IOException
     *             in case another IO error occurs not implying data corruption
     * @throws IllegalStateException
     *             in case the transaction is read-only
     */
    public void add(final Statement statement) throws DataCorruptedException, IOException {

        Preconditions.checkNotNull(statement);
        checkWritable();

        try {
            this.connection.add(statement);
        } catch (final RepositoryException ex) {
            throw new IOException("Failed to add statement: " + statement, ex);
        }
    }

    @Override
    public void add(final Iterable<? extends Statement> stream) throws IOException,
            IllegalStateException {
        addBulk(stream, false);
    }

    /**
     * Adds the specified RDF statements to the triple store. Implementations are designed to
     * perform high throughput insertion.
     *
     * @param statements
     *            the RDF statements to add
     * @throws DataCorruptedException
     *             in case a non-recoverable data corruption situation is detected
     * @throws IOException
     *             in case another IO error occurs not implying data corruption
     * @throws IllegalStateException
     *             in case the transaction is read-only
     */
    public void addBulk(final Iterable<? extends Statement> statements, final boolean transaction)
            throws DataCorruptedException, IOException {

        Preconditions.checkNotNull(statements);
        checkWritable();

        try {
            if (!transaction && !this.store.existsTransactionMarker()) {
                this.store.addTransactionMarker();
                // log_enable affects only the current transaction.
                this.connection.getQuadStoreConnection().prepareCall("log_enable(2)").execute();
            }
            this.connection.add(statements);
            this.connection.commit();

        } catch (final SQLException sqle) {
            throw new IllegalStateException("Invalid internal operation.", sqle);
        } catch (final RepositoryException e) {
            throw new DataCorruptedException("Error while adding bulk data.", e);
        }
    }

    /**
     * Removes the specified RDF statement from the triple store. Virtuoso may buffer the
     * operation, performing it when more opportune and in any case ensuring that the same effects
     * are produced as obtainable by directly executing the operation.
     *
     * @param statement
     *            the RDF statement to remove
     * @throws DataCorruptedException
     *             in case a non-recoverable data corruption situation is detected
     * @throws IOException
     *             in case another IO error occurs not implying data corruption
     * @throws IllegalStateException
     *             in case the transaction is read-only
     */
    public void remove(final Statement statement) throws DataCorruptedException, IOException {

        Preconditions.checkState(!this.readOnly);
        checkWritable();

        try {
            this.connection.remove(statement);
        } catch (final RepositoryException ex) {
            throw new IOException("Failed to remove statement: " + statement, ex);
        }
    }

    @Override
    public void remove(final Iterable<? extends Statement> stream) throws IOException,
            IllegalStateException {
        removeBulk(stream, false);
    }

    /**
     * Removes the specified RDF statements from the triple store. Implementations are designed to
     * perform high throughput insertion.
     *
     * @param statements
     *            the RDF statements to add
     * @throws DataCorruptedException
     *             in case a non-recoverable data corruption situation is detected
     * @throws IOException
     *             in case another IO error occurs not implying data corruption
     * @throws IllegalStateException
     *             in case the transaction is read-only
     */
    public void removeBulk(final Iterable<? extends Statement> statements,
            final boolean transaction) throws DataCorruptedException, IOException {

        Preconditions.checkNotNull(statements);
        checkWritable();

        try {
            if (!transaction && !this.store.existsTransactionMarker()) {
                this.store.addTransactionMarker();
                // log_enable affects only the current transaction.
                this.connection.getQuadStoreConnection().prepareCall("log_enable(2)").execute();
            }

            this.connection.remove(statements);
            this.connection.commit();

        } catch (final SQLException sqle) {
            throw new IllegalStateException("Invalid internal operation.", sqle);
        } catch (final RepositoryException e) {
            throw new DataCorruptedException("Error while adding bulk data.", e);
        }
    }

    @Override
    public void end(final boolean commit) throws IOException {

        final long ts = System.currentTimeMillis();
        boolean committed = false;

        try {
            if (commit) {
                try {
                    if (this.store.existsTransactionMarker()) {
                        this.connection.getQuadStoreConnection().prepareCall("log_enable(1)")
                                .execute();
                        this.store.removeTransactionMarker();
                    }
                    this.connection.commit();
                    committed = true;

                } catch (final Throwable ex) {
                    try {
                        if (this.store.existsTransactionMarker()) {
                            throw new DataCorruptedException("Cannot rollback! "
                                    + "Modifications performed outside a transaction.");
                        }
                        this.connection.rollback();
                        LOGGER.debug("{} rolled back after commit failure", this);

                    } catch (final RepositoryException ex2) {
                        throw new DataCorruptedException(
                                "Failed to rollback transaction after commit failure", ex);
                    }
                    throw new IOException("Failed to commit transaction (rollback forced)", ex);
                }
            } else {
                try {
                    this.connection.rollback();
                } catch (final Throwable ex) {
                    throw new DataCorruptedException("Failed to rollback transaction", ex);
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
