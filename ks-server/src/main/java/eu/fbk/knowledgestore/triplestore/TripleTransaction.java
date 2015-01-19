package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

/**
 * A triple store transaction.
 * <p>
 * A {@code TripleTransaction} is a unit of work over the contents of a {@link TripleStore} that
 * provides atomicity (i.e., changes are either completely stored or discarded), isolation (i.e.,
 * other transaction do not see the modifications of this transaction) and durability (i.e.,
 * changes are persisted across different program runs) guarantees.
 * </p>
 * <p>
 * A {@code TripleTransaction} supports the following four main features:
 * </p>
 * <ul>
 * <li><b>Statement retrieval</b>, via method {@link #get(Resource, URI, Value, Resource)};</li>
 * <li><b>SPARQL querying</b>, via method {@link #query(SelectQuery, BindingSet)};</li>
 * <li><b>Inference materialization</b>, via method {@link #infer(Handler)}</li>
 * <li><b>Triple modification</b>, via bulk methods {@link #add(Iterable)} and
 * {@link #remove(Iterable)}.</li>
 * </ul>
 * <p>
 * Note that the latter two functionalities are not available for read-only transactions (an
 * {@link IllegalStateException} is thrown in that case).
 * </p>
 * <p>
 * Transactions are terminated via {@link #end(boolean)}, whose parameter specifies whether
 * changes should be committed. Method {@code end()} always tries to terminate the transaction: if
 * it throws an {@code IOException} exception a rollback must be assumed, even if a commit was
 * asked; if it throws a {@code DataCorruptedException}, then neither commit or rollback were
 * possible and the {@code TripleStore} is left in some unpredictable state with no possibility of
 * automatic recovery. In that case, external code may resort to the {@link TripleStore#reset()}
 * and re-population procedure to recover the situation. In case the JVM is abruptly shutdown
 * during a transaction, the effects of the transaction should be the same as if a rollback was
 * performed.
 * </p>
 * <p>
 * {@code TripleTransaction} objects are not required to be thread safe. Access by at most one
 * thread at a time can be assumed and must be guaranteed externally, with the only exception of
 * method {@link #end(boolean)} that can be called at any moment by any thread in case the
 * transaction need to be rolled back. Note also that it must be allowed for operations to be
 * issued while streams from previous operations are still open; if a stream is open and a write
 * operations is performed that affects one of the statements/query solutions still to be returned
 * by the stream (or made a statement/query solution returnable/not returnable by the stream),
 * then the behaviour of the stream is undefined.
 * </p>
 */
public interface TripleTransaction {

    /**
     * Returns a Sesame {@code CloseableIteration} over all the RDF statements matching the
     * (optional) subject, predicate, object, context supplied. Null values are used as wildcard.
     * Implementations are designed to perform high throughput statement extraction (likely more
     * efficient than doing a SPARQL query).
     *
     * @param subject
     *            the subject to match, null to match any subject
     * @param predicate
     *            the predicate to match, null to match any predicate
     * @param object
     *            the object to match, null to match any object
     * @param context
     *            the context to match, null to match any context
     * @return an iteration over matching RDF statements
     * @throws IOException
     *             in case some IO error occurs
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended
     */
    CloseableIteration<? extends Statement, ? extends Exception> get(@Nullable Resource subject,
            @Nullable URI predicate, @Nullable Value object, @Nullable Resource context)
            throws IOException, IllegalStateException;

    /**
     * Evaluates the supplied SPARQL SELECT query, returning a Sesame {@code CloseableIteration}
     * over its solutions. The method accepts an optional <tt>BindingSet</tt> object, providing
     * bindings for variables in the query; it can be used to instantiate parametric queries,
     * similarly to <tt>PreparedStatements</tt> in JDBC.
     *
     * @param query
     *            the SPARQL SELECT query to evaluate
     * @param bindings
     *            optional bindings for variables in the query;
     * @param timeout
     *            optional timeout in milliseconds for the query
     * @return an iteration over the results of the query
     * @throws IOException
     *             in case some IO error occurs
     * @throws UnsupportedOperationException
     *             in case the query, while being valid according to SPARQL, uses a construct,
     *             function or feature that is not supported by the triple store implementation
     *             (refer to the implementation documentation for unsupported features)
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended
     */
    CloseableIteration<BindingSet, QueryEvaluationException> query(SelectQuery query,
            @Nullable BindingSet bindings, @Nullable Long timeout) throws IOException,
            UnsupportedOperationException, IllegalStateException;

    /**
     * Performs inference, materializing the logical closure of the RDF statements contained in
     * the triple store. The method accepts an optional {@code Handler} inferred statements can be
     * notified to, allowing for their additional processing by external code.
     *
     * @param handler
     *            an optional handler inferred RDF statements can be notified to
     * @throws IOException
     *             in case some IO error occurs, with no guarantee that the {@code TripleStore} is
     *             left in the same state if was when the method was called; note that this
     *             exception may trigger a rollback on the caller side
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended, or if it is read-only
     */
    void infer(@Nullable Handler<? super Statement> handler) throws IOException,
            IllegalStateException;

    /**
     * Adds all the RDF statements in the {@code Iterable} specified to the triple store.
     * Implementations are designed to perform high throughput insertion. They are also allowed to
     * buffer part or all of the operation, successfully returning before specified triples have
     * been completely modified; in this case, it must be however guaranteed that subsequent calls
     * to {@link #query(SelectQuery, BindingSet)} will 'see' all the triples added.
     *
     * @param statements
     *            the statements to add
     * @throws IOException
     *             in case some IO error occurs, with no guarantee that the {@code TripleStore} is
     *             left in the same state if was when the method was called; note that this
     *             exception may trigger a rollback on the caller side
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended, or if it is read-only
     */
    void add(Iterable<? extends Statement> statements) throws IOException, IllegalStateException;

    /**
     * Removes all the RDF statements in the {@code Iterable} specified from the triple store.
     * Implementations are designed to perform high throughput deletion. They are also allowed to
     * return before specified triples have been completely removed; in this case, it must be
     * however guaranteed that subsequent calls to {@link #query(SelectQuery, BindingSet)} will
     * not 'see' any of the triples removed.
     *
     * @param statements
     *            the statements to remove
     * @throws IOException
     *             in case some IO error occurs, with no guarantee that the {@code TripleStore} is
     *             left in the same state if was when the method was called; note that this
     *             exception may trigger a rollback on the caller side
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended, or if it is read-only
     */
    void remove(Iterable<? extends Statement> statements) throws IOException,
            IllegalStateException;

    /**
     * Ends the transaction, either committing or rolling back its changes (if any). This method
     * always tries to terminate the transaction: if commit is requested but fails, a rollback is
     * forced by the method and an {@code IOException} is thrown. If it is not possible either to
     * commit or rollback, then the {@code TripleStore} is possibly left in an unknown state and a
     * {@code DataCorruptedException} is thrown to signal a data corruption situation that cannot
     * be automatically recovered (apart from calling {@link TripleStore#reset()} and repopulating
     * the {@code TripleStore}).
     *
     * @param commit
     *            true in case changes made by the transaction should be committed
     * @throws IOException
     *             in case some IO error occurs or the commit request cannot be satisfied for any
     *             reason; it is however guaranteed that a forced rollback has been performed
     * @throws DataCorruptedException
     *             in case it was not possible either to commit or rollback, which implies the
     *             state of the {@code TripleStore} is unknown and automatic recovery is not
     *             possible (hence, data is corrupted)
     * @throws IllegalStateException
     *             if the {@code TripleTransaction} has been already ended
     */
    void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException;

}

// IMPLEMENTATION NOTES
//
// - ask, construct and describe queries can be implemented on top of select; a limit clause
// should be generated for ask queries for efficiency; a stream wrapper based on
// MultiProjectionIterator and the code of SailGraphQuery can be used for construct and describe.
// - during query execution, QueryEvaluationException is thrown in case of IO errors or if a
// malformed query is detected after execution began: in the first case an IOException seems more
// appropriate, while in the second case an UnsupportedOperationException seems appropriate
// (it's the client fault, not something expected to be recoverable)
//
// Corruption may result from
// - internal failure of the triple store (e.g. due to bug)
// - bug in the Java code or unsupported rollback that leaves partial modifications in the store
