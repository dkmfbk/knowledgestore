package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * A {@code DataStore} transaction.
 * <p>
 * A {@code DataTransaction} is a unit of work over the contents of a {@link DataStore} that
 * provides atomicity (i.e., changes are either completely stored or discarded), isolation (i.e.,
 * other transaction do not see the modifications of this transaction) and durability (i.e.,
 * changes are persisted across different program runs) guarantees.
 * </p>
 * <p>
 * A <tt>DataTransaction</tt> supports the following features:
 * </p>
 * <ul>
 * <li>Lookup of records by ID, via method {@link #lookup(URI, Set, Set)};</li>
 * <li>Matching of records based on type and optional condition, consisting either in the
 * retrieval of (selected properties of) matching records (method
 * {@link #retrieve(URI, XPath, Set)}) or of their count (method {@link #count(URI, XPath)});</li>
 * <li>Matching of record combinations (method {@link #match(Map, Map, Map)});</li>
 * <li>Storing and deletion of single records (methods {@link #store(URI, Record)},
 * {@link #delete(URI, URI)}).</li>
 * </ul>
 * <p>
 * Note that the latter modification methods are not available for read-only transactions (an
 * {@link IllegalStateException} is thrown in that case); moreover, they can return to the caller
 * even if the operation has not yet completed (e.g., due to buffering), in which case it is
 * however guaranteed that following read operation will be able to see newly written data. For
 * all methods accepting a type URI parameter, that parameter can only be one of the supported
 * record types listed in {@link DataStore#SUPPORTED_TYPES}.
 * </p>
 * <p>
 * Transactions are terminated via {@link #end(boolean)}, whose parameter specifies whether
 * changes should be committed or not (this doesn't matter for read-only transactions). Method
 * {@code end()} has always the effect of terminating the transaction: if it throws an exception a
 * rollback must be assumed, even if a commit was asked. In case the JVM is abruptly shutdown
 * during a transaction, the effects of the transaction should be the same as if a rollback was
 * performed. As a particular case of <tt>IOException</tt>, method {@code end()} may throw a
 * {@link DataCorruptedException} in case neither a commit or rollback were possible and the
 * {@code DataStore} is left in some unpredictable state with no possibility of automatic
 * recovery.
 * </p>
 * <p>
 * {@code DataTransaction} objects are not required to be thread safe. Access by at most one
 * thread at a time is guaranteed externally. However, it must be allowed for operations to be
 * issued while streams from previous operations are still open; if a stream is open and a write
 * operations is performed that affects one of the objects still to be returned by the stream (or
 * made an object returnable/not returnable by the stream), then it is allowed for the stream both
 * to return the previous state of the object or to return the new state.
 * </p>
 */
public interface DataTransaction {

    /**
     * Returns a stream of records having the type and IDs specified.
     * 
     * @param type
     *            the URI of the type of records to return
     * @param ids
     *            a set with the IDs of the records to return, not to be modified by the method
     * @param properties
     *            a set with the properties to return for matching records, not modified by the
     *            method; if null, all the available properties must be returned
     * @return a stream with the records matching the IDs and type specified, possibly empty and
     *         in no particular order
     * @throws IOException
     *             in case some IO error occurs
     * @throws IllegalArgumentException
     *             in case the type specified is not supported
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended
     */
    Stream<Record> lookup(URI type, Set<? extends URI> ids, @Nullable Set<? extends URI> properties)
            throws IOException, IllegalArgumentException, IllegalStateException;

    /**
     * Returns a stream of records having the type and matching the optional condition specified.
     * 
     * @param type
     *            the URI of the type of records to return
     * @param condition
     *            an optional condition to be satisfied by matching records; if null, no condition
     *            must be checked
     * @param properties
     *            a set with the properties to return for matching records, not modified by the
     *            method; if null, all the available properties must be returned
     * @return a stream over the records matching the condition and type specified, possibly empty
     *         and in no particular order
     * @throws IOException
     *             in case some IO error occurs
     * @throws IllegalArgumentException
     *             in case the type specified is not supported
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended
     */
    Stream<Record> retrieve(URI type, @Nullable XPath condition,
            @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException,
            IllegalStateException;

    /**
     * Counts the records having the type and matching the optional condition specified. This
     * method performs similarly to {@link #retrieve(URI, XPath, Set)}, but returns only the
     * number of matching instances instead of retrieving the corresponding {@code Record}
     * objects.
     * 
     * @param type
     *            the URI of the type of records to return
     * @param condition
     *            an optional condition to be satisfied by matching records; if null, no condition
     *            must be checked
     * @return the number of records matching the optional condition and type specified
     * @throws IOException
     *             in case some IO error occurs
     * @throws IllegalArgumentException
     *             in case the type specified is not supported
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended
     */
    long count(URI type, @Nullable XPath condition) throws IOException, IllegalArgumentException,
            IllegalStateException;

    /**
     * Evaluates a {@code match} request with the parameters supplied. The operation:
     * <ol>
     * <li>Considers all the combinations {@code <resource, mention, entity, axiom>} such that
     * <ul>
     * <li>{@code mention} {@link KS#MENTION_OF} {@code resource};</li>
     * <li>{@code mention} {@link KS#REFERS_TO} {@code entity} (optional if no condition or
     * projection on entities);</li>
     * <li>{@code mention} {@link KS#EXPRESSES} {@code axiom} (optional if no condition or
     * projection on axioms).</li>
     * </ul>
     * </li>
     * <li>Filters the combinations so that optional {@code conditions} / {@code ids} selections
     * on resource, mention, entity and axiom components are satisfied.</li>
     * <li>Perform projection with duplicate removal of filtered combinations, keeping only the
     * components occurring in {@code properties.keySet()}, returning for each component the
     * subset of properties of {@code properties.get(component_type_URI)}.</li>
     * </ol>
     * In the maps supplied as parameters, components are identified by their type URI, that is
     * {@link KS#RESOURCE}, {@link KS#MENTION}, {@link KS#ENTITY} and {@link KS#AXIOM}.
     * 
     * @param conditions
     *            a non-null map with optional component conditions, indexed by the component type
     *            URI; note the map may be possibly empty or contain conditions only for a subset
     *            of components
     * @param ids
     *            a non-null map with optional ID selections for different components, indexed by
     *            the component type URI; note the map may be possibly empty or contain selections
     *            only for a subset of components
     * @param properties
     *            a non-null, non-empty map with the properties to return for different
     *            components, indexed by the component type URI; if the set of property URIs
     *            mapped to a component is null or empty, then all the properties of the component
     *            should be returned; if a component is not referenced in the map, then it must
     *            not be returned
     * @return a {@code Stream} of combination records
     * @throws IOException
     *             in case some IO error occurs
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended
     */
    Stream<Record> match(Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
            final Map<URI, Set<URI>> properties) throws IOException, IllegalStateException;

    /**
     * Stores a record in the {@code DataStore}. A record may or may not exist for the same ID; in
     * case it exists, it is replaced by the newly specified record. In case the method call
     * returns successfully, there is no guarantee that the write operation completed (e.g.,
     * because of internal buffering); however, it is guaranteed (e.g., via internal flushing)
     * that read operations called subsequently will see the result of the modification. In case
     * the method call fails with an {@code IOException}, there is no guarantee that the
     * {@code DataStore} is left in the same state it was at the time of calling.
     * 
     * @param type
     *            the URI of the type of record to store, not null
     * @param record
     *            the record to store, not null
     * @throws IOException
     *             in case the operation failed, with no guarantee that the {@code DataStore} is
     *             left in the same state if was when the method was called; note that this
     *             exception may trigger a rollback on the caller side
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended, or if it is read-only
     */
    void store(URI type, Record record) throws IOException, IllegalStateException;

    /**
     * Deletes the record stored in the {@code DataStore} with the ID specified.A record may or
     * may not be stored for the specified ID; in case it exists, it is deleted by the operation.
     * In case the method call returns successfully, there is no guarantee that the write
     * operation completed (e.g., because of internal buffering); however, it is guaranteed (e.g.,
     * via internal flushing) that read operations called subsequently will see the result of the
     * modification. In case the method call fails with an {@code IOException}, there is no
     * guarantee that the {@code DataStore} is left in the same state it was at the time of
     * calling.
     * 
     * @param type
     *            the URI of the type of record to store, not null
     * @param id
     *            the ID of the record to delete, not null
     * @throws IOException
     *             in case the operation failed, with no guarantee that the {@code DataStore} is
     *             left in the same state if was when the method was called; note that this
     *             exception may trigger a rollback on the caller side
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended, or if it is read-only
     */
    void delete(URI type, URI id) throws IOException, IllegalStateException;

    /**
     * Ends the transaction, either committing or rolling back its changes (if any). This method
     * always tries to terminate the transaction: if commit is requested but fails, a rollback is
     * forced by the method and an {@code IOException} is thrown. If it is not possible either to
     * commit or rollback, then the {@code DataStore} is possibly left in an unknown state and a
     * {@code DataCorruptedException} is thrown to signal a data corruption situation that cannot
     * be automatically recovered.
     * 
     * @param commit
     *            <tt>true</tt> in case changes made by the transaction should be committed
     * @throws IOException
     *             in case some IO error occurs or the commit request cannot be satisfied for any
     *             reason; it is however guaranteed that a forced rollback has been performed
     * @throws DataCorruptedException
     *             in case it was not possible either to commit or rollback, which implies the
     *             state of the {@code DataStore} is unknown and automatic recovery is not
     *             possible (hence, data is corrupted)
     * @throws IllegalStateException
     *             if the {@code DataTransaction} has been already ended
     */
    void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException;

}

// DESIGN NOTES
//
// XXX 'union' merge criteria has a natural mapping in a HBase layout where the value is
// incorporated in the column name; when writing an attribute, this layout avoid the need to
// retrieve the previous values of an attribute in order to do the merge and compute the new
// values, which is more efficient in case a large number of values can be associated to the
// attribute; however, we do not expect this to be the case (apart from the 'isReferredBy'
// attribute, that is not stored however) -> ignoring this consideration, it seems OK to move all
// the logic related to merge criteria in the frontend
//
// XXX Coprocessors could be used in order to implement the merge and update primitives (the
// latter via a custom CoprocessorProtocol); still, they would need to implement: merge criteria,
// validation, update of related object (e.g., to manipulate bidirectional relations). If we avoid
// coprocessors, then the KS server (= HBase client) would need to fetch the previous values for
// the object being modified and handle merge criteria, validation and enforcing of
// bidirectionality locally. This would require an additional communication between the KS server
// and the affected region server(s), whose cost depend on round-trip latency and bandwidth. We
// may ignore bandwidth (100+MBits/sec in a LAN) and use batching techniques (HBase batch calls)
// to distribute latency (~1ms) over multiple calls, so to make it almost irrelevant. By adopting
// this approach, the benefits of using coprocessors seems greatly overcome by their far greater
// implementation costs, hence they are not adopted
//
// XXX AggregateClient can be used to implement count (in future, we may extract more elaborated
// statistics introducing some kind of 'stats' primitive and a corresponding coprocessor)
//
// XXX an alternative way to delete records would be something like delete(condition), which would
// allow deleting a bunch of objects satisfying a condition without first retrieving them; still,
// it is unlikely the frontend may delete objects without looking at their data and fixing related
// objects, so a retrieval would still be needed in most cases; given also that delete
// performances are not so important as the performances of other operations, the decisions is to
// stick with delete(object) which seems simpler to implement
