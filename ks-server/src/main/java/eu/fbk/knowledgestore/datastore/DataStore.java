package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.openrdf.model.URI;

import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * A persistent storage component for resource, mention, entity, axiom and context records.
 * <p>
 * A {@code DataStore} component abstracts the access to a storage system for resources, mentions,
 * entities and contexts (the types listed in {@link #SUPPORTED_TYPES}). Access to such a storage
 * system can occur only in the scope of a transaction, which can either be read-only or
 * read/write, identifies a unit of work and provides atomicity, isolation and durability
 * guarantees. Note that a {@code DataStore} obeys the general contract and lifecycle of
 * {@link Component}. In particular, note that a {@code DataStore} must be thread safe, even if
 * the produced {@code DataTransaction} are not required to be so.
 * </p>
 */
public interface DataStore extends Component {

    /** The types of records that can be stored in a {@code DataStore}. */
    Set<URI> SUPPORTED_TYPES = ImmutableSet.of(KS.RESOURCE, KS.MENTION, KS.ENTITY, KS.CONTEXT);

    /**
     * Begins a new read-only / read-write {@code DataStore} transaction. All the accesses to a
     * {@code DataStore} must occur in the scope of a transaction, that must be ended (possibly
     * committing the modifications done) as soon as possible to allow improving throughput.
     * 
     * @param readOnly
     *            <tt>true</tt> if the transaction is not allowed to modify the contents of the
     *            {@code DataStore} (this allows for optimizing accesses).
     * @return the created transaction
     * @throws DataCorruptedException
     *             in case a transaction cannot be started due to the {@code DataStore} persistent
     *             data being damaged or non-existing (this may trigger some external recovery
     *             procedure)
     * @throws IOException
     *             if another IO error occurs while starting the transaction
     * @throws IllegalStateException
     *             if the {@code DataStore} object has been already closed
     */
    DataTransaction begin(boolean readOnly) throws DataCorruptedException, IOException,
            IllegalStateException;
    
}
