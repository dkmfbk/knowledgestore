package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;

import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

/**
 * A storage for triples, supporting named graphs, SPARQL queries, inference and transactions.
 * <p>
 * A <tt>TripleStore</tt> object abstracts the access to a triple store service, which efficiently
 * stores a bunch of triples organized in named graphs, providing support for inference of derived
 * triples, SPARQL queries and transactions. Access to those functionalities and to a
 * <tt>TripleStore</tt> contents can occur only in the scope of a transaction, which can either be
 * read-only or read/write, identifies a unit of work and provides atomicity, isolation and
 * durability guarantees. Note that a {@code TripleStore} obeys the general contract and lifecycle
 * of {@link Component}.
 * </p>
 * <p>
 * As a special kind of <tt>IOException</tt>, implementations of this interface may throw a
 * {@link DataCorruptedException} in case a data corruption situation is detected, caused either
 * by any or all of the triple store files / external resources being non-existent or corrupted
 * for whatever reason. Throwing a <tt>DataCorruptedException</tt> when such a situation is
 * detected is important, as it allows external code to attempt a recovery procedure by calling
 * the {@link #reset()} function, which allows wiping out the content of the triple store, that
 * can then be re-populated again.
 * </p>
 */
public interface TripleStore extends Component {

    /**
     * Begins a new read-only / read-write triple store transaction. All the accesses to a triple
     * store must occur in the scope of a transaction, that must be ended (possibly committing the
     * modifications done) as soon as possible to allow improving throughput.
     * 
     * @param readOnly
     *            <tt>true</tt> if the transaction is not allowed to modify the contents of the
     *            triple store (this allows for optimizing the access to the triple store).
     * @return the created transaction
     * @throws DataCorruptedException
     *             in case a transaction cannot be started due to triple store files being damaged
     *             or non-existing; a {@link #reset()} call followed by a full triple store
     *             re-population should be attempted to recover this situation
     * @throws IOException
     *             if another IO error occurs while starting the transaction, not implying a data
     *             corruption situation
     */
    TripleTransaction begin(boolean readOnly) throws DataCorruptedException, IOException;

    /**
     * Resets the triple store contents, possibly recreating or reinitializing the external
     * services / files this triple store is based on. This method is expected to be called either
     * to initialize the triple store if it does not exist, or to recover a data corruption
     * situation. On success, the triple store is left in an empty status. It is a task of the
     * user code to re-populate it, if necessary.
     * 
     * @throws IOException
     *             if an IO error occurs while resetting the triple store (this situation is not
     *             expected to be recovered automatically via code).
     */
    void reset() throws IOException;

}
