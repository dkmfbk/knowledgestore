/**
 * {@code DataStore} server-side component API ({@code ks-server}).
 * <p>
 * This package defines the API of the {@code DataStore} internal server-side component, whose
 * responsibility is to provide a scalable storage for semi-strutured data about resources,
 * mentions and entities. More in details, the package provides:
 * </p>
 * <ul>
 * <li>the {@code DataStore} API ({@link eu.fbk.knowledgestore.datastore.DataStore},
 * {@link eu.fbk.knowledgestore.datastore.DataTransaction});</li>
 * <li>a base implementation ({@link eu.fbk.knowledgestore.datastore.MemoryDataStore}) that stores
 * all data in memory in a space inefficient way (this implementation is good for testing);</li>
 * <li>abstract classes {@link eu.fbk.knowledgestore.datastore.ForwardingDataStore} and
 * {@link eu.fbk.knowledgestore.datastore.ForwardingDataTransaction} for implementing the
 * decorator pattern;</li>
 * <li>three concrete decorator classes that wrap another {@code DataStore} and add it logging
 * capabilities ({@link eu.fbk.knowledgestore.datastore.LoggingDataStore}), synchronization (
 * {@link eu.fbk.knowledgestore.datastore.SynchronizedDataStore}) and per-transaction caching (
 * {@link eu.fbk.knowledgestore.datastore.CachingDataStore}).</li>
 * </ul>
 * <p>
 * Custom implementations of the {@code DataStore} component may be provided by the user to
 * customize the way the KnowledgeStore stores its data.
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.datastore;

