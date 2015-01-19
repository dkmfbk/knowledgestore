/**
 * KnowledgeStore embeddable server ({@code ks-server}).
 * <p>
 * This package provides an embeddable {@link eu.fbk.knowledgestore.server.Server} implementation
 * of the {@link eu.fbk.knowledgestore.KnowledgeStore} interface based on the internal server-side
 * components {@link eu.fbk.knowledgestore.filestore.FileStore},
 * {@link eu.fbk.knowledgestore.datastore.DataStore} and
 * {@link eu.fbk.knowledgestore.triplestore.TripleStore}. Operations of the KnowledgeStore API are
 * routed to internal components, and necessary coordination is implemented in order to support
 * the partial replication of entity data between the {@code DataStore} and the
 * {@code TripleStore}. This implementation is suitable for being embedded in Java applications,
 * allowing them to run an in-process instance of the KnowledgeStore. In case (HTTP) network
 * access is desired, the {@link HttpServer} implementation of module {@code ks-server-http}
 * should rather be used.
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.server;

