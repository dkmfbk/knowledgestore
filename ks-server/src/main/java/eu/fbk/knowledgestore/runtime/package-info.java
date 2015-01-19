/**
 * Server-side runtime services ({@code ks-server}).
 * <p>
 * This package provides server-side runtime services used by all the internal components of the
 * KnowledgeStore; in other word, it contains all the logic common to the server-side
 * implementation of the system. Implemented functionalities include component lifecycle
 * definition ({@link eu.fbk.knowledgestore.runtime.Component}), configuration and instantiation (
 * {@link eu.fbk.knowledgestore.runtime.Factory}), service startup (
 * {@link eu.fbk.knowledgestore.runtime.Launcher}), synchronization (
 * {@code eu.fbk.knowledgestore.runtime.Synchronizer}) and file utility methods (
 * {@code eu.fbk.knowledgestore.runtime.Files}).
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.runtime;

