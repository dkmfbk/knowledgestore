package eu.fbk.knowledgestore;

import java.io.Closeable;

import javax.annotation.Nullable;

import org.openrdf.model.URI;

/**
 * A KnowledgeStore instance.
 * <p>
 * This interface represents the entry point of the KnowledgeStore API. Users have to first obtain
 * an instance of this interface, in a way that depends on whether the KnowledgeStore is running
 * locally in embedded mode or is accessed via its CRUD and SPARQL endpoints at some base URL
 * (reported by method {@link #getURL()}). After an instance is obtained, new user sessions can be
 * created calling methods {@link #newSession()} (for anonymous sessions} or
 * {@link #newSession(URI, String)} (for authenticated user sessions); the returned
 * {@link Session} objects allow in turn to issue API calls to the KnowledgeStore. When the
 * KnowledgeStore instance is no more used, method {@link #close()} should be called to release
 * the {@code KnowledgeStore} instance and any resource possibly allocated to it.
 * </p>
 * <p>
 * {@code KnowledgeStore} instances are thread safe. Object equality is used for comparing
 * {@code Store} instances.
 * </p>
 */
public interface KnowledgeStore extends Closeable {

    /**
     * Creates a new anonymous user session. Which operations may be called on the returned
     * {@code Session} depend on the security settings for anonymous users of the KnowledgeStore
     * instance.
     * 
     * @return the created {@code Session}
     * @throws IllegalStateException
     *             in case the {@code KnowledgeStore} instance has been closed
     */
    Session newSession() throws IllegalStateException;

    /**
     * Creates a new user session, using the optional username and password specified. This method
     * allows to specify the user the session will be associated to; if the username is null, the
     * session will be anonymous. Supplying a password is necessary for creating a session on a
     * remote KS (as user authentication is enforced); it may be optional otherwise. Which
     * operations may be called on the returned {@code Session} depend on the security settings
     * for the specific authenticated user of the KnowledgeStore instance.
     * 
     * @param username
     *            the username, possibly null
     * @param password
     *            the user password, possibly null
     * @return the created {@code Session}
     * @throws IllegalStateException
     *             in case the {@code KnowledgeStore} instance has been closed
     */
    Session newSession(@Nullable String username, @Nullable String password)
            throws IllegalStateException;

    /**
     * Tests whether this {@code KnowledgeStore} instance has been closed.
     * 
     * @return true, if this {@code KnowledgeStore} instance has been closed
     */
    boolean isClosed();

    /**
     * {@inheritDoc} Closes the {@code KnowledgeStore} instance, releasing any resource possibly
     * allocated. Calling this method additional times has no effect. After this method is called,
     * calls to other methods of the {@code KnowledgeStore} interface will result in
     * {@link IllegalStateException}s being thrown.
     */
    @Override
    void close();

}
