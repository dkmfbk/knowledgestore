package eu.fbk.knowledgestore;

import eu.fbk.knowledgestore.Operation.*;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;
import org.openrdf.model.URI;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;

/**
 * A KnowledgeStore user session.
 * <p>
 * Interaction with the KnowledgeStore through its Java API occurs within a {@code Session}, which
 * encapsulates user-specific configuration and, if authenticated, user credentials. A
 * {@code Session} can be obtained via {@link KnowledgeStore#newSession()} (for anonymous user) or
 * {@link KnowledgeStore#newSession(URI, String)} (authenticated user) and must be released with
 * {@link #close()} to freed resources possibly allocated to the session.
 * </p>
 * <p>
 * The identifier of the session's user, if any, is provided by method {@link #getUser()}, while
 * the configured password is available using {@link #getPassword()}. Method
 * {@link #getNamespaces()} returns a session-specific, modifiable prefix-to-namespace map that
 * can be used for interacting with the KnowledgeStore; this map is initially filled with mappings
 * from {@link Data#getNamespaceMap()}. Namespace mappings play a relevant role in the interaction
 * with the KnowledgeStore, as they are used for encoding and decoding {@link XPath} expressions,
 * merge {@link Criteria}s and SPARQL queries.
 * </p>
 * <p>
 * Operations of the KnowledgeStore API can be invoked starting from a {@code Session} object and
 * using <i>operation objects</i>. Every operation has its own {@link Operation} sub-class, an
 * instance of which can be obtained by calling a method in {@code Session}; after an operation
 * object is created, a number of common and operation-specific parameters can be set on this
 * object to specify and control the execution of the operation, which is triggered by calling one
 * of the {@code exec()} / {@code execXXX()} methods on the operation objects. More in details,
 * the API operations in the following table are offered:
 * </p>
 * <blockquote>
 * <table border="1">
 * <tr>
 * <th>{@code Session} method</th>
 * <th>{@code Operation} sub-class</th>
 * <th>Type</th>
 * <th>Endpoint</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>{@link #download()}</td>
 * <td>{@link Download}</td>
 * <td>read-only</td>
 * <td>CRUD</td>
 * <td>Downloads one or more resource representations.</td>
 * </tr>
 * <tr>
 * <td>{@link #upload()}</td>
 * <td>{@link Upload}</td>
 * <td>read-write</td>
 * <td>CRUD</td>
 * <td>Uploads one or more resource representations.</td>
 * </tr>
 * <tr>
 * <td>{@link #count(URI)}</td>
 * <td>{@link Count}</td>
 * <td>read-only</td>
 * <td>CRUD</td>
 * <td>Count the number of resources / mentions / entities / axioms that match an optional
 * condition.</td>
 * </tr>
 * <tr>
 * <td>{@link #retrieve(URI)}</td>
 * <td>{@link Retrieve}</td>
 * <td>read-only</td>
 * <td>CRUD</td>
 * <td>Retrieves selected properties of resources / mentions / entities / axioms that match an
 * optional condition</td>
 * </tr>
 * <tr>
 * <td>{@link #create(URI)}</td>
 * <td>{@link Create}</td>
 * <td>read-write</td>
 * <td>CRUD</td>
 * <td>Creates one or more new resources / mentions / entities / axioms.</td>
 * </tr>
 * <tr>
 * <td>{@link #merge(URI)}</td>
 * <td>{@link Merge}</td>
 * <td>read-write</td>
 * <td>CRUD</td>
 * <td>Merges local record descriptions of resources / mentions / entities / axioms with
 * descriptions stored in the KnowledgeStore, applying supplied merge criteria.</td>
 * </tr>
 * <tr>
 * <td>{@link #update(URI)}</td>
 * <td>{@link Update}</td>
 * <td>read-write</td>
 * <td>CRUD</td>
 * <td>Updates one or more properties of all the resources / mentions / entities / axioms that
 * match an optional condition.</td>
 * </tr>
 * <tr>
 * <td>{@link #delete(URI)}</td>
 * <td>{@link Delete}</td>
 * <td>read-write</td>
 * <td>CRUD</td>
 * <td>Deletes all the resources / mentions / entities / axioms matching an optional condition.</td>
 * </tr>
 * <tr>
 * <td>{@link #match()}</td>
 * <td>{@link Match}</td>
 * <td>read-only</td>
 * <td>CRUD</td>
 * <td>Returns all the &lt;resource, mention, entity, axioms&gt; combinations matching optional
 * condition where the mention refers to the resource and entity and the axioms have been
 * extracted from the mention.</td>
 * </tr>
 * <tr>
 * <td>{@link #sparql(String, Object...)}</td>
 * <td>{@link Sparql}</td>
 * <td>read-only</td>
 * <td>SPARQL</td>
 * <td>Evaluates a SPARQL select / construct / describe / ask query on crystallized axiom data.</td>
 * </tr>
 * </table>
 * </blockquote>
 * <p>
 * Note that operation marked as read-write may be forbidden to anonymous or non-privileged users,
 * based on the security configuration of the KnowledgeStore. {@code Session} objects are thread
 * safe. Object equality is used for comparing {@code Session} instances.
 * </p>
 */
public interface Session extends Closeable {

    /**
     * Returns the username associated to this {@code Session}, if any.
     * 
     * @return the username, null if the {@code Session} is anonymous
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    @Nullable
    String getUsername() throws IllegalStateException;

    /**
     * Returns the user password, if any.
     * 
     * @return the password
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    @Nullable
    String getPassword() throws IllegalStateException;

    /**
     * Returns the modifiable namespace map of this {@code Session}. The map is backed by the
     * default mappings of {@code Data#getNamespaceMap()}. New mappings are stored in the
     * {@code Session} map and may override the default mappings. Removal of mappings is performed
     * on the {@code Session} map, and may reactivate the default mapping as a consequence.
     * 
     * @return the modifiable namespace map of this {@code Session}
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Map<String, String> getNamespaces() throws IllegalStateException;

    /**
     * Creates a new {@code Download} operation object.
     * 
     * @param resourceID
     *            the ID of the resource whose representation should be downloaded
     * @return the created {@code Download} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Download download(URI resourceID) throws IllegalStateException;

    /**
     * Creates a new {@code Upload} operation object.
     * 
     * @param resourceID
     *            the ID of the resource whose representation should be updated
     * @return the created {@code Upload} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Upload upload(URI resourceID) throws IllegalStateException;

    /**
     * Creates a new {@code Count} operation object for counting records of the type specified.
     * 
     * @param type
     *            the URI of the type of record to count, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Count} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Count count(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Retrieve} operation object for retrieving records of the type
     * specified.
     * 
     * @param type
     *            the URI of the type of record to retrieve, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Retrieve} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Retrieve retrieve(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Create} operation object for storing new records of the type
     * specified.
     * 
     * @param type
     *            the URI of the type of record to create, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Create} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Create create(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Merge} operation object for merging local record description of the
     * type specified with descriptions stored in the KnowledgeStore.
     * 
     * @param type
     *            the URI of the type of record to merge, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Merge} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Merge merge(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Update} operation object for updating one or more properties of
     * records of the type specified.
     * 
     * @param type
     *            the URI of the type of record to update, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Update} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Update update(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Delete} operation object for deleting records of the type specified.
     * 
     * @param type
     *            the URI of the type of record to delete, either {@link KS#RESOURCE},
     *            {@link KS#MENTION}, {@link KS#ENTITY} or {@link KS#AXIOM}
     * @return the created {@code Delete} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Delete delete(URI type) throws IllegalStateException;

    /**
     * Creates a new {@code Match} operation object for matching &lt;resource, mention, entity,
     * axioms&gt; 4-tuples matching specific conditions.
     * 
     * @return the created {@code Match} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Match match() throws IllegalStateException;

    /**
     * Creates a new {@code Sparql} operation object for evaluating a SPARQL query on crystallized
     * axiom data.
     * 
     * @param expression
     *            the SPARQL query expression, not null
     * @param arguments
     *            arguments to be injected in {@code $$} placeholders in the query expression; can
     *            be {@code URI}s, {@code Literal}s or scalar values that can be converted to
     *            {@code Literal}s (e.g., strings, integers)
     * @return the created {@code Sparql} operation object
     * @throws IllegalStateException
     *             if the {@code Session} has been closed
     */
    Sparql sparql(String expression, Object... arguments) throws IllegalStateException;

    SparqlUpdate sparqlupdate() throws IllegalStateException;

    /**
     * Tests whether this {@code Session} instance has been closed.
     * 
     * @return true, if this {@code Session} instance has been closed
     */
    boolean isClosed();

    /**
     * {@inheritDoc} Closes the {@code Session} instance, releasing any resource possibly
     * allocated. Calling this method additional times has no effect. After this method is called,
     * calls to other method of the {@code Session} interface will result in
     * {@link IllegalStateException}s to be thrown.
     */
    @Override
    void close();

}
