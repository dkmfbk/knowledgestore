package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the KnowledgeStore Runtime Vocabulary.
 * 
 * @see <a href="http://dkm.fbk.eu/ontologies/knowledgestore-runtime">vocabulary specification</a>
 */
public final class KSR {

    /** Recommended prefix for the vocabulary namespace: "ksr". */
    public static final String PREFIX = "ksr";

    /** Vocabulary namespace: "http://dkm.fbk.eu/ontologies/knowledgestore-runtime#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/ontologies/knowledgestore-runtime#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class ksr:Invocation. */
    public static final URI INVOCATION = createURI("Invocation");

    /** Class ksr:StatusCode. */
    public static final URI STATUS_CODE = createURI("StatusCode");

    // PROPERTIES

    /** Property ksr:message. */
    public static final URI MESSAGE = createURI("message");

    /** Property ksr:object. */
    public static final URI OBJECT = createURI("object");

    /** Property ksr:status. */
    public static final URI STATUS = createURI("status");

    /** Property ksr:result. */
    public static final URI RESULT = createURI("result");

    // INDIVIDUALS

    /** Individual ksr:error_bulk. */
    public static final URI ERROR_BULK = createURI("error_bulk");

    /** Individual ksr:error_not_acceptable. */
    public static final URI ERROR_NOT_ACCEPTABLE = createURI("error_not_acceptable");

    /** Individual ksr:error_dependency_not_found. */
    public static final URI ERROR_DEPENDENCY_NOT_FOUND = createURI("error_dependency_not_found");

    /** Individual ksr:error_forbidden. */
    public static final URI ERROR_FORBIDDEN = createURI("error_forbidden");

    /** Individual ksr:error_interrupted. */
    public static final URI ERROR_INTERRUPTED = createURI("error_interrupted");

    /** Individual ksr:error_invalid_input. */
    public static final URI ERROR_INVALID_INPUT = createURI("error_invalid_input");

    /** Individual ksr:error_object_not_found. */
    public static final URI ERROR_OBJECT_NOT_FOUND = createURI("error_object_not_found");

    /** Individual ksr:error_object_already_exists. */
    public static final URI ERROR_OBJECT_ALREADY_EXISTS = createURI("error_object_already_exists");

    /** Individual ksr:error_unexpected. */
    public static final URI ERROR_UNEXPECTED = createURI("error_unexpected");

    /** Individual ksr:ok_bulk. */
    public static final URI OK_BULK = createURI("ok_bulk");

    /** Individual ksr:ok_created. */
    public static final URI OK_CREATED = createURI("ok_created");

    /** Individual ksr:ok_deleted. */
    public static final URI OK_DELETED = createURI("ok_deleted");

    /** Individual ksr:ok_modified. */
    public static final URI OK_MODIFIED = createURI("ok_modified");

    /** Individual ksr:ok_unmodified. */
    public static final URI OK_UNMODIFIED = createURI("ok_unmodified");

    /** Individual ksr:unknown. */
    public static final URI UNKNOWN = createURI("unknown");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private KSR() {
    }

}
