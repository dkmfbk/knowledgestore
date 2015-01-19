package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the Contextualized Knowledge Repository (CKR) vocabulary.
 * 
 * @see <a href="http://dkm.fbk.eu/ckr/meta">vocabulary specification</a>
 */
public final class CKR {

    /** Recommended prefix for the vocabulary namespace: "ckr". */
    public static final String PREFIX = "ckr";

    /** Vocabulary namespace: "http://dkm.fbk.eu/ckr/meta#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/ckr/meta#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class ckr:AttributeValue. */
    public static final URI ATTRIBUTE_VALUE = createURI("AttributeValue");

    /** Class ckr:Context. */
    public static final URI CONTEXT = createURI("Context");

    /** Class ckr:Module. */
    public static final URI MODULE = createURI("Module");

    // PROPERTIES

    /** Property ckr:hasAttribute. */
    public static final URI HAS_ATTRIBUTE = createURI("hasAttribute");

    /** Property ckr:hasEvalMeta. */
    public static final URI HAS_EVAL_META = createURI("hasEvalMeta");

    /** Property ckr:hasEvalObject. */
    public static final URI HAS_EVAL_OBJECT = createURI("hasEvalObject");

    /** Property ckr:hasModule. */
    public static final URI HAS_MODULE = createURI("hasModule");

    // INDIVIDUALS

    /** Individual ckr:global. */
    public static final URI GLOBAL = createURI("global");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private CKR() {
    }

}
