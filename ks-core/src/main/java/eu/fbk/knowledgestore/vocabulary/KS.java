package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the KnowledgeStore Core Data Model.
 * 
 * @see <a href="http://dkm.fbk.eu/ontologies/knowledgestore">vocabulary specification</a>
 */
public final class KS {

    /** Recommended prefix for the vocabulary namespace: "ks". */
    public static final String PREFIX = "ks";

    /** Vocabulary namespace: "http://dkm.fbk.eu/ontologies/knowledgestore#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/ontologies/knowledgestore#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class ks:User. */
    public static final URI USER = createURI("User");

    /** Class ks:Axiom. */
    public static final URI AXIOM = createURI("Axiom");

    /** Class ks:Combination. */
    public static final URI COMBINATION = createURI("Combination");

    /** Class ks:Context. */
    public static final URI CONTEXT = createURI("Context");

    /** Class ks:Entity. */
    public static final URI ENTITY = createURI("Entity");

    /** Class ks:Mention. */
    public static final URI MENTION = createURI("Mention");

    /** Class ks:Representation. */
    public static final URI REPRESENTATION = createURI("Representation");

    /** Class ks:Resource. */
    public static final URI RESOURCE = createURI("Resource");

    // PROPERTIES

    /** Property ks:hasCredential. */
    public static final URI HAS_CREDENTIAL = createURI("hasCredential");

    /** Property ks:hasPermission. */
    public static final URI HAS_PERMISSION = createURI("hasPemission");

    /** Property ks:describedBy. */
    public static final URI DESCRIBED_BY = createURI("describedBy");

    /** Property ks:describes. */
    public static final URI DESCRIBES = createURI("describes");

    /** Property ks:encodedBy. */
    public static final URI ENCODED_BY = createURI("encodedBy");

    /** Property ks:expressedBy. */
    public static final URI EXPRESSED_BY = createURI("expressedBy");

    /** Property ks:expresses. */
    public static final URI EXPRESSES = createURI("expresses");

    /** Property ks:hasMention. */
    public static final URI HAS_MENTION = createURI("hasMention");

    /** Property ks:holdsIn. */
    public static final URI HOLDS_IN = createURI("holdsIn");

    /** Property ks:matchedAxiom. */
    public static final URI MATCHED_AXIOM = createURI("matchedAxiom");

    /** Property ks:matchedEntity. */
    public static final URI MATCHED_ENTITY = createURI("matchedEntity");

    /** Property ks:matchedMention. */
    public static final URI MATCHED_MENTION = createURI("matchedMention");

    /** Property ks:matchedResource. */
    public static final URI MATCHED_RESOURCE = createURI("matchedResource");

    /** Property ks:mentionOf. */
    public static final URI MENTION_OF = createURI("mentionOf");

    /** Property ks:referredBy. */
    public static final URI REFERRED_BY = createURI("referredBy");

    /** Property ks:refersTo. */
    public static final URI REFERS_TO = createURI("refersTo");

    /** Property ks:storedAs. */
    public static final URI STORED_AS = createURI("storedAs");

    /** Property ks:storedAttribute. */
    public static final URI STORED_ATTRIBUTE = createURI("storedAttribute");

    // INDIVIDUALS

    /** Individual ks:global. */
    public static final URI GLOBAL = createURI("global");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private KS() {
    }

}
