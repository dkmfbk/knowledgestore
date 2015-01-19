package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the NEPOMUK Information Element Ontology.
 * 
 * @see <a href="http://www.semanticdesktop.org/ontologies/2007/01/19/nie">vocabulary
 *      specification</a>
 */
public final class NIE {

    /** Recommended prefix for the vocabulary namespace: "nie". */
    public static final String PREFIX = "nie";

    /** Vocabulary namespace: "http://www.semanticdesktop.org/ontologies/2007/01/19/nie#". */
    public static final String NAMESPACE = "http://www.semanticdesktop.org"
            + "/ontologies/2007/01/19/nie#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class nie:DataObject. */
    public static final URI DATA_OBJECT = createURI("DataObject");

    /** Class nie:DataSource. */
    public static final URI DATA_SOURCE = createURI("DataSource");

    /** Class nie:InformationElement. */
    public static final URI INFORMATION_ELEMENT = createURI("InformationElement");

    // PROPERTIES

    /** Property nie:byteSize. */
    public static final URI BYTE_SIZE = createURI("byteSize");

    /** Property nie:characterSet. */
    public static final URI CHARACTER_SET = createURI("characterSet");

    /** Property nie:comment. */
    public static final URI COMMENT = createURI("comment");

    /** Property nie:contentCreated. */
    public static final URI CONTENT_CREATED = createURI("contentCreated");

    /** Property nie:contentLastModified. */
    public static final URI CONTENT_LAST_MODIFIED = createURI("contentLastModified");

    /** Property nie:contentSize. */
    public static final URI CONTENT_SIZE = createURI("contentSize");

    /** Property nie:copyright. */
    public static final URI COPYRIGHT = createURI("copyright");

    /** Property nie:created. */
    public static final URI CREATED = createURI("created");

    /** Property nie:dataSource. */
    public static final URI DATA_SOURCE_PROPERTY = createURI("dataSource");

    /** Property nie:depends. */
    public static final URI DEPENDS = createURI("depends");

    /** Property nie:description. */
    public static final URI DESCRIPTION = createURI("description");

    /** Property nie:disclaimer. */
    public static final URI DISCLAIMER = createURI("disclaimer");

    /** Property nie:generator. */
    public static final URI GENERATOR = createURI("generator");

    /** Property nie:generatorOption. */
    public static final URI GENERATOR_OPTION = createURI("generatorOption");

    /** Property nie:hasLogicalPart. */
    public static final URI HAS_LOGICAL_PART = createURI("hasLogicalPart");

    /** Property nie:hasPart. */
    public static final URI HAS_PART = createURI("hasPart");

    /** Property nie:identifier. */
    public static final URI IDENTIFIER = createURI("identifier");

    /** Property nie:informationElementDate. */
    public static final URI INFORMATION_ELEMENT_DATE = createURI("informationElementDate");

    /** Property nie:interpretedAs. */
    public static final URI INTERPRETED_AS = createURI("interpretedAs");

    /** Property nie:isLogicalPartOf. */
    public static final URI IS_LOGICAL_PART_OF = createURI("isLogicalPartOf");

    /** Property nie:isPartOf. */
    public static final URI IS_PART_OF = createURI("isPartOf");

    /** Property nie:isStoredAs. */
    public static final URI IS_STORED_AS = createURI("isStoredAs");

    /** Property nie:keyword. */
    public static final URI KEYWORD = createURI("keyword");

    /** Property nie:language. */
    public static final URI LANGUAGE = createURI("language");

    /** Property nie:lastRefreshed. */
    public static final URI LAST_REFRESHED = createURI("lastRefreshed");

    /** Property nie:legal. */
    public static final URI LEGAL = createURI("legal");

    /** Property nie:license. */
    public static final URI LICENSE = createURI("license");

    /** Property nie:licenseType. */
    public static final URI LICENSE_TYPE = createURI("licenseType");

    /** Property nie:links. */
    public static final URI LINKS = createURI("links");

    /** Property nie:mimeType. */
    public static final URI MIME_TYPE = createURI("mimeType");

    /** Property nie:plainTextContent. */
    public static final URI PLAIN_TEXT_CONTENT = createURI("plainTextContent");

    /** Property nie:relatedTo. */
    public static final URI RELATED_TO = createURI("relatedTo");

    /** Property nie:rootElementOf. */
    public static final URI ROOT_ELEMENT_OF = createURI("rootElementOf");

    /** Property nie:subject. */
    public static final URI SUBJECT = createURI("subject");

    /** Property nie:title. */
    public static final URI TITLE = createURI("title");

    /** Property nie:version. */
    public static final URI VERSION = createURI("version");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private NIE() {
    }

}
