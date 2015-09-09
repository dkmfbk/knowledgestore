package eu.fbk.knowledgestore.internal.jaxrs;

import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.rdf.HtmlRDF;
import eu.fbk.knowledgestore.internal.rdf.HtmlSparql;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.rdfpro.tql.TQL;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;

import javax.ws.rs.core.GenericType;

public final class Protocol {

    // Type declarations

    public static final GenericType<Stream<Record>> STREAM_OF_RECORDS //
    = new GenericType<Stream<Record>>() { /* empty */};

    public static final GenericType<Stream<Outcome>> STREAM_OF_OUTCOMES //
    = new GenericType<Stream<Outcome>>() { /* empty */};

    public static final GenericType<Stream<Statement>> STREAM_OF_STATEMENTS //
    = new GenericType<Stream<Statement>>() { /* empty */};

    public static final GenericType<Stream<BindingSet>> STREAM_OF_TUPLES //
    = new GenericType<Stream<BindingSet>>() { /* empty */};

    public static final GenericType<Stream<Boolean>> STREAM_OF_BOOLEANS //
    = new GenericType<Stream<Boolean>>() { /* empty */};

    // MIME types

    public static final String MIME_TYPES_RDF = "" //
            + "application/ld+json;charset=UTF-8," //
            + "application/rdf+xml;charset=UTF-8," //
            + "application/n-triples;charset=UTF-8," //
            + "text/x-nquads;charset=UTF-8," //
            + "application/x-tql;charset=UTF-8," //
            + "text/turtle;charset=UTF-8," //
            + "text/n3;charset=UTF-8," //
            + "application/trix;charset=UTF-8," //
            + "application/x-trig;charset=UTF-8," //
            + "application/x-binary-rdf," //
            + "application/rdf+json;charset=UTF-8," //
            + "text/html;charset=UTF-8";

    public static final String MIME_TYPES_SPARQL_TUPLE = "" //
            + "application/sparql-results+json;charset=UTF-8," //
            + "application/sparql-results+xml;charset=UTF-8," //
            + "text/csv;charset=UTF-8," //
            + "text/tab-separated-values;charset=UTF-8," //
            + "application/x-binary-rdf-results-table," //
            + "text/html;charset=UTF-8";

    public static final String MIME_TYPES_SPARQL_BOOLEAN = "" //
            + "application/sparql-results+json;charset=UTF-8,"
            + "application/sparql-results+xml;charset=UTF-8," //
            + "text/boolean;charset=UTF-8," //
            + "text/html;charset=UTF-8";

    public static final String MIME_TYPES_ALL = MIME_TYPES_RDF + "," + MIME_TYPES_SPARQL_TUPLE
            + "," + MIME_TYPES_SPARQL_BOOLEAN;

    // Paths

    public static final String PATH_REPRESENTATIONS = "files";

    public static final String PATH_RESOURCES = "resources";

    public static final String PATH_MENTIONS = "mentions";

    public static final String PATH_ENTITIES = "entities";

    public static final String PATH_AXIOMS = "axioms";

    public static final String PATH_MATCH = "match";

    public static final String PATH_UPDATE = "sparqlupdate";

    public static final String PATH_SPARQL = "sparql";

    public static final String SUBPATH_COUNT = "count";

    public static final String SUBPATH_CREATE = "create";

    public static final String SUBPATH_MERGE = "merge";

    public static final String SUBPATH_UPDATE = "update";

    public static final String SUBPATH_DELETE = "delete";

    public static final String PARAMETER_PROBE = "probe";

    public static final String PARAMETER_ACCEPT = "accept";

    public static final String PARAMETER_TIMEOUT = "timeout";

    public static final String PARAMETER_ID = "id";

    public static final String PARAMETER_CONDITION = "condition";

    public static final String PARAMETER_PROPERTY = "property";

    public static final String PARAMETER_OFFSET = "offset";

    public static final String PARAMETER_LIMIT = "limit";

    public static final String PARAMETER_CRITERIA = "criteria";

    public static final String PARAMETER_QUERY = "query";

    public static final String PARAMETER_DEFAULT_GRAPH = "default-graph-uri";

    public static final String PARAMETER_NAMED_GRAPH = "named-graph-uri";

    public static final String HEADER_INVOCATION = "X-KS-Invocation";

    public static final String HEADER_CHUNKED = "X-KS-Chunked";

    public static final String HEADER_META = "X-KS-Content-Meta";

    public static final String[] HTTPS_PROTOCOLS = new String[] { "TLSv1" };

    public static final String[] HTTPS_CIPHER_SUITES = new String[] {
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
            "TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA",
            "TLS_DHE_DSS_WITH_AES_128_CBC_SHA", "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA" };

    public static String pathFor(final URI type) {
        if (KS.RESOURCE.equals(type)) {
            return PATH_RESOURCES;
        } else if (KS.MENTION.equals(type)) {
            return PATH_MENTIONS;
        } else if (KS.ENTITY.equals(type)) {
            return PATH_ENTITIES;
        } else if (KS.AXIOM.equals(type)) {
            return PATH_AXIOMS;
        } else if (KS.REPRESENTATION.equals(type)) {
            return PATH_REPRESENTATIONS;
        }
        throw new IllegalArgumentException("Invalid type " + type);
    }

    public static URI typeFor(final String path) {
        if (path.equals(PATH_RESOURCES)) {
            return KS.RESOURCE;
        } else if (path.equals(PATH_MENTIONS)) {
            return KS.MENTION;
        } else if (path.equals(PATH_ENTITIES)) {
            return KS.ENTITY;
        } else if (path.equals(PATH_AXIOMS)) {
            return KS.AXIOM;
        } else if (path.equals(PATH_REPRESENTATIONS)) {
            return KS.REPRESENTATION;
        }
        throw new IllegalArgumentException("Invalid path " + path);
    }

    static {
        try {
            TQL.register();
        } catch (Throwable ex) {
            // ignore: RDFpro TQL lib not available
        }
        HtmlRDF.register();
        HtmlSparql.register();
    }

}
