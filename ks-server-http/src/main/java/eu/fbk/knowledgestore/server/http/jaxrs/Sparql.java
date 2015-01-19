package eu.fbk.knowledgestore.server.http.jaxrs;

import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.collect.Sets;

import org.codehaus.enunciate.jaxrs.TypeHint;
import org.openrdf.model.URI;

import eu.fbk.knowledgestore.Operation;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;

/**
 * Provides the KnowledgeStore SPARQL endpoint.
 * <p style="color: red">
 * DOCUMENTATION COMING SOON
 * </p>
 */
@Path("/" + Protocol.PATH_SPARQL)
public class Sparql extends Resource {

    @GET
    @Produces(Protocol.MIME_TYPES_ALL)
    @TypeHint(Stream.class)
    public Response get(
            @QueryParam(Protocol.PARAMETER_DEFAULT_GRAPH) final List<String> defaultGraphs,
            @QueryParam(Protocol.PARAMETER_NAMED_GRAPH) final List<String> namedGraphs,
            @QueryParam(Protocol.PARAMETER_QUERY) final String query) throws OperationException {
        return query(query, defaultGraphs, namedGraphs);
    }

    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(Protocol.MIME_TYPES_ALL)
    @TypeHint(Stream.class)
    public Response postURLencoded(
            @FormParam(Protocol.PARAMETER_DEFAULT_GRAPH) final List<String> defaultGraphs,
            @FormParam(Protocol.PARAMETER_NAMED_GRAPH) final List<String> namedGraphs,
            @FormParam(Protocol.PARAMETER_QUERY) final String query) throws OperationException {
        return query(query, defaultGraphs, namedGraphs);
    }

    @POST
    @Consumes("application/sparql-query")
    @Produces(Protocol.MIME_TYPES_ALL)
    @TypeHint(Stream.class)
    public Response postDirect(
            @QueryParam(Protocol.PARAMETER_DEFAULT_GRAPH) final List<String> defaultGraphs,
            @QueryParam(Protocol.PARAMETER_NAMED_GRAPH) final List<String> namedGraphs,
            final String query) throws OperationException {
        return query(query, defaultGraphs, namedGraphs);
    }

    private Response query(final String query, final List<String> defaultGraphs,
            final List<String> namedGraphs) throws OperationException {

        // Check mandatory parameter
        checkNotNull(query, Outcome.Status.ERROR_INVALID_INPUT, "Missing query");

        // Prepare the SPARQL operation, returning an error if parameters are wrong
        final String form;
        final Operation.Sparql operation;
        try {
            form = RDFUtil.detectSparqlForm(query);
            operation = getSession().sparql(query) //
                    .timeout(getTimeout()) //
                    .defaultGraphs(parseGraphURIs(defaultGraphs)) //
                    .namedGraphs(parseGraphURIs(namedGraphs));
        } catch (final RuntimeException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT, "%s",
                    ex.getMessage()), ex);
        }

        // Select correct MIME type via negotiation, validate preconditions and handle probes
        final GenericType<?> type;
        if (form.equals("construct") || form.equals("describe")) {
            init(false, Protocol.MIME_TYPES_RDF, null, null);
            type = Protocol.STREAM_OF_STATEMENTS;
        } else if (form.equals("select")) {
            init(false, Protocol.MIME_TYPES_SPARQL_TUPLE, null, null);
            type = Protocol.STREAM_OF_TUPLES;
        } else {
            init(false, Protocol.MIME_TYPES_SPARQL_BOOLEAN, null, null);
            type = Protocol.STREAM_OF_BOOLEANS;
        }

        // Setup the result stream based on the selected type. Note that an empty result is
        // selected for HEAD methods, as only headers will be returned.
        Stream<?> entity;
        if (getMethod().equals(HttpMethod.HEAD)) {
            entity = Stream.create();
        } else if (type == Protocol.STREAM_OF_STATEMENTS) {
            entity = operation.execTriples();
        } else if (type == Protocol.STREAM_OF_TUPLES) {
            entity = operation.execTuples();
        } else if (type == Protocol.STREAM_OF_BOOLEANS) {
            entity = Stream.create(operation.execBoolean());
        } else {
            throw new Error("Unexpected type: " + type);
        }

        // Build and return the SPARQL response
        return newResponseBuilder(Status.OK, closeOnCompletion(entity), type).build();
    }

    private static Set<URI> parseGraphURIs(final List<String> strings) {
        if (strings == null || strings.isEmpty()) {
            return null;
        }
        final Set<URI> uris = Sets.newHashSetWithExpectedSize(strings.size());
        for (final String string : strings) {
            uris.add(Data.getValueFactory().createURI(string));
        }
        return uris;
    }

}
