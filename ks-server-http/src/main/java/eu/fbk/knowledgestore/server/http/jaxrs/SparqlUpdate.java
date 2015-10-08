package eu.fbk.knowledgestore.server.http.jaxrs;

import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.openrdf.model.Statement;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Created by alessio on 31/08/15.
 */

@Path("/" + Protocol.PATH_UPDATE)
public class SparqlUpdate extends Resource {

	@POST
	@Produces(Protocol.MIME_TYPES_ALL)
	@Consumes(Protocol.MIME_TYPES_RDF)
	@TypeHint(Stream.class)
	public Response post(final Stream<Statement> statements) throws OperationException {

		closeOnCompletion(statements);

		// Validate preconditions and handle probe requests here, before body is consumed
		// POST URI does not support GET, hence no tag and last modified
		init(true, null);

		Outcome outcome = getSession().sparqlupdate().statements(statements).exec();

		// Setup the response stream
		final int httpStatus = outcome.getStatus().getHTTPStatus();
		final Stream<Outcome> entity = Stream.create(outcome);

		// Stream the result to the client
		return newResponseBuilder(httpStatus, entity, Protocol.STREAM_OF_OUTCOMES).build();
	}

}
