package eu.fbk.knowledgestore.server.http.jaxrs;

import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.server.http.CustomConfig;
import eu.fbk.rdfpro.*;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;

import javax.annotation.Nullable;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 * Created by alessio on 28/09/15.
 */

@Path("/" + Protocol.PATH_CUSTOM)
public class Custom extends Resource {

	@POST
	@Path("/{customID}")
	@Produces(Protocol.MIME_TYPES_ALL)
	@Consumes(Protocol.MIME_TYPES_RDF)
	@TypeHint(Stream.class)
	public Response post(@PathParam("customID") String customID, @Nullable final Stream<Statement> statements) throws OperationException {

		init(true, null);

		CustomConfig customConfig = this.getApplication().getCustomConfigs().get(customID);
		if (customConfig == null) {
			throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT, "Custom operation %s not found", customID));
		}
		if (statements == null) {
			throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT, "No statements"));
		}

		String command = customConfig.getCommand();

		RDFSource source = RDFSources.wrap(statements);
		try {
			RDFProcessor processor = RDFProcessors.parse(true, command);
			processor.apply(source, RDFHandlers.NIL, 1);
		} catch (RDFHandlerException ex) {
			throw new OperationException(newOutcome(Outcome.Status.ERROR_UNEXPECTED, "%s", ex.getMessage()), ex);
		}

		return newResponseBuilder(Response.Status.OK, null, null).build();

//		System.out.println(command);
//		// Validate preconditions and handle probe requests here, before body is consumed
//		// POST URI does not support GET, hence no tag and last modified
//		init(true, null);
//
//		Outcome outcome = getSession().sparqldelete().statements(statements).exec();
//
//		// Setup the response stream
//		final int httpStatus = outcome.getStatus().getHTTPStatus();
//		final Stream<Outcome> entity = Stream.create(outcome);
//
//		// Stream the result to the client
//		return newResponseBuilder(httpStatus, entity, Protocol.STREAM_OF_OUTCOMES).build();
//		return null;
	}

}
