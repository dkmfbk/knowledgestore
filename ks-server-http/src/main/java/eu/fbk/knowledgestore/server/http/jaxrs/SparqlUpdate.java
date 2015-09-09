package eu.fbk.knowledgestore.server.http.jaxrs;

import eu.fbk.knowledgestore.OperationException;
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
		getSession().sparqlupdate().statements(statements).exec();
		return Response.ok().build();
	}

}
