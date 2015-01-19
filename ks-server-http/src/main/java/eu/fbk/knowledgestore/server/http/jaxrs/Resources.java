package eu.fbk.knowledgestore.server.http.jaxrs;

import javax.ws.rs.Path;

import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Provide access to resources.
 * <p style="color: red">
 * DOCUMENTATION COMING SOON
 * </p>
 */
@Path("/" + Protocol.PATH_RESOURCES)
public class Resources extends Crud {

    public Resources() {
        super(KS.RESOURCE);
    }

}
