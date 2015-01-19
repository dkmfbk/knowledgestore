package eu.fbk.knowledgestore.server.http.jaxrs;

import javax.ws.rs.Path;

import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Provide access to mentions.
 * <p style="color: red">
 * DOCUMENTATION COMING SOON
 * </p>
 */
@Path("/" + Protocol.PATH_MENTIONS)
public class Mentions extends Crud {

    public Mentions() {
        super(KS.MENTION);
    }

}
