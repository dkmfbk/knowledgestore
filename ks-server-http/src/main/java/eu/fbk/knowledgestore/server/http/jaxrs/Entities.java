package eu.fbk.knowledgestore.server.http.jaxrs;

import eu.fbk.knowledgestore.vocabulary.KS;

// @Path("/" + Protocol.PATH_ENTITIES)
public class Entities extends Crud {

    public Entities() {
        super(KS.ENTITY);
    }

}
