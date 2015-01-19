package eu.fbk.knowledgestore.server.http.jaxrs;

import eu.fbk.knowledgestore.vocabulary.KS;

// @Path("/" + Protocol.PATH_AXIOMS)
public class Axioms extends Crud {

    public Axioms() {
        super(KS.AXIOM);
    }

}
