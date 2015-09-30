package eu.fbk.knowledgestore.client;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

public class ClientSparqlTest {

    public static void main(final String... args) throws Throwable {
        final String serverURL = "http://localhost:9058/";
        final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                .maxConnections(2).validateServer(false).build();
        final Session session = ks.newSession();
        session.sparqldelete().statements(new StatementImpl(OWL.THING, RDF.TYPE, OWL.CARDINALITY)).exec();
    }

}
