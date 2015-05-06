package eu.fbk.knowledgestore.client;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.openrdf.model.Statement;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;

public class ClientSparqlTest {

    private static final String NS = "http://dbpedia.org/resource/";

    private static final List<String> NAMES = ImmutableList.<String>of("Barack_Obama", "Europe",
            "Italy", "USA");

    public static void main(final String... args) throws Throwable {
        final String serverURL = "https://knowledgestore2.fbk.eu/nwr/dutchhouse";
        final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                .maxConnections(2).validateServer(false).build();
        final Session session = ks.newSession("nwr_partner", "ks=2014!");
        for (int i = 0; i < 1; ++i) {
            for (final String name : NAMES) {
                final List<Statement> triples = session.sparql("DESCRIBE <" + NS + name + ">")
                        .execTriples().toList();
                System.out.println(triples.size());
            }
        }
    }

}
