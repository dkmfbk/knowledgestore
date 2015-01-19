package eu.fbk.knowledgestore.client;

import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.vocabulary.KS;

public class ClientExample {

    private static final String SERVER_URL = "http://localhost:8080/";

    private static final String USERNAME = "ks";

    private static final String PASSWORD = "kspass";

    private static final String RECORD_RESOURCE = "resources.ttl";

    private static final Map<String, String> FILE_RESOURCES = ImmutableMap
            .<String, String>builder() //
            .put("http://www.newsreader-project.eu/2013/4/30/58B2-1JT1-DYTM-916F.xml",
                    "58B2-1JT1-DYTM-916F.xml") //
            .put("http://www.newsreader-project.eu/2013/4/30/58B2-1JT1-DYTM-916F.naf",
                    "58B2-1JT1-DYTM-916F.naf") //
            .put("http://www.newsreader-project.eu/2013/4/30/589F-S2Y1-JD34-V1MC.xml",
                    "589F-S2Y1-JD34-V1MC.xml") //
            .put("http://www.newsreader-project.eu/2013/4/30/589F-S2Y1-JD34-V1MC.naf",
                    "589F-S2Y1-JD34-V1MC.naf") //
            .put("http://www.newsreader-project.eu/2013/4/30/58B2-1JT1-DYTM-9159.xml",
                    "58B2-1JT1-DYTM-9159.xml") //
            .put("http://www.newsreader-project.eu/2013/4/30/58B2-1JT1-DYTM-9159.naf",
                    "58B2-1JT1-DYTM-9159.naf") //
            .build();

    public static void main(final String... args) throws OperationException {

        // Initialize a KnowledgeStore client
        final KnowledgeStore store = Client.builder(SERVER_URL).maxConnections(2)
                .validateServer(false).build();

        try {
            // Acquire a session for a given username/password pair
            final Session session = store.newSession(USERNAME, PASSWORD);

            // Clear all resources stored in the KS
            session.delete(KS.RESOURCE).exec();

            // Store resource records, in a single operation
            final List<Record> records = loadRecords(RECORD_RESOURCE);
            session.merge(KS.RESOURCE).criteria("overwrite *").records(records).exec();

            // Store resource files, one at a time
            for (final Map.Entry<String, String> entry : FILE_RESOURCES.entrySet()) {
                final URI resourceID = Data.getValueFactory().createURI(entry.getKey());
                final Representation representation = loadRepresentations(entry.getValue());
                try {
                    session.upload(resourceID).representation(representation).exec();
                } finally {
                    representation.close();
                }
            }

            // Count and print the number of resources in the KS
            final long numResources = session.count(KS.RESOURCE).exec();
            System.out.println(numResources + " resources in the KS");

            // Count and print the number of files in the KS
            long numFiles = 0L;
            for (final String id : FILE_RESOURCES.keySet()) {
                final URI resourceID = Data.getValueFactory().createURI(id);
                final Representation storedRepresentation = session.download(resourceID).exec();
                if (storedRepresentation != null) {
                    storedRepresentation.close();
                    ++numFiles;
                }
            }
            System.out.println(numFiles + " files in the KS");

            // Close the session
            session.close();

        } finally {
            // Ensure to close the KS (will also close pending sessions)
            store.close();
        }
    }

    private static List<Record> loadRecords(final String resourceName) {
        final InputStream in = ClientExample.class.getResourceAsStream(resourceName);
        try {
            final RDFFormat format = RDFFormat.forFileName(resourceName);
            final Stream<Statement> stmt = RDFUtil.readRDF(in, format, null, null, false);
            return Record.decode(stmt, ImmutableList.of(KS.RESOURCE, KS.MENTION), false).toList();
        } finally {
            Util.closeQuietly(in);
        }
    }

    private static Representation loadRepresentations(final String resourceName) {
        final URL url = ClientExample.class.getResource(resourceName);
        return Representation.create(url);
    }

}
