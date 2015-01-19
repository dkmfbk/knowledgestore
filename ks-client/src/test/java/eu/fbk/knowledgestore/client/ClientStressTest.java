package eu.fbk.knowledgestore.client;

import java.util.Date;

import com.google.common.collect.AbstractIterator;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.DCTERMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.vocabulary.KS;

public class ClientStressTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientStressTest.class);

    private static final String SERVER_URL = "http://localhost:8080/";

    private static final String USERNAME = "ks";

    private static final String PASSWORD = "kspass";

    private static final int NUM_RECORDS = 100;

    public static void main(final String... args) throws Throwable {

        // Initialize a KnowledgeStore client
        final KnowledgeStore store = Client.builder(SERVER_URL).maxConnections(2)
                .validateServer(false).build();

        try {
            // Acquire a session for a given username/password pair
            final Session session = store.newSession(USERNAME, PASSWORD);

            // Clear all resources stored in the KS
            session.delete(KS.RESOURCE).exec();
            session.delete(KS.MENTION).exec();

            // Upload records
            storeSingle(session);

            // Clear all resources stored in the KS
            session.delete(KS.RESOURCE).exec();

            // Close the session
            session.close();

        } finally {
            // Ensure to close the KS (will also close pending sessions)
            store.close();
        }
    }

    private static void storeSingle(final Session session) throws Throwable {
        final Stream<Record> records = createRecordStream();
        for (final Record record : records) {
            session.merge(KS.RESOURCE).criteria("overwrite *").records(record).exec();
        }
    }

    private static void storeBatch(final Session session) throws OperationException {
        session.merge(KS.RESOURCE).criteria("overwrite *").records(createRecordStream())
                .exec(new Handler<Outcome>() {

                    private boolean started = false;

                    @Override
                    public void handle(final Outcome outcome) throws Throwable {
                        if (outcome == null) {
                            LOGGER.info("Done receiving outcomes");

                        } else if (!this.started) {
                            LOGGER.info("Started receiving outcomes");
                            this.started = true;
                        }
                    }

                });
    }

    private static Stream<Record> createRecordStream() {
        return Stream.create(new AbstractIterator<Record>() {

            private int index = 0;

            @Override
            protected Record computeNext() {
                ++this.index;
                if (this.index > NUM_RECORDS) {
                    return endOfData();
                }
                if (this.index % 100 == 0) {
                    LOGGER.info("{} records generated", this.index);
                }
                final ValueFactory factory = Data.getValueFactory();
                final Record record = Record.create(factory.createURI("ex:resource" + this.index),
                        KS.RESOURCE);
                record.set(DCTERMS.TITLE, "Resource " + this.index);
                record.set(DCTERMS.CREATOR, "John Smith");
                record.set(DCTERMS.CREATED, new Date());
                // record.set(DCTERMS.ABSTRACT, Strings.repeat("... bla ...\n", 1000));
                return record;
            }

        });
    }

}
