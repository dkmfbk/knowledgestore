package eu.fbk.knowledgestore.client;

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.http.conn.EofSensorInputStream;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.vocabulary.KS;

public class KnowledgeStoreClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    // TODO: intercept client-side connection close

    @Ignore
    @Test
    public void test2() throws Throwable {
        final Set<String> namespaces = Sets.newHashSet();
        final String query = "" //
                + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" //
                + "select distinct ?t\n" //
                + "where {\n" //
                + "  { ?s a ?t } union { ?t a rdfs:Class } union { ?t a owl:Class }\n" //
                + "  union { ?t rdfs:subClassOf ?x } union { ?y rdfs:subClassOf ?t }\n" //
                + "  filter (isIRI(?t))" + "}";

        final KnowledgeStore store = Client.builder("http://localhost:8080/")
                .maxConnections(2).validateServer(false).build();
        try {
            final Session session = store.newSession();
            final Stream<URI> uris = session.sparql(query).execTuples()
                    .transform(URI.class, true, "t");
            int index = 0;
            for (final URI uri : uris) {
                namespaces.add(uri.getNamespace());
                ++index;
                if (index % 10000 == 0) {
                    System.out.println(index + " URIs found");
                }
            }
            System.out.println(Joiner.on('\n').join(Ordering.natural().sortedCopy(namespaces)));
            uris.close();

            final EofSensorInputStream x;

        } finally {
            store.close();
        }
    }

    @Test
    public void test() throws Throwable {
        final KnowledgeStore store = Client.builder("http://localhost:8080/")
                .maxConnections(2).validateServer(false).build();
        try {
            final URI exampleID = new URIImpl("ex:test");
            final URI resourceID = new URIImpl(
                    "http://www.newsreader-project.eu/2013/4/30/589F-S2Y1-JD34-V1MC.xml");

            final Session session = store.newSession("ks", "kspass");

            System.out.println(serialize(session.retrieve(KS.RESOURCE)
                    .condition("not(dct:creator = 'Francesco')").exec()));

            session.count(KS.RESOURCE).condition("not(dct:creator = 'Francesco')").exec();

            // System.err.println(session.upload(exampleID).representation(null).exec());

            session.delete(KS.RESOURCE).ids(exampleID).exec();

            final Record record = Record.create(exampleID, KS.RESOURCE).set(DCTERMS.CREATOR,
                    "Francesco");
            session.merge(KS.RESOURCE).records(record).criteria(Criteria.overwrite()).exec();

            System.out.println(session.sparql("select (count(*) as ?n) where { ?s ?p ?o }")
                    .execTuples().transform(Integer.class, true, "n").getUnique());
            System.out.println(session
                    .sparql("describe <http://dbpedia.org/resource/Michael_Schumacher>")
                    .execTriples().count());

            session.merge(KS.RESOURCE).records(record).criteria(Criteria.overwrite()).exec();

            System.out.println(session.sparql(
                    "ask { <http://dbpedia.org/resource/David_Beckam> ?p ?o }").execBoolean());

            final Representation representation = session.download(resourceID).exec();
            System.out.println(representation.writeToString());
            System.out.println(serialize(ImmutableList.of(representation.getMetadata())));

            session.download(new URIImpl("ex:does-not-exist")).exec();

            System.out.println(session
                    .upload(exampleID)
                    .representation(
                            Representation.create("Nel bel mezzo del cammin di nostra vita"))
                    .exec());
        } catch (final Throwable ex) {
            LOGGER.error("Test failed", ex);
            Throwables.propagate(ex);

        } finally {
            store.close();
        }
    }

    private String serialize(final Iterable<Record> records) {
        final StringBuilder builder = new StringBuilder();
        try {
            int index = 0;
            for (final Record record : records) {
                builder.append("(").append(++index)
                        .append(") -----------------------------------------------------\n");
                builder.append(record.toString(Data.getNamespaceMap(), true)).append("\n\n");
            }
            return builder.toString();
        } finally {
            Util.closeQuietly(records);
        }

        // final ByteArrayOutputStream out = new ByteArrayOutputStream();
        // RDFUtil.writeRDF(RDFFormat.TURTLE, out, Data.getNamespaceMap(),
        // Record.encode(Stream.create(records), ImmutableSet.<URI>of()));
        // return new String(out.toByteArray());
    }
}
