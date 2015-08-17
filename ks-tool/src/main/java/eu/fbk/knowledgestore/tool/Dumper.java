package eu.fbk.knowledgestore.tool;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.base.Throwables;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Dictionary;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Serializer;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.CommandLine;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;
import eu.fbk.knowledgestore.vocabulary.SEM;
import eu.fbk.knowledgestore.vocabulary.TIME;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Statements;

public class Dumper {

    private static Logger LOGGER = LoggerFactory.getLogger(Dumper.class);

    public static void main(final String[] args) throws Throwable {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("ks-dumper")
                    .withHeader("Downloads the contents of the resource " //
                            + "or mention layer as a single RDF file")
                    .withOption("s", "server", "the URL of the KS instance", "URL",
                            CommandLine.Type.STRING, true, false, true)
                    .withOption("u", "username", "the KS username (if required)", "USER",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("p", "password", "the KS password (if required)", "PWD",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("r", "resources", "dump resources data (default: false)")
                    .withOption("m", "mentions", "dump mentions data (default: false)")
                    .withOption("b", "binary", "use binary format (two files produced)")
                    .withOption("o", "output", "the output file", "FILE", CommandLine.Type.FILE,
                            true, false, true)
                    .withFooter(
                            "The RDF format and compression type is automatically detected based on the\n"
                                    + "file extension (e.g., 'tql.gz' = gzipped TQL file)")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.knowledgestore")).parse(args);

            final String serverURL = cmd.getOptionValue("s", String.class);
            final String username = Strings.emptyToNull(cmd.getOptionValue("u", String.class));
            final String password = Strings.emptyToNull(cmd.getOptionValue("p", String.class));
            final boolean dumpResources = cmd.hasOption("r");
            final boolean dumpMentions = cmd.hasOption("m");
            final boolean binary = cmd.hasOption("b");
            final File outputFile = cmd.getOptionValue("o", File.class);

            final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                    .maxConnections(2).validateServer(false).build();
            try {
                final Session session;
                if (username != null && password != null) {
                    session = ks.newSession(username, password);
                } else {
                    session = ks.newSession();
                }
                final Stream<Record> records = download(session, dumpResources, dumpMentions);
                if (binary) {
                    writeBinary(records, outputFile);
                } else {
                    writeRDF(records, outputFile);
                }
                session.close();
            } finally {
                ks.close();
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static Stream<Record> download(final Session session, final boolean dumpResources,
            final boolean dumpMentions) throws Throwable {

        final List<URI> types = Lists.newArrayList();
        if (dumpResources) {
            types.add(KS.RESOURCE);
        }
        if (dumpMentions) {
            types.add(KS.MENTION);
        }

        return Stream.concat(Stream.create(types).transform(
                (final URI type) -> {
                    LOGGER.info("Downloading {} data", type.getLocalName().toLowerCase());
                    try {
                        return session.retrieve(type).limit((long) Integer.MAX_VALUE)
                                .timeout(24 * 60 * 60 * 1000L).exec();
                    } catch (final Throwable ex) {
                        throw Throwables.propagate(ex);
                    }
                }, 1));
    }

    private static void writeRDF(final Stream<Record> records, final File file)
            throws RDFHandlerException, IOException {

        final RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath());
        final OutputStream stream = IO.write(file.getAbsolutePath());

        try {
            final RDFWriter writer = Rio.createWriter(format, stream);
            writer.startRDF();

            writer.handleNamespace("rdf", RDF.NAMESPACE);
            writer.handleNamespace("rdfs", RDFS.NAMESPACE);
            writer.handleNamespace("owl", OWL.NAMESPACE);
            writer.handleNamespace("ks", KS.NAMESPACE);
            writer.handleNamespace("nwr", NWR.NAMESPACE);
            writer.handleNamespace("nif", NIF.NAMESPACE);
            writer.handleNamespace("nfo", NFO.NAMESPACE);
            writer.handleNamespace("nie", NIE.NAMESPACE);
            writer.handleNamespace("sem", SEM.NAMESPACE);
            writer.handleNamespace("time", TIME.NAMESPACE);

            records.toHandler(new Handler<Record>() {

                private int records = 0;

                private long triples = 0;

                @Override
                public void handle(final Record record) throws Throwable {
                    if (record == null || this.records > 0 && this.records % 1000 == 0) {
                        LOGGER.info(this.records + " records, " + this.triples
                                + " triples processed");
                    }
                    if (record != null) {
                        final List<Statement> statements = Record.encode(Stream.create(record),
                                ImmutableSet.of()).toList();
                        writer.handleComment(record.getID().toString());
                        for (final Statement statement : statements) {
                            writer.handleStatement(statement);
                        }
                        ++this.records;
                        this.triples += statements.size();
                    }
                }

            });

            writer.endRDF();

        } finally {
            stream.close();
        }
    }

    private static void writeBinary(final Stream<Record> records, final File file)
            throws IOException {

        final String base = file.getAbsolutePath();
        final Dictionary<URI> dictionary = Dictionary.createLocalDictionary(URI.class, new File(
                base + ".dict"));
        final Serializer serializer = new Serializer(false, dictionary, Statements.VALUE_FACTORY);
        final CountingOutputStream stream = new CountingOutputStream(IO.write(base + ".gz"));
        try {
            records.toHandler(new Handler<Record>() {

                private int records = 0;

                @Override
                public void handle(final Record record) throws Throwable {
                    if (record == null || this.records > 0 && this.records % 1000 == 0) {
                        LOGGER.info("{} records, {} bytes processed ({} bytes/record)",
                                this.records, stream.getCount(), stream.getCount() / this.records);
                    }
                    if (record != null) {
                        serializer.toStream(stream, record);
                        ++this.records;
                    }
                }

            });

        } finally {
            IO.closeQuietly(records);
            IO.closeQuietly(stream);
        }
    }

}
