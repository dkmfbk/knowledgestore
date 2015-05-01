package eu.fbk.knowledgestore.tool;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.CommandLine;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;
import eu.fbk.knowledgestore.vocabulary.SEM;
import eu.fbk.knowledgestore.vocabulary.TIME;

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
                download(session, outputFile, dumpResources, dumpMentions);
                session.close();
            } finally {
                ks.close();
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static void download(final Session session, final File file,
            final boolean dumpResources, final boolean dumpMentions) throws Throwable {

        final OutputStream baseStream = new BufferedOutputStream(new FileOutputStream(file));
        final OutputStream stream = file.getAbsolutePath().endsWith(".gz") ? new GZIPOutputStream(
                baseStream) : baseStream;

        try {
            final RDFFormat format = RDFFormat.forFileName(file.getAbsolutePath());
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

            final List<URI> types = Lists.newArrayList();
            if (dumpResources) {
                types.add(KS.RESOURCE);
            }
            if (dumpMentions) {
                types.add(KS.MENTION);
            }

            for (final URI type : types) {
                LOGGER.info("Downloading {} data", type.getLocalName().toLowerCase());
                session.retrieve(type).limit((long) Integer.MAX_VALUE)
                        .timeout(24 * 60 * 60 * 1000L).exec().toHandler(new Handler<Record>() {

                            private int records = 0;

                            private long triples = 0;

                            @Override
                            public void handle(final Record record) throws Throwable {
                                if (record == null || this.records > 0 && this.records % 1000 == 0) {
                                    LOGGER.info(this.records + " records, " + this.triples
                                            + " triples processed");
                                }
                                if (record != null) {
                                    final List<Statement> statements = Record.encode(
                                            Stream.create(record), ImmutableSet.of(type)).toList();
                                    writer.handleComment(type.getLocalName().toUpperCase() + " "
                                            + record.getID().toString());
                                    for (final Statement statement : statements) {
                                        writer.handleStatement(statement);
                                    }
                                    ++this.records;
                                    this.triples += statements.size();
                                }
                            }

                        });
            }

            writer.endRDF();

        } finally {
            stream.close();
        }
    }

}
