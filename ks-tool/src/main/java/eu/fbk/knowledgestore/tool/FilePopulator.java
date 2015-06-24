package eu.fbk.knowledgestore.tool;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ixa.kaflib.KAFDocument;
import ixa.kaflib.WF;
import jersey.repackaged.com.google.common.collect.Lists;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.internal.CommandLine;
import eu.fbk.rdfpro.util.IO;

public class FilePopulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePopulator.class);

    public static void main(final String[] args) throws Throwable {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("ks-file-populator")
                    .withHeader("Uploads files for NAF and associated raw text into a KS instance")
                    .withOption("s", "server", "the URL of the KS instance", "URL",
                            CommandLine.Type.STRING, true, false, true)
                    .withOption("u", "username", "the KS username (if required)", "USER",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("p", "password", "the KS password (if required)", "PWD",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("r", "recursive", "recurse into sub-directories")
                    .withOption("n", "add-newlines",
                            "add newlines after each sentence in raw text")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.knowledgestore")).parse(args);

            final String serverURL = cmd.getOptionValue("s", String.class);
            final String username = Strings.emptyToNull(cmd.getOptionValue("u", String.class));
            final String password = Strings.emptyToNull(cmd.getOptionValue("p", String.class));
            final boolean recursive = cmd.hasOption("r");
            final boolean newlines = cmd.hasOption("n");
            final List<File> nafs = Lists.newArrayList();
            for (final File file : cmd.getArgs(File.class)) {
                listFiles(file, recursive, true, nafs);
            }

            final KnowledgeStore ks = Client.builder(serverURL).compressionEnabled(true)
                    .maxConnections(2).validateServer(false).build();
            try {
                final Session session;
                if (username != null && password != null) {
                    session = ks.newSession(username, password);
                } else {
                    session = ks.newSession();
                }
                upload(session, nafs, newlines);
                session.close();
            } finally {
                ks.close();
            }

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    private static void listFiles(final File fileOrDir, final boolean recursive,
            final boolean root, final List<File> result) {
        if (fileOrDir.isFile()) {
            result.add(fileOrDir);
        } else if (recursive || root) {
            for (final File child : fileOrDir.listFiles()) {
                final String name = child.getName();
                if (child.isDirectory() || name.endsWith(".naf") || name.endsWith(".naf.gz")
                        || name.endsWith(".xml") || name.endsWith(".xml.gz")) {
                    listFiles(child, recursive, false, result);
                }
            }
        }
    }

    private static void upload(final Session session, final List<File> nafs, final boolean newlines)
            throws IOException, OperationException {
        for (final File naf : nafs) {
            try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(naf.getAbsolutePath())))) {
                final String nafString = Joiner.on('\n').join(CharStreams.readLines(reader));
                final KAFDocument nafDoc = KAFDocument
                        .createFromStream(new StringReader(nafString));
                final URI rawURI = Data.getValueFactory().createURI(
                        Data.cleanIRI(nafDoc.getPublic().uri));
                final URI nafURI = Data.getValueFactory().createURI(rawURI.toString() + ".naf");
                final Representation nafRep = Representation.create(nafString);
                final Representation rawRep;
                if (!newlines) {
                    rawRep = Representation.create(nafDoc.getRawText());
                } else {
                    final StringBuilder raw = new StringBuilder(nafDoc.getRawText());
                    int sent = 1;
                    for (final WF wf : nafDoc.getWFs()) {
                        if (wf.getSent() != sent) {
                            sent = wf.getSent();
                            for (int i = wf.getOffset() - 1; i >= 0; --i) {
                                if (!Character.isWhitespace(raw.charAt(i))) {
                                    if (Character.isWhitespace(raw.charAt(i + 1))) {
                                        raw.setCharAt(i + 1, '\n');
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    rawRep = Representation.create(raw.toString());
                }
                session.upload(rawURI).representation(rawRep).exec();
                session.upload(nafURI).representation(nafRep).exec();
            } catch (final Throwable ex) {
                LOGGER.error("Failed to upload " + naf, ex);
            }
        }
    }

}
