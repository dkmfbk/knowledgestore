package eu.fbk.knowledgestore.populator.rdf;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import eu.fbk.knowledgestore.Operation;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Compression;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.vocabulary.CKR;
import eu.fbk.knowledgestore.vocabulary.KS;

public final class RDFPopulator {

    private static final String VERSION = Util.getVersion("eu.fbk.knowledgestore",
            "ks-populator-rdf", "devel");

    private static final String HEADER = Util.getResource(RDFPopulator.class, "header").trim();

    private static final String FOOTER = Util.getResource(RDFPopulator.class, "footer").trim();

    private static final String DISCLAIMER = Util.getResource(RDFPopulator.class, "disclaimer")
            .trim();

    private static final Logger MAIN_LOGGER = LoggerFactory.getLogger(RDFPopulator.class);

    private static final Logger STATUS_LOGGER = LoggerFactory.getLogger("status");

    public static void main(final String... args) {
        try {
            // Parse command line, handling -h and -v commands
            final CommandLine cmd = parseCommandLine(args);

            // Extract command line options
            final String base = cmd.getOptionValue('b');
            final int parallelism = !cmd.hasOption('p') ? 1 : //
                    Integer.parseInt(cmd.getOptionValue('p'));
            final boolean listStdin = cmd.hasOption('@');
            final String listFile = cmd.getOptionValue('T');
            final List<String> sourceFiles = cmd.getArgList();
            final boolean sourceStdin = !cmd.hasOption('@') && !cmd.hasOption('T')
                    && cmd.getArgs().length == 0;
            final String sourceFormat = cmd.getOptionValue('s');
            final String errorFile = cmd.getOptionValue('e');
            final String target = cmd.getOptionValue('o');
            final String targetFormat = cmd.getOptionValue('t');
            final boolean validate = !cmd.hasOption('i');
            final Criteria criteria = !cmd.hasOption('c') ? Criteria.overwrite() : //
                    Criteria.parse(cmd.getOptionValue('c'), Data.getNamespaceMap());
            final URI globalURI = cmd.hasOption('g') ? (URI) Data.parseValue(
                    cmd.getOptionValue('g'), Data.getNamespaceMap()) : CKR.GLOBAL;
            final String credentials = cmd.getOptionValue('u');

            // Split username / password
            String username = null;
            String password = null;
            if (credentials != null) {
                final int index = credentials.indexOf(':');
                username = credentials.substring(0, index < 0 ? credentials.length() : index);
                password = index < 0 ? null : credentials.substring(index + 1);
            }

            // Select input files based on supplied options and arguments
            final List<File> sources = select(listStdin, listFile, sourceFiles, sourceStdin);
            for (final File file : sources) {
                checkFileParseable(file, sourceFormat);
            }

            // Setup axiom decoding
            final Stream<Record> axioms = decode(sources, globalURI, parallelism, base,
                    sourceFormat);

            // Handle 3 cases based on option -o
            if (target == null) {
                // (1) emit axioms to STDOUT
                disableLogging();
                final OutputStream out = System.out;
                System.setOut(new PrintStream(ByteStreams.nullOutputStream()));
                write(axioms, out, targetFormat);

            } else if (!target.startsWith("http://") && !target.startsWith("https://")) {
                // (2) emit axioms to FILE
                write(axioms, new File(target), targetFormat);

            } else {
                // (3) upload axioms to KS, emit rejected axioms to FILE / STDERR
                Session session = null;
                final Client client = Client.builder(target).maxConnections(2)
                        .validateServer(validate).build();
                try {
                    session = client.newSession(username, password);
                    final Stream<Record> rejected = upload(session, criteria, axioms);
                    if (errorFile == null) {
                        write(rejected, System.err, targetFormat);
                    } else {
                        write(rejected, new File(errorFile), targetFormat);
                    }
                } finally {
                    Util.closeQuietly(session);
                    client.close();
                }
            }

            // Signal success
            System.exit(0);

        } catch (final IllegalArgumentException ex) {
            // Signal wrong user input
            ex.printStackTrace();
            System.err.println("INVALID INPUT. " + ex.getMessage());
            System.exit(-1);

        } catch (final ParseException ex) {
            // Signal syntax error
            System.err.println("SYNTAX ERROR. " + ex.getMessage());
            System.exit(-1);

        } catch (final Throwable ex) {
            // Signal other error
            System.err.println("EXECUTION FAILED. " + ex.getMessage() + "\n");
            ex.printStackTrace();
            System.exit(-2);
        }
    }

    private static CommandLine parseCommandLine(final String... args) throws ParseException {

        // Define input options
        final List<Option> inputOpts = Lists.newArrayList();
        newOption(inputOpts, '@', "files-from-stdin", 0, false, null,
                "read names of input files from STDIN");
        newOption(inputOpts, 'T', "files-from", 1, false, "FILE",
                "read names of input files from FILE");
        newOption(inputOpts, 's', "source-format", 1, false, "FMT",
                "use input RDF format/compression FMT (eg: ttl.gz; default: "
                        + "autodetect based on file name)");
        newOption(inputOpts, 'b', "base", 1, false, "URI",
                "base URI for resolving parsed relative URIs");
        newOption(inputOpts, 'p', "parallel-files", 1, false, "N",
                "parse at most N files in parallel (default: 1)");

        // Define extraction options
        final List<Option> extractOpts = Lists.newArrayList();
        newOption(extractOpts, 'g', "global-uri", 1, false, "URI",
                "use URI in place of ckr:global (default: ckr:global)");
        newOption(extractOpts, 'd', "default", 1, false, "FILE",
                "augment axioms with default metadata/context in FILE");

        // Define output options
        final List<Option> outputOpts = Lists.newArrayList();
        newOption(outputOpts, 'o', "output", 1, false, "FILE|URL",
                "send axioms to FILE | server URL (default: STDOUT)");
        newOption(outputOpts, 'e', "error", 1, false, "FILE",
                "write non-uploaded axioms to FILE (default: STDERR)");
        newOption(outputOpts, 't', "target-format", 1, false, "FMT",
                "use output file RDF format/compression FMT (e.g., ttl.gz; "
                        + "default: autodetect based on file name)");
        newOption(outputOpts, 'u', "user", 1, false, "user[:pwd]",
                "upload using login user:pwd (default: anonymous)");
        newOption(outputOpts, 'i', "ignore-certificate", 0, false, null,
                "don't check server certificate (default: check)");
        newOption(outputOpts, 'c', "criteria", 1, false, "C",
                "upload with merge criteria C (default: overwrite *)");
        // -U|--proxy-user "user[:password]" proxy
        // -x|--proxy host:port

        // Define miscellaneous options
        final List<Option> miscOpts = Lists.newArrayList();
        newOption(miscOpts, 'h', "help", 0, false, null, "print this help message and exit");
        newOption(miscOpts, 'v', "version", 0, false, null, "print version information and exit");

        // Define combined option list
        final List<Option> allOpts = ImmutableList.copyOf(Iterables.concat(inputOpts, extractOpts,
                outputOpts, miscOpts));

        // Parse command line
        final CommandLine cmd = new GnuParser().parse(newOptions(allOpts), args);

        // Handle help and version commands
        if (cmd.hasOption('h')) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(new Comparator<Option>() {

                @Override
                public int compare(final Option option1, final Option option2) {
                    return allOpts.indexOf(option1) - allOpts.indexOf(option2);
                }

            });
            final PrintWriter out = new PrintWriter(System.out);
            formatter.printUsage(out, 80, "ksrdf [-o URL|FILE] [OPTIONS] [INPUT_FILE ...]");
            out.println();
            formatter.printWrapped(out, 80, HEADER);
            formatter.printWrapped(out, 80, DISCLAIMER);
            out.println("\nInput options:");
            formatter.printOptions(out, 80, newOptions(inputOpts), 2, 2);
            out.println("\nExtraction options:");
            formatter.printOptions(out, 80, newOptions(extractOpts), 2, 5);
            out.println("\nOutput options:");
            formatter.printOptions(out, 80, newOptions(outputOpts), 2, 2);
            out.println("\nMiscellaneous options:");
            formatter.printOptions(out, 80, newOptions(miscOpts), 2, 14);
            out.println();
            out.println(FOOTER);
            out.flush();
            System.exit(0);

        } else if (cmd.hasOption('v')) {
            System.out.println(String.format(
                    "ksrdf (FBK KnowledgeStore) %s\njava %s bit (%s) %s\n%s", VERSION,
                    System.getProperty("sun.arch.data.model"), System.getProperty("java.vendor"),
                    System.getProperty("java.version"), DISCLAIMER));
            System.exit(0);
        }

        // Return parsed options
        return cmd;
    }

    private static List<File> select(final boolean listStdin, final String listFile,
            final List<String> sourceFiles, final boolean sourceStdin) throws IOException {

        // Extract file names from non-option command line arguments
        final List<String> inputs = Lists.newArrayList(sourceFiles);

        // Extract file names from the file pointed by option -T, if any
        if (listFile != null) {
            final File file = new File(listFile);
            checkFileExist(file);
            for (final String line : Files.readLines(file, Charsets.UTF_8)) {
                final String trimmedLine = line.trim();
                if (!"".equals(trimmedLine)) {
                    inputs.add(line);
                }
            }
        }

        // Extract file names from STDIN, if option -@ has been specified
        if (listStdin) {
            for (final String line : CharStreams.readLines(new InputStreamReader(System.in))) {
                final String trimmedLine = line.trim();
                if (!"".equals(trimmedLine)) {
                    inputs.add(line);
                }
            }
        }

        // Convert to File objects and return the result
        final List<File> files = Lists.newArrayListWithCapacity(inputs.size());
        for (final String input : inputs) {
            files.add(new File(input));
        }

        // Add null in case STDIN should be included
        if (sourceStdin) {
            files.add(null);
        }
        return files;
    }

    private static Stream<Record> decode(final List<File> files, final URI globalURI,
            final int parallelism, @Nullable final String base, //
            @Nullable final String formatString) {

        // Determine source RDF format and compression based on format string
        final Compression compression = detectCompression(formatString, null);
        final RDFFormat format = detectRDFFormat(formatString, null);

        // Return a stream that read input RDF and decodes contained axioms
        return new Stream<Record>() {

            @Override
            protected void doToHandler(final Handler<? super Record> handler) throws Throwable {

                // Create the decoder
                final Decoder decoder = new Decoder(handler, globalURI);

                // Wrap the decoder in a RDFHandler
                RDFHandler rdfHandler = new RDFHandlerBase() {

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        emit(stmt);
                    }

                    @Override
                    public void endRDF() throws RDFHandlerException {
                        emit(null);
                    }

                    private void emit(@Nullable final Statement stmt) throws RDFHandlerException {
                        try {
                            decoder.handle(stmt);
                        } catch (final Throwable ex) {
                            Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                            Throwables.propagate(ex);
                        }
                    }

                };

                // Add logging
                rdfHandler = RDFUtil.newLoggingHandler(rdfHandler, STATUS_LOGGER, null,
                        "parsing: %d triples (%d triples/s, %d triples/s avg)", null);

                // Add decoupling queue to parallelize parsing and encoding
                rdfHandler = RDFUtil.newDecouplingHandler(rdfHandler, null);

                // Perform parallel parsing.
                final Map<File, RDFHandler> map = Maps.newLinkedHashMap();
                for (final File file : files) {
                    final RDFHandler fileHandler = RDFUtil.newLoggingHandler(rdfHandler,
                            MAIN_LOGGER, null, null,
                            "parsed " + (file == null ? "STDIN" : file.getAbsolutePath())
                                    + ": %d triples, (%d triples/s avg)");
                    map.put(file, fileHandler);
                }
                rdfHandler.startRDF();
                RDFUtil.readRDF(map, format, null, base, false, compression, parallelism);
                rdfHandler.endRDF();
                STATUS_LOGGER.info("");
            }

        };
    }

    private static Stream<Record> upload(final Session session, final Criteria criteria,
            final Stream<Record> axioms) {

        return axioms.transform(null, new Function<Handler<Record>, Handler<Record>>() {

            @Override
            public Handler<Record> apply(final Handler<Record> handler) {
                return new UploadHandler(session, criteria, handler);
            }

        });
    }

    private static void write(final Stream<Record> axioms, final OutputStream stream,
            @Nullable final String formatString) throws IOException {

        // Determine target RDF format and compression based on format string
        final Compression compression = detectCompression(formatString, Compression.NONE);
        final RDFFormat format = detectRDFFormat(formatString, null);
        if (format == null) {
            if (formatString == null) {
                throw new IllegalArgumentException(
                        "Must specify output format (-t) if writing to STDOUT");
            } else {
                throw new IllegalArgumentException("Cannot detect RDF format for " + formatString);
            }
        }

        // Setup compression, if necessary
        final OutputStream actualStream = compression.write(Data.getExecutor(), stream);

        // Performs writing
        RDFUtil.writeRDF(actualStream, format, Data.getNamespaceMap(), null,
                Record.encode(axioms, ImmutableSet.of(KS.AXIOM)));
    }

    private static void write(final Stream<Record> axioms, final File file,
            @Nullable final String formatString) throws IOException {

        // Determine target RDF format and compression based on format string
        Compression compression = detectCompression(file.getName(), null);
        if (compression == null) {
            compression = detectCompression(formatString, Compression.NONE);
        }
        RDFFormat format = detectRDFFormat(file.getName(), null);
        if (format == null) {
            format = detectRDFFormat(formatString, null);
        }
        if (format == null) {
            throw new IllegalArgumentException("Cannot detect RDF format of " + file);
        }

        // Setup compression, if necessary
        final OutputStream actualStream = compression.write(Data.getExecutor(), file);

        // Performs writing
        try {
            RDFUtil.writeRDF(actualStream, format, Data.getNamespaceMap(), null,
                    Record.encode(axioms, ImmutableSet.of(KS.AXIOM)));
        } finally {
            Util.closeQuietly(actualStream);
        }
    }

    private static Options newOptions(final Iterable<? extends Option> options) {
        final Options result = new Options();
        for (final Object option : options) {
            result.addOption((Option) option);
        }
        return result;
    }

    private static void newOption(final Collection<? super Option> options,
            @Nullable final Character shortName, final String longName, final int argCount,
            final boolean argOpt, @Nullable final String argName, final String description) {

        OptionBuilder.withLongOpt(longName);
        OptionBuilder.withDescription(description);
        if (argCount != 0) {
            OptionBuilder.withArgName(argName);
            if (argOpt) {
                if (argCount == 1) {
                    OptionBuilder.hasOptionalArg();
                } else if (argCount > 1) {
                    OptionBuilder.hasOptionalArgs(argCount);
                } else {
                    OptionBuilder.hasOptionalArgs();
                }
            } else {
                if (argCount == 1) {
                    OptionBuilder.hasArg();
                } else if (argCount > 1) {
                    OptionBuilder.hasArgs(argCount);
                } else {
                    OptionBuilder.hasArgs();
                }
            }
        }
        options.add(shortName == null ? OptionBuilder.create() : OptionBuilder.create(shortName));
    }

    private static RDFFormat detectRDFFormat(@Nullable final String string,
            final RDFFormat fallback) {
        return string == null ? fallback : RDFFormat.forFileName("dummy." + string.trim(),
                fallback);
    }

    private static Compression detectCompression(@Nullable final String string,
            final Compression fallback) {
        return string == null ? fallback : Compression.forFileName("dummy." + string.trim(),
                fallback);
    }

    private static void checkFileExist(@Nullable final File file) {
        if (file == null) {
            return;
        } else if (!file.exists()) {
            throw new IllegalArgumentException("File '" + file + "' does not exist");
        } else if (file.isDirectory()) {
            throw new IllegalArgumentException("Path '" + file + "' denotes a directory");
        }
    }

    private static void checkFileParseable(@Nullable final File file,
            @Nullable final String formatString) {
        if (file == null) {
            if (formatString == null) {
                throw new IllegalArgumentException("Cannot detect RDF format "
                        + "and compression of STDIN: please specify option -s");
            }
            return;
        }
        checkFileExist(file);
        final RDFFormat defaultFormat = detectRDFFormat(formatString, null);
        final Compression defaultCompression = detectCompression(formatString, null);
        final RDFFormat format = RDFFormat.forFileName(file.getName());
        if (format == null && defaultFormat == null) {
            throw new IllegalArgumentException("Unknown RDF format for file " + file);
        } else if (format != null && defaultFormat != null && !format.equals(defaultFormat)) {
            System.err.println("Warning: detected RDF format for file " + file
                    + " doesn't match specified format");
        }
        final Compression compression = Compression.forFileName(file.getName(), Compression.NONE);
        if (defaultCompression != null && !compression.equals(defaultCompression)) {
            System.err.println("Warning: detected compression format for file " + file
                    + " doesn't match specified format");
        }
    }

    private static void disableLogging() {
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        try {
            final JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();
            configurator.doConfigure(RDFPopulator.class.getResource("logback.disabled.xml"));
        } catch (final JoranException je) {
            // ignore
        }
    }

    private static final class UploadHandler implements Handler<Record> {

        private static final int BUFFER_SIZE = 1024;

        private final Session session;

        private final Criteria criteria;

        private final Handler<Record> errorHandler;

        private final Map<URI, Record> buffer;

        UploadHandler(final Session session, final Criteria criteria,
                final Handler<Record> errorHandler) {
            this.session = session;
            this.criteria = criteria;
            this.errorHandler = errorHandler;
            this.buffer = Maps.newHashMapWithExpectedSize(BUFFER_SIZE);
        }

        @Override
        public void handle(final Record axiom) throws Throwable {
            if (axiom == null) {
                flush(true);
            } else {
                this.buffer.put(axiom.getID(), axiom);
                if (this.buffer.size() == BUFFER_SIZE) {
                    flush(false);
                }
            }
        }

        private void flush(final boolean done) throws Throwable {
            if (!this.buffer.isEmpty()) {
                try {
                    final Operation.Merge operation = this.session.merge(KS.AXIOM)
                            .criteria(this.criteria).records(this.buffer.values());
                    operation.exec(new Handler<Outcome>() {

                        @Override
                        public void handle(final Outcome outcome) throws Throwable {
                            if (outcome.getStatus().isOK()) {
                                UploadHandler.this.buffer.remove(outcome.getObjectID());
                            }
                        }

                    });
                } catch (final Throwable ex) {
                    MAIN_LOGGER.error("Upload failure: " + ex.getMessage(), ex);
                }
                for (final Record record : this.buffer.values()) {
                    this.errorHandler.handle(record);
                }
            }
            if (done) {
                this.errorHandler.handle(null);
            }
        }

    }

}
