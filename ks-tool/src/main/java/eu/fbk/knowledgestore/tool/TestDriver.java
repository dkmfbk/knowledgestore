package eu.fbk.knowledgestore.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import eu.fbk.knowledgestore.Operation.Sparql;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.CommandLine;
import eu.fbk.knowledgestore.internal.Logging;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

public final class TestDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDriver.class);

    private static final ValueFactory FACTORY = ValueFactoryImpl.getInstance();

    private final String url; // test.url

    private final String username; // test.username

    private final String password; // test.password

    private final int warmupMixes; // test.warmupmixes

    private final int testMixes; // test.testmixes

    private final long warmupTime; // test.warmuptime

    private final long testTime; // test.testtime

    private final int clients; // test.clients

    private final long timeout; // test.timeout

    private final Query[] queries; // test.queries

    private final List<String> outputVariables;

    private List<String> inputVariables;

    private final byte[][] inputData;

    private final File outputFile;

    private final long seed;

    public static void main(final String... args) {
        try {
            // MDC.put(Logging.MDC_CONTEXT, "main");

            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("ks-test-driver")
                    .withHeader(
                            "Perform a query scalability test against a KnowledgeStore. "
                                    + "Test parameters and queries are supplied in a .properties file. "
                                    + "Test data (produced with query-test-generator) "
                                    + "is supplied in a .tsv file.")
                    .withOption("c", "config", "the configuration file", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withFooter(
                            "Test configuration may be overridden by supplying additional "
                                    + "property=value\narguments on the command line.")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.nwrtools")).parse(args);

            final File configFile = cmd.getOptionValue("c", File.class);

            final Properties config = new Properties();
            try (InputStream configStream = IO.read(configFile.getAbsolutePath())) {
                config.load(configStream);
            }

            for (final String arg : cmd.getArgs(String.class)) {
                final int index = arg.indexOf('=');
                if (index > 0) {
                    final String name = arg.substring(0, index);
                    final String value = arg.substring(index + 1);
                    config.setProperty(name, value);
                }
            }

            new TestDriver(config, configFile.getParentFile()).run();

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    public TestDriver(final Properties properties, @Nullable final File baseDir)
            throws IOException {

        // Get base path
        final Path base = (baseDir != null ? baseDir : new File(System.getProperty("user.dir")))
                .toPath();

        // Parse seed
        this.seed = TestUtil.read(properties, "test.seed", Long.class, 0L);

        // Parse file names
        final String dataArg = TestUtil.read(properties, "test.data", String.class);
        final String outputArg = TestUtil.read(properties, "test.out", String.class);
        final File dataFile = base.resolve(Paths.get(dataArg)).toFile();
        this.outputFile = outputArg == null ? null : base.resolve(Paths.get(outputArg)).toFile();

        // Parse server URL, username and password
        this.url = TestUtil.read(properties, "test.url", String.class);
        this.username = TestUtil.read(properties, "test.username", String.class, null);
        this.password = TestUtil.read(properties, "test.password", String.class, null);
        LOGGER.info("SUT: {}{}", this.url,
                this.username == null && this.password == null ? " (anonymous access)"
                        : " (authenticated access)");

        // Parse number of mixes, max times and client counts
        this.warmupMixes = TestUtil.read(properties, "test.warmupmixes", Integer.class, 0);
        this.testMixes = TestUtil.read(properties, "test.testmixes", Integer.class, 1);
        this.warmupTime = TestUtil.read(properties, "test.warmuptime", Long.class, 3600L) * 1000;
        this.testTime = TestUtil.read(properties, "test.testtime", Long.class, 3600L) * 1000;
        this.clients = TestUtil.read(properties, "test.clients", Integer.class, 1);
        LOGGER.info("{} mix(es), {} s warmup; {} mix(es), {} s test; {} client(s)",
                this.warmupMixes, this.warmupTime / 1000, this.testMixes, this.testTime / 1000,
                this.clients);

        // Parse default timeout
        this.timeout = TestUtil.read(properties, "test.timeout", Long.class, -1L);

        // Parse test data
        Preconditions.checkArgument(dataFile.exists(), "File " + dataFile + " does not exist");
        final List<byte[]> data = Lists.newArrayList();
        try (BufferedReader reader = new BufferedReader(IO.utf8Reader(IO.buffer(IO.read(dataFile
                .getAbsolutePath()))))) {
            String line = reader.readLine();
            final String[] inputVariables = line.split("\t");
            for (int i = 0; i < inputVariables.length; ++i) {
                inputVariables[i] = inputVariables[i].substring(1);
            }
            this.inputVariables = ImmutableList.copyOf(inputVariables);
            LOGGER.info("Input schema: ({})", Joiner.on(", ").join(this.inputVariables));
            final Tracker tracker = new Tracker(LOGGER, null, //
                    "Parsed " + dataFile + ": %d tuples (%d tuple/s avg)", //
                    "Parsed %d tuples (%d tuple/s, %d tuple/s avg)");
            tracker.start();
            while ((line = reader.readLine()) != null) {
                data.add(line.getBytes(Charsets.UTF_8));
                tracker.increment();
            }
            tracker.end();
        }
        this.inputData = data.toArray(new byte[data.size()][]);

        // Parse queries
        final Properties defaultQueryProperties = new Properties();
        if (this.timeout >= 0) {
            defaultQueryProperties.setProperty("timeout", Long.toString(this.timeout));
        }
        final List<Query> allQueries = Query.create(properties, defaultQueryProperties);
        final List<Query> enabledQueries = Lists.newArrayList();
        final Set<String> enabledNames = Sets.newLinkedHashSet(Arrays.asList(TestUtil.read(
                properties, "test.queries", String.class).split("\\s*[,]\\s*")));
        for (final String name : enabledNames) {
            boolean added = false;
            for (final Query query : allQueries) {
                if (query.getName().equals(name)) {
                    enabledQueries.add(query);
                    added = true;
                    break;
                }
            }
            Preconditions.checkArgument(added, "Unknown query " + name);
        }
        this.queries = enabledQueries.toArray(new Query[enabledQueries.size()]);
        LOGGER.info("{} queries enabled ({} defined): {}", enabledQueries.size(),
                allQueries.size(), Joiner.on(", ").join(enabledQueries));

        // Check query variables and build list of output variables
        final List<String> outputVariables = Lists.newArrayList("mix.client", "mix.index",
                "mix.input", "mix.start", "mix.time");
        for (final String variable : this.inputVariables) {
            outputVariables.add("input." + variable);
        }
        for (final Query query : this.queries) {
            for (final String inputVariable : query.getInputVariables()) {
                if (!this.inputVariables.contains(inputVariable)) {
                    throw new IllegalArgumentException("Query " + query
                            + " refers to unknown input variable " + inputVariable);
                }
            }
            for (final String outputVariable : Ordering.natural().immutableSortedCopy(
                    query.getOutputVariables())) {
                outputVariables.add(query.getName() + "." + outputVariable);
            }
        }
        this.outputVariables = ImmutableList.copyOf(outputVariables);
        LOGGER.info("Output schema: {} attributes", this.outputVariables.size());
    }

    public void run() throws Throwable {

        // Open writer if possible
        final Writer writer = this.outputFile == null ? null : IO.utf8Writer(IO.buffer(IO
                .write(this.outputFile.getAbsolutePath())));

        try {
            // Allocate a random number generator
            final Random random = new Random(this.seed);

            // Take a timestamp and allocate data structure for computing statistics
            final long ts = System.currentTimeMillis();
            final List<String> queryNames = Lists.newArrayList();
            for (final Query query : this.queries) {
                queryNames.add(query.getName());
            }
            final Statistics stats = new Statistics(queryNames);

            // Log test started
            LOGGER.info("Test started");

            // Perform warmup (if enabled)
            if (this.warmupMixes > 0) {
                runClients(this.warmupMixes, this.warmupTime, random, "Warmup", null, null);
            }

            // Perform the real test (if enabled)
            if (this.clients > 0 && this.testMixes > 0) {
                if (writer != null) {
                    for (int i = 0; i < this.outputVariables.size(); ++i) {
                        writer.write(i == 0 ? "?" : "\t?");
                        writer.write(this.outputVariables.get(i));
                    }
                    writer.write("\n");
                }
                runClients(this.testMixes, this.testTime, random, "Measurement", writer, stats);
            }

            // Log test completion
            LOGGER.info("Test completed in {} ms\n\n{}\n", System.currentTimeMillis() - ts, stats);

        } finally {
            // Close TSV file
            IO.closeQuietly(writer);
        }
    }

    private void runClients(final int maxMixes, final long maxTime, final Random random,
            final String phaseName, @Nullable final Writer writer, @Nullable final Statistics stats)
            throws Throwable {

        // Log start
        LOGGER.info("{} started ({} clients, {} mix(es), {} queries/mix)", phaseName,
                this.clients, maxMixes, this.queries.length);

        // Create a Tracker to track the progress of the process
        final Tracker tracker = new Tracker(LOGGER, null,
                "Completed %d query mixes (%d mixes/s avg)",
                "Completed %d query mixes (%d mixes/s, %d mixes/s avg)");
        tracker.start();

        // Start a thread for each concurrent client
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        final Thread mainThread = Thread.currentThread();
        final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setDaemon(true).setNameFormat("client-%02d").build());
        final AtomicLong startTimestamp = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong endTimestamp = new AtomicLong(Long.MIN_VALUE);
        final long[] clientExecutionTimes = new long[this.clients];
        final int[] clientMixes = new int[this.clients];
        try {
            final AtomicInteger globalMixCounter = new AtomicInteger(maxMixes);
            for (int i = 0; i < this.clients; ++i) {
                final int clientId = i;
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        final String oldContext = MDC.get(Logging.MDC_CONTEXT);
                        try {
                            MDC.put(Logging.MDC_CONTEXT, String.format("client%d", clientId));
                            final AtomicInteger localMixCounter = new AtomicInteger(0);
                            final long startTs = System.currentTimeMillis();
                            synchronized (startTimestamp) {
                                if (startTs < startTimestamp.get()) {
                                    startTimestamp.set(startTs);
                                }
                            }
                            final long endTs = runClient(clientId, globalMixCounter,
                                    localMixCounter, maxTime, random, tracker, startTs, writer,
                                    stats);
                            clientExecutionTimes[clientId] = endTs - startTs;
                            clientMixes[clientId] = localMixCounter.get();
                            synchronized (endTimestamp) {
                                if (endTs > endTimestamp.get()) {
                                    endTimestamp.set(endTs);
                                }
                            }
                        } catch (final Throwable ex) {
                            LOGGER.error("[{}] Client failed", clientId, ex);
                            exceptionHolder.compareAndSet(null, ex);
                            mainThread.interrupt(); // Stop waiting for other threads
                        } finally {
                            MDC.put(Logging.MDC_CONTEXT, oldContext);
                        }
                    }

                });
            }
            executor.shutdown();
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);

        } catch (final InterruptedException ex) {
            final Throwable ex2 = exceptionHolder.get();
            if (ex2 != null) {
                ex.addSuppressed(ex2); // keep track of both exceptions
            }
            throw ex;

        } finally {
            executor.shutdownNow();
            tracker.end();
        }

        // Report exception, if any
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        // Compute and report elapsed time
        final long elapsed = endTimestamp.get() - startTimestamp.get();
        if (stats != null) {
            stats.reportElapsedTest(elapsed);
        }

        // Log completion
        LOGGER.info("{} completed in {} ms (client time: {}-{} ms; client mixes: {}-{})",
                phaseName, elapsed, Longs.min(clientExecutionTimes),
                Longs.max(clientExecutionTimes), Ints.min(clientMixes), Ints.max(clientMixes));
    }

    private long runClient(final int clientId, final AtomicInteger globalMixCounter,
            final AtomicInteger localMixCounter, final long maxTime, final Random random,
            @Nullable final Tracker tracker, final long startTimestamp,
            @Nullable final Writer writer, @Nullable final Statistics stats) throws IOException {

        // Log start
        LOGGER.debug("Client started");

        // Initialize a client and open a session with the SUT
        long timestamp = startTimestamp;
        try (Client client = Client.builder(this.url).compressionEnabled(true)
                .validateServer(false).build()) {
            try (final Session session = client.newSession(this.username, this.password)) {

                // Log connection acquired
                LOGGER.debug("Client ready");

                // Perform as many query mixes as requested
                final String clientContext = MDC.get(Logging.MDC_CONTEXT);
                try {
                    while (globalMixCounter.getAndDecrement() > 0
                            && timestamp < startTimestamp + maxTime) {
                        // Update MDC context
                        final int mixIndex = localMixCounter.incrementAndGet();
                        final String mixContext = String.format("%s.mix%d", clientContext,
                                mixIndex);
                        MDC.put(Logging.MDC_CONTEXT, mixContext);

                        // Pick up a random input tuple
                        final int index;
                        synchronized (random) {
                            index = random.nextInt(this.inputData.length);
                        }
                        final String inputLine = new String(this.inputData[index], Charsets.UTF_8);
                        final BindingSet input = TestUtil.decode(this.inputVariables, inputLine);

                        // Start building the output tuple adding data identifying this mix
                        final ValueFactory vf = Statements.VALUE_FACTORY;
                        final MapBindingSet output = new MapBindingSet();
                        final long mixStartTimestamp = timestamp;
                        output.addBinding("mix.client", vf.createLiteral(clientId));
                        output.addBinding("mix.index", vf.createLiteral(mixIndex));
                        output.addBinding("mix.input", vf.createLiteral(index));
                        output.addBinding("mix.start", vf.createLiteral(mixStartTimestamp));

                        // Log beginning of query mix
                        LOGGER.debug("Started for input #{}", index);

                        // Evaluate the queries of the mix, augmenting the output tuple
                        for (final Query query : this.queries) {
                            try {
                                MDC.put(Logging.MDC_CONTEXT,
                                        String.format("%s.%s", mixContext, query.getName()));
                                final MapBindingSet queryOutput = new MapBindingSet();
                                timestamp = query.evaluate(session, timestamp, input, queryOutput,
                                        stats);
                                for (final Binding binding : queryOutput) {
                                    output.addBinding(query.getName() + "." + binding.getName(),
                                            binding.getValue());
                                }
                            } finally {
                                MDC.put(Logging.MDC_CONTEXT, mixContext);
                            }
                        }
                        final long elapsed = timestamp - mixStartTimestamp;

                        // Log completion of query mix
                        LOGGER.debug("Completed in {} ms", elapsed);
                        if (tracker != null) {
                            tracker.increment();
                        }

                        // Store query mix time and update associated statistics, if supplied
                        output.addBinding("mix.time", vf.createLiteral(elapsed));
                        if (stats != null) {
                            stats.reportQueryMixCompletion(elapsed);
                        }

                        // Emit the output tuple if a Writer has been supplied
                        if (writer != null) {
                            LOGGER.trace("Emitting:\n{}",
                                    TestUtil.format(this.outputVariables, output, "\n"));
                            final String outputLine = TestUtil
                                    .encode(this.outputVariables, output);
                            synchronized (writer) {
                                writer.write(outputLine);
                                writer.write("\n");
                            }
                        }
                    }
                } finally {
                    MDC.put(Logging.MDC_CONTEXT, clientContext);
                }
            }
        }

        // Log end
        LOGGER.debug("Client terminated ({} query mixes)", localMixCounter.get());

        // Return end timestamp of last executed query
        return timestamp;
    }

    private static abstract class Query {

        private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

        private final String name;

        private final Long timeout;

        private final Set<String> inputVariables;

        private final Set<String> outputVariables;

        Query(final String name, final Properties properties,
                final Iterable<String> inputVariables, final Iterable<String> outputVariables) {

            final String timeout = properties.getProperty("timeout");

            this.name = name;
            this.timeout = timeout != null ? Long.parseLong(timeout) : null;
            this.inputVariables = ImmutableSet.copyOf(inputVariables);
            this.outputVariables = ImmutableSet.copyOf(Iterables.concat( //
                    ImmutableSet.of("start", "time", "error"), outputVariables));
        }

        public static List<Query> create(final Properties properties,
                final Properties defaultQueryProperties) {

            final Map<String, Properties> map = Maps.newLinkedHashMap();
            for (final Object key : properties.keySet()) {
                final String keyString = key.toString();
                final int index = keyString.indexOf(".");
                if (index > 0) {
                    final String queryName = keyString.substring(0, index);
                    final String propertyName = keyString.substring(index + 1);
                    final String propertyValue = properties.getProperty(keyString);
                    Properties queryProperties = map.get(queryName);
                    if (queryProperties == null) {
                        queryProperties = new Properties();
                        queryProperties.putAll(defaultQueryProperties);
                        map.put(queryName, queryProperties);
                    }
                    queryProperties.setProperty(propertyName, propertyValue);
                }
            }

            final List<Query> queries = Lists.newArrayList();
            for (final Map.Entry<String, Properties> entry : map.entrySet()) {
                final String queryName = entry.getKey();
                final Properties queryProperties = entry.getValue();
                final String queryType = queryProperties.getProperty("type");
                if ("download".equalsIgnoreCase(queryType)) {
                    queries.add(new DownloadQuery(queryName, queryProperties));
                } else if ("retrieve".equalsIgnoreCase(queryType)) {
                    queries.add(new RetrieveQuery(queryName, queryProperties));
                } else if ("lookup".equalsIgnoreCase(queryType)) {
                    queries.add(new LookupQuery(queryName, queryProperties));
                } else if ("lookupall".equalsIgnoreCase(queryType)) {
                    queries.add(new LookupAllQuery(queryName, queryProperties));
                } else if ("count".equalsIgnoreCase(queryType)) {
                    queries.add(new CountQuery(queryName, queryProperties));
                } else if ("sparql".equalsIgnoreCase(queryType)) {
                    queries.add(new SparqlQuery(queryName, queryProperties));
                }
            }
            return queries;
        }

        public String getName() {
            return this.name;
        }

        public Long getTimeout() {
            return this.timeout;
        }

        public Set<String> getInputVariables() {
            return this.inputVariables;
        }

        public Set<String> getOutputVariables() {
            return this.outputVariables;
        }

        public long evaluate(final Session session, final long startTimestamp,
                final BindingSet input, final MapBindingSet output,
                @Nullable final Statistics stats) {

            final ValueFactory vf = ValueFactoryImpl.getInstance();

            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Started: ");
                builder.append(TestUtil.format(this.inputVariables, input, " "));
                LOGGER.debug(builder.toString());
            }

            String error = "";
            try {
                doEvaluate(session, input, output);
            } catch (final Throwable ex) {
                error = ex.getClass().getSimpleName() + " - "
                        + Strings.nullToEmpty(ex.getMessage());
                LOGGER.warn("Got exception", ex);
            }

            long size = -1;
            try {
                final Value value = output.getValue("size");
                if (value != null) {
                    size = Long.parseLong(value.stringValue());
                }
            } catch (final Throwable ex) {
                // Ignore
            }

            final long endTimestamp = System.currentTimeMillis();
            final long elapsed = endTimestamp - startTimestamp;

            if (stats != null) {
                stats.reportQueryCompletion(this.name, !"".equals(error), elapsed, size);
            }

            output.addBinding("time", vf.createLiteral(elapsed));

            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("".equals(error) ? "Success" : "Failure");
                builder.append(": ");
                builder.append(TestUtil.format(this.inputVariables, input, " "));
                builder.append(" -> ");
                builder.append(TestUtil.format(this.outputVariables, output, " "));
                LOGGER.debug(builder.toString());
            }

            output.addBinding("start", vf.createLiteral(startTimestamp));
            output.addBinding("error", vf.createLiteral(error));

            return endTimestamp;
        }

        abstract void doEvaluate(Session session, BindingSet input, MapBindingSet output)
                throws Throwable;

        @Override
        public String toString() {
            return this.name;
        }

        private static class DownloadQuery extends Query {

            private final Template id;

            private final boolean caching;

            DownloadQuery(final String name, final Properties properties) {
                this(name, properties, new Template(properties.getProperty("id")));
            }

            private DownloadQuery(final String name, final Properties properties, final Template id) {
                super(name, properties, id.getVariables(), ImmutableList.of("size"));
                this.id = id;
                this.caching = "false".equalsIgnoreCase(properties.getProperty("caching"));
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final URI id = (URI) Statements.parseValue(this.id.instantiate(input),
                        Namespaces.DEFAULT);

                long size = 0L;
                try (final Representation representation = session.download(id)
                        .caching(this.caching).timeout(getTimeout()).exec()) {
                    if (representation != null) {
                        size = representation.writeToByteArray().length;
                    } else {
                        LOGGER.warn("No results for DOWNLOAD request, id " + id);
                    }
                } catch (final Throwable ex) {
                    throw new RuntimeException("Failed DOWNLOAD, id " + TestUtil.format(id)
                            + ", caching " + this.caching, ex);
                } finally {
                    output.addBinding("size", FACTORY.createLiteral(size));
                }
            }
        }

        private static class RetrieveQuery extends Query {

            private final URI layer;

            @Nullable
            private final Template condition;

            @Nullable
            private final Long offset;

            @Nullable
            private final Long limit;

            @Nullable
            private final List<URI> properties;

            RetrieveQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("condition")));
            }

            private RetrieveQuery(final String name, final Properties properties,
                    @Nullable final Template condition) {

                super(name, properties, condition.getVariables(), ImmutableList.of("size"));

                final String offset = properties.getProperty("offset");
                final String limit = properties.getProperty("limit");

                List<URI> props = null;
                if (properties.containsKey("properties")) {
                    props = Lists.newArrayList();
                    for (final String token : Splitter.onPattern("[ ,;]").omitEmptyStrings()
                            .trimResults().split(properties.getProperty("properties"))) {
                        props.add((URI) Statements.parseValue(token));
                    }
                }

                this.layer = (URI) Statements.parseValue(properties.getProperty("layer"),
                        Namespaces.DEFAULT);
                this.condition = condition;
                this.offset = offset == null ? null : Long.parseLong(offset);
                this.limit = limit == null ? null : Long.parseLong(limit);
                this.properties = props;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final String condition = Strings.nullToEmpty(this.condition.instantiate(input));

                long numTriples = 0L;
                try {
                    // FIXME: conditions do not seem to work
                    final Stream<Record> stream = session.retrieve(this.layer)
                            .condition(condition).offset(this.offset).limit(this.limit)
                            .properties(this.properties).exec();
                    numTriples = Record.encode(stream, ImmutableList.of(this.layer)).count();
                    if (numTriples == 0) {
                        LOGGER.warn("No results for RETRIEVE request, layer "
                                + TestUtil.format(this.layer) + ", condition '" + condition
                                + "', offset " + this.offset + ", limit " + this.limit);
                    }

                } catch (final Throwable ex) {
                    throw new RuntimeException("Failed RETRIEVE " + TestUtil.format(this.layer)
                            + ", condition " + condition + ", offset " + this.offset + ", limit"
                            + this.limit + ", properties " + this.properties, ex);
                } finally {
                    output.addBinding("size", FACTORY.createLiteral(numTriples));
                }
            }
        }

        private static class LookupQuery extends Query {

            private final URI layer;

            @Nullable
            private final Template id;

            @Nullable
            private final List<URI> properties;

            LookupQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("id")));
            }

            private LookupQuery(final String name, final Properties properties,
                    @Nullable final Template id) {

                super(name, properties, id.getVariables(), ImmutableList.of("size"));

                List<URI> props = null;
                if (properties.containsKey("properties")) {
                    props = Lists.newArrayList();
                    for (final String token : Splitter.onPattern("[ ,;]").omitEmptyStrings()
                            .trimResults().split(properties.getProperty("properties"))) {
                        props.add((URI) Statements.parseValue(token));
                    }
                }

                this.layer = (URI) Statements.parseValue(properties.getProperty("layer"),
                        Namespaces.DEFAULT);
                this.id = id;
                this.properties = props;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final URI id = (URI) Statements.parseValue(this.id.instantiate(input),
                        Namespaces.DEFAULT);

                long numTriples = 0L;
                try {
                    final Stream<Record> stream = session.retrieve(this.layer).ids(id)
                            .properties(this.properties).exec();
                    numTriples = Record.encode(stream, ImmutableList.of(this.layer)).count();
                    if (numTriples == 0) {
                        LOGGER.warn("No results for LOOKUP request, layer "
                                + TestUtil.format(this.layer) + ", id " + id);
                    }

                } catch (final Throwable ex) {
                    throw new RuntimeException("Failed LOOKUP " + TestUtil.format(this.layer)
                            + ", id " + TestUtil.format(id) + ", properties " + this.properties,
                            ex);
                } finally {
                    output.addBinding("size", FACTORY.createLiteral(numTriples));
                }
            }

        }

        private static class LookupAllQuery extends Query {

            @Nullable
            private final Template id;

            LookupAllQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("id")));
            }

            private LookupAllQuery(final String name, final Properties properties,
                    @Nullable final Template id) {
                super(name, properties, id.getVariables(), ImmutableList.of("size"));
                this.id = id;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final URI id = (URI) Statements.parseValue(this.id.instantiate(input),
                        Namespaces.DEFAULT);

                long numTriples = 0L;
                try {
                    numTriples += Record.encode(session.retrieve(KS.RESOURCE).ids(id).exec(),
                            ImmutableList.of(KS.RESOURCE)).count();
                    numTriples += Record.encode(
                            session.retrieve(KS.MENTION).condition("ks:mentionOf = $$", id)
                                    .limit(100000L).exec(), ImmutableList.of(KS.MENTION)).count();
                    if (numTriples == 0) {
                        LOGGER.warn("No results for LOOKUP ALL request, id " + id);
                    }
                } catch (final Throwable ex) {
                    throw new RuntimeException("Failed LOOKUP ALL, id " + TestUtil.format(id), ex);
                } finally {
                    output.addBinding("size", FACTORY.createLiteral(numTriples));
                }
            }
        }

        private static class CountQuery extends Query {

            private final URI layer;

            @Nullable
            private final Template condition;

            CountQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("condition")));
            }

            private CountQuery(final String name, final Properties properties,
                    @Nullable final Template condition) {

                super(name, properties, condition.getVariables(), ImmutableList.of("size"));

                this.layer = (URI) Statements.parseValue(properties.getProperty("layer"));
                this.condition = condition;
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                final String condition = Strings.nullToEmpty(this.condition.instantiate(input));
                long numResults = 0L;
                try {
                    numResults = session.count(this.layer).condition(condition).exec();
                    if (numResults == 0) {
                        LOGGER.warn("No results for COUNT request, layer "
                                + TestUtil.format(this.layer) + ", condition '" + condition + "'");
                    }
                } catch (final Throwable ex) {
                    throw new RuntimeException("Count " + TestUtil.format(this.layer) + " where "
                            + condition + " failed", ex);
                } finally {
                    output.addBinding("size", FACTORY.createLiteral(numResults));
                }
            }

        }

        private static final class SparqlQuery extends Query {

            private final Template query;

            private final String form;

            SparqlQuery(final String name, final Properties properties) {
                this(name, properties, Template.forString(properties.getProperty("query")));
            }

            private SparqlQuery(final String name, final Properties properties,
                    final Template query) {
                super(name, properties, query.getVariables(), ImmutableList.of("size"));
                this.query = query;
                this.form = detectQueryForm(query.getText());
            }

            private static String detectQueryForm(final String query) {

                final int length = query.length();

                int start = 0;
                while (start < length) {
                    final char ch = query.charAt(start);
                    if (ch == '#') { // comment
                        while (start < length && query.charAt(start) != '\n') {
                            ++start;
                        }
                    } else if (ch == 'p' || ch == 'b' || ch == 'P' || ch == 'B') { // prefix/base
                        while (start < length && query.charAt(start) != '>') {
                            ++start;
                        }
                    } else if (!Character.isWhitespace(ch)) { // found
                        break;
                    }
                    ++start;
                }

                for (int i = start; i < query.length(); ++i) {
                    final char ch = query.charAt(i);
                    if (Character.isWhitespace(ch)) {
                        final String form = query.substring(start, i).toLowerCase();
                        if (!"select".equals(form) && !"construct".equals(form)
                                && !"describe".equals(form) && !"ask".equals(form)) {
                            throw new IllegalArgumentException("Unknown query form: " + form);
                        }
                        return form;
                    }
                }

                throw new IllegalArgumentException("Cannot detect query form");
            }

            @Override
            void doEvaluate(final Session session, final BindingSet input,
                    final MapBindingSet output) throws Throwable {

                long numResults = 0;
                final String queryString = this.query.instantiate(input);
                final Sparql operation = session.sparql(queryString).timeout(getTimeout());

                try {
                    switch (this.form) {
                    case "select":
                        numResults = operation.execTuples().count();
                        break;
                    case "construct":
                    case "describe":
                        numResults = operation.execTriples().count();
                        break;
                    case "ask":
                        operation.execBoolean();
                        numResults = 1;
                        break;
                    default:
                        throw new Error();
                    }
                    if (numResults == 0) {
                        LOGGER.warn("No results for SPARQL request, query is\n" + queryString);
                    }
                    output.addBinding("size", FACTORY.createLiteral(numResults));
                } catch (final Throwable ex) {
                    throw new RuntimeException("Failed SPARQL, form " + this.form.toUpperCase()
                            + ", query:\n" + queryString, ex);
                } finally {
                }
            }

        }

        private static final class Template {

            private static final Pattern PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");

            private static Template EMPTY = new Template("");

            private final String text;

            private final String[] placeholderVariables;

            private final Set<String> variables;

            private Template(final String string) {
                Preconditions.checkNotNull(string);
                final List<String> variables = Lists.newArrayList();
                final StringBuilder builder = new StringBuilder();
                final Matcher matcher = PATTERN.matcher(string);
                int offset = 0;
                while (matcher.find()) {
                    builder.append(string.substring(offset, matcher.start()).replace("%", "%%"));
                    builder.append("%s");
                    variables.add(matcher.group(1));
                    offset = matcher.end();
                }
                builder.append(string.substring(offset).replace("%", "%%"));
                this.text = builder.toString();
                this.placeholderVariables = variables.toArray(new String[variables.size()]);
                this.variables = ImmutableSet.copyOf(variables);
            }

            static Template forString(@Nullable final String string) {
                return string == null ? EMPTY : new Template(string);
            }

            String getText() {
                return this.text;
            }

            Set<String> getVariables() {
                return this.variables;
            }

            String instantiate(final BindingSet bindings) {
                final Object[] placeholderValues = new String[this.placeholderVariables.length];
                for (int i = 0; i < placeholderValues.length; ++i) {
                    final Value value = bindings.getValue(this.placeholderVariables[i]);
                    placeholderValues[i] = Data.toString(value, null);
                }
                return String.format(this.text, placeholderValues);
            }

        }

    }

    private static final class Statistics {

        private static final String EMPTY = String.format("%-8s", "");

        private final DescriptiveStatistics queryMixTime;

        private final Map<String, QueryInfo> queryInfos;

        private final QueryInfo globalInfo;

        private long elapsedTime;

        public Statistics(final Iterable<String> queryNames) {
            this.queryMixTime = new DescriptiveStatistics();
            this.queryInfos = Maps.newLinkedHashMap();
            this.globalInfo = new QueryInfo();
            this.elapsedTime = 0L;
            for (final String queryName : queryNames) {
                this.queryInfos.put(queryName, new QueryInfo());
            }
        }

        public synchronized void reportQueryCompletion(final String queryName,
                final boolean failure, final long time, final long size) {
            final QueryInfo info = this.queryInfos.get(queryName);
            info.time.addValue(time);
            this.globalInfo.time.addValue(time);
            if (size >= 0) {
                info.size.addValue(size);
                this.globalInfo.size.addValue(size);
            }
            if (failure) {
                ++info.numFailures;
                ++this.globalInfo.numFailures;
            }
        }

        public synchronized void reportQueryMixCompletion(final long time) {
            this.queryMixTime.addValue(time);
        }

        public synchronized void reportElapsedTest(final long elapsedTime) {
            this.elapsedTime = elapsedTime;
        }

        @Override
        public synchronized String toString() {

            // Compute sum of query execution times
            long testTotalTime = 0;
            for (final QueryInfo info : this.queryInfos.values()) {
                testTotalTime += (long) info.time.getSum();
            }

            // Create and return a statistics table
            final StringBuilder builder = new StringBuilder();
            emitHeader(builder);
            emitSeparator(builder);
            for (final Map.Entry<String, QueryInfo> entry : this.queryInfos.entrySet()) {
                emitStats(builder, testTotalTime, entry.getKey(), entry.getValue());
            }
            emitSeparator(builder);
            emitStats(builder, testTotalTime, "query (avg)", this.globalInfo);
            emitSeparator(builder);
            emitStats(builder, testTotalTime, "query mix", (int) this.queryMixTime.getN(), -1,
                    null, this.queryMixTime);
            return builder.toString();
        }

        private void emitHeader(final StringBuilder builder) {

            builder.append(String.format("%-12s%-16s%-64s%-64s%-24s%-16s\n", "", "   Executions",
                    "     Result size [solutions, triples or bytes]", "     Execution time [ms]",
                    "     Total time [ms]", "    Rate"));

            builder.append(Strings.repeat(" ", 12));
            for (final String field : new String[] { "Total", "Error", "Min", "Q1", "Q2", "Q3",
                    "Max", "Geom", "Mean", "Std", "Min", "Q1", "Q2", "Q3", "Max", "Geom", "Mean",
                    "Std", "Sum", "Clock", "Share", "/Sec", "/Hour" }) {
                builder.append(String.format("%8s", field));
            }
            builder.append("\n");
        }

        private void emitSeparator(final StringBuilder builder) {
            builder.append(Strings.repeat("-", 8 * 23 + 12)).append("\n");
        }

        private void emitStats(final StringBuilder builder, final long testTotalTime,
                final String label, final QueryInfo info) {
            emitStats(builder, testTotalTime, label, (int) info.time.getN(), info.numFailures,
                    info.size, info.time);
        }

        private void emitStats(final StringBuilder builder, final long testTotalTime,
                final String label, final int numSuccesses, final int numFailures,
                @Nullable final DescriptiveStatistics size, final DescriptiveStatistics time) {

            builder.append(String.format("%-12s", label));

            builder.append(numSuccesses >= 0 ? String.format("%8d", numSuccesses) : EMPTY);
            builder.append(numFailures >= 0 ? String.format("%8d", numFailures) : EMPTY);

            if (size != null) {
                builder.append(String.format("%8d%8d%8d%8d%8d%8.0f%8.0f%8.0f",
                        (long) size.getMin(), (long) size.getPercentile(25),
                        (long) size.getPercentile(50), (long) size.getPercentile(75),
                        (long) size.getMax(), size.getGeometricMean(), size.getMean(),
                        size.getStandardDeviation()));
            } else {
                builder.append(Strings.repeat(EMPTY, 8));
            }

            if (time != null) {
                final long queryTotalTime = (long) time.getSum();
                final double share = (double) queryTotalTime / testTotalTime;
                final long elapsed = (long) (this.elapsedTime * share);
                final double rate = 1000.0 * time.getN() / elapsed;

                builder.append(String.format("%8d%8d%8d%8d%8d%8.0f%8.0f%8.0f",
                        (long) time.getMin(), (long) time.getPercentile(25),
                        (long) time.getPercentile(50), (long) time.getPercentile(75),
                        (long) time.getMax(), time.getGeometricMean(), time.getMean(),
                        time.getStandardDeviation()));

                builder.append(String.format("%8d%8d%8.2f", queryTotalTime, elapsed, share));
                builder.append(String.format("%8.2f%8.0f", rate, rate * 3600));

            } else {
                builder.append(Strings.repeat(EMPTY, 13));
            }

            builder.append("\n");
        }

        private static class QueryInfo {

            public final DescriptiveStatistics time = new DescriptiveStatistics();

            public final DescriptiveStatistics size = new DescriptiveStatistics();

            public int numFailures;

        }

    }

}
