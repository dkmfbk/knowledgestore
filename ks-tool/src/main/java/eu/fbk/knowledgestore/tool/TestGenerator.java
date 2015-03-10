package eu.fbk.knowledgestore.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.client.Client;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.CommandLine;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Tracker;

public final class TestGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestGenerator.class);

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private final Dictionary dictionary;

    private final String url;

    private final String username;

    private final String password;

    private final int mixes;

    private final File outputFile;

    private final Query[] queries;

    public static void main(final String[] args) {
        try {
            final CommandLine cmd = CommandLine
                    .parser()
                    .withName("ks-test-generator")
                    .withHeader(
                            "Generates the request mixes for the test, by querying the "
                                    + "KnowledgeStore. Generator parameters and queries are "
                                    + "supplied in a .properties file. Output data is written "
                                    + "to a .tsv file.")
                    .withOption("c", "config", "the configuration file", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withFooter(
                            "Configuration parameters may be overridden by supplying additional "
                                    + "property=value\narguments on the command line.")
                    .withLogger(LoggerFactory.getLogger("eu.fbk.knowledgestore")).parse(args);

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

            new TestGenerator(config, configFile.getParentFile()).run();

        } catch (final Throwable ex) {
            CommandLine.fail(ex);
        }
    }

    public TestGenerator(final Properties properties, @Nullable final File basePath) {

        // Create a global dictionary for mapping values to codes and back
        this.dictionary = new Dictionary();

        // Get base path
        final Path base = (basePath != null ? basePath : new File(System.getProperty("user.dir")))
                .toPath();

        // Parse server URL, username and password
        this.url = TestUtil.read(properties, "test.url", String.class);
        this.username = TestUtil.read(properties, "test.username", String.class, null);
        this.password = TestUtil.read(properties, "test.password", String.class, null);
        LOGGER.info("SUT: {}{}", this.url,
                this.username == null && this.password == null ? " (anonymous access)"
                        : " (authenticated access)");

        // Parse number of mixes to generate and output file
        this.mixes = TestUtil.read(properties, "test.mixes", Integer.class, 0);
        this.outputFile = base.resolve(
                Paths.get(TestUtil.read(properties, "test.out", String.class))).toFile();
        LOGGER.info("{} mix(es) to be written to {}", this.mixes,
                this.outputFile.getAbsolutePath());

        // Parse queries
        final List<Query> allQueries = Query.create(properties, basePath);
        final List<Query> enabledQueries = Lists.newArrayList();
        final Set<String> enabledNames = Sets.newLinkedHashSet(Arrays.asList(TestUtil.read(
                properties, "test.queries", String.class).split("\\s*[,]\\s*")));
        for (final String name : enabledNames) {
            boolean added = false;
            for (final Query query : allQueries) {
                if (query.name.equals(name)) {
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
    }

    @SuppressWarnings("resource")
    public void run() throws IOException, OperationException {

        Client client = null;
        Session session = null;

        try {
            // Obtain a session
            client = Client.builder(this.url).compressionEnabled(true).validateServer(false)
                    .build();
            session = client.newSession(this.username, this.password);

            // Read schema and tuples from input files
            final List<List<String>> fileVars = Lists.newArrayList();
            final List<List<Tuple>> fileTuples = Lists.newArrayList();
            for (int i = 0; i < this.queries.length; ++i) {
                final List<String> vars = Lists.newArrayList();
                final List<Tuple> tuples = Lists.newArrayList();
                final File file = this.queries[i].download(session);
                read(file, vars, tuples, this.dictionary);
                fileVars.add(vars);
                fileTuples.add(tuples);
            }

            // Compute output schema and mappings from file to output schema
            final int[][] fileMappings = new int[this.queries.length][]; // m_ij -> var j file i
            final List<String> outputVars = Lists.newArrayList();
            for (int i = 0; i < fileVars.size(); ++i) {
                boolean insidePrefix = true;
                fileMappings[i] = new int[fileVars.get(i).size()];
                for (int j = 0; j < fileMappings[i].length; ++j) {
                    final String var = fileVars.get(i).get(j);
                    int index = outputVars.indexOf(var);
                    if (index < 0) {
                        insidePrefix = false;
                        index = outputVars.size();
                        outputVars.add(var);
                    } else if (!insidePrefix) {
                        throw new IllegalArgumentException("Variable " + var + " of query "
                                + this.queries[i] + " matches var in previous files "
                                + "but is preceded by newly intruduced variable ");
                    }
                    fileMappings[i][j] = index;
                }
            }
            LOGGER.info("Output schema: ({})", Joiner.on(", ").join(outputVars));

            // Use a tracker to display the progress of the operation
            final Tracker tracker = new Tracker(LOGGER, null, //
                    "Generated %d tuples (%d tuple/s avg)", //
                    "Generated %d tuples (%d tuple/s, %d tuple/s avg)");
            tracker.start();

            // Generate a set of (unique) joined tuples, of the size specified
            int numFailures = 0;
            int numDuplicates = 0;
            final Set<Tuple> outputTuples = Sets.newLinkedHashSet();
            final int[] outputCodes = new int[outputVars.size()];
            outer: while (outputTuples.size() < this.mixes) {
                Arrays.fill(outputCodes, 0);
                for (int i = 0; i < fileTuples.size(); ++i) {
                    if (!pick(fileTuples.get(i), fileMappings[i], outputCodes)) {
                        ++numFailures;
                        continue outer;
                    }
                }
                if (outputTuples.add(Tuple.create(outputCodes))) {
                    tracker.increment();
                } else {
                    ++numDuplicates;
                }
            }

            // Signal completion
            tracker.end();

            // Log number of failures and number of duplicate tuples during generation
            LOGGER.info("Tuple generation statistics: {} attempts failed, {} duplicates",
                    numFailures, numDuplicates);

            // Write resulting tuples
            write(this.outputFile, outputVars, outputTuples, this.dictionary);

        } finally {
            // Release session
            Util.closeQuietly(session);
            Util.closeQuietly(client);
        }
    }

    private static boolean pick(final List<Tuple> tuples, final int[] mappings,
            final int[] outputCodes) {

        final int numVariables = mappings.length;
        final int numTuples = tuples.size();

        // The a-priori range where to pick the tuple is the full tuples list
        int start = 0;
        int end = tuples.size();

        // Check if range can be constrained based on codes previously assigned (i.e., join)
        boolean constrained = false;
        final int[] searchCodes = new int[numVariables];
        for (int i = 0; i < numVariables; ++i) {
            final int code = outputCodes[mappings[i]];
            if (code != 0) {
                searchCodes[i] = code;
                constrained = true;
            }
        }

        // If range can be constrained, build a 'search' tuple whose first codes are given by
        // variables previously assigned, and remaining variables are zero; then do binary search
        // followed by a scan for matching tuples to determine the range
        if (constrained) {
            final Tuple searchTuple = Tuple.create(searchCodes);
            start = Collections.binarySearch(tuples, searchTuple);
            if (start < 0) {
                start = -start - 1; // in case exact match not found
            }
            if (start >= numTuples || !tuples.get(start).matches(searchTuple)) {
                return false; // if range is empty or cannot join
            }
            end = start + 1;
            while (end < numTuples && tuples.get(end).matches(searchTuple)) {
                ++end;
            }
        }

        // Pick a random index inside the allowed range and use that tuple to augment output
        final int chosenIndex = start + RANDOM.nextInt(end - start);
        final Tuple chosenTuple = tuples.get(chosenIndex);
        for (int i = 0; i < numVariables; ++i) {
            final int slot = mappings[i];
            final int oldValue = outputCodes[slot];
            final int newValue = chosenTuple.get(i);
            if (oldValue != 0 && newValue != oldValue) {
                throw new Error("Join error: " + chosenTuple + " - "
                        + Arrays.toString(outputCodes) + " (search:  "
                        + Arrays.toString(searchCodes) + "; start " + start + "; end " + end + ")");
            }
            outputCodes[mappings[i]] = chosenTuple.get(i);
        }

        // Return true upon success
        return true;
    }

    private static void read(final File file, final List<String> vars, final List<Tuple> tuples,
            final Dictionary dictionary) throws IOException {

        // Read the file specified, populating the supplied vars and tuples list
        try (final BufferedReader reader = new BufferedReader(IO.utf8Reader(IO.buffer(IO.read(file
                .getAbsolutePath()))))) {

            // Read variables
            for (final String token : reader.readLine().split("\t")) {
                vars.add(token.trim().substring(1));
            }

            // Use a tracker to show the progress of the operation
            final Tracker tracker = new Tracker(LOGGER, null, //
                    "Parsed " + file.getAbsolutePath() + " (" + Joiner.on(", ").join(vars)
                            + "): %d tuples (%d tuple/s avg)", //
                    "Parsed %d tuples (%d tuple/s, %d tuple/s avg)");
            tracker.start();

            // Read data tuples, mapping values to codes using the dictionary
            int lineNum = 0;
            String line;
            final int[] codes = new int[vars.size()];
            while ((line = reader.readLine()) != null) {
                try {
                    ++lineNum;
                    final String[] tokens = line.split("\t");
                    for (int j = 0; j < codes.length; ++j) {
                        codes[j] = dictionary.codeFor(tokens[j]);
                    }
                    tuples.add(Tuple.create(codes));
                    tracker.increment();
                } catch (final Throwable ex) {
                    LOGGER.warn("Ignoring invalid line " + lineNum + " of file " + file + " - "
                            + ex.getMessage() + " [" + line + "]");
                }
            }

            // Signal completion
            tracker.end();

            // Sort read tuples
            Collections.sort(tuples);
        }

    }

    private static void write(final File file, final List<String> vars,
            final Collection<Tuple> tuples, final Dictionary dictionary) throws IOException {

        // Use a tracker to show the progress of the operation
        final Tracker tracker = new Tracker(LOGGER, null, //
                "Written " + file.getAbsolutePath() + " (" + Joiner.on(", ").join(vars)
                        + "): %d tuples (%d tuple/s avg)", //
                "Written %d tuples (%d tuple/s, %d tuple/s avg)");
        tracker.start();

        // Write to the file specified one line at a time
        final int numVars = vars.size();
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(file.getAbsolutePath())))) {

            // Start writing the header line: ?v1 ?v2 ...
            for (int i = 0; i < numVars; ++i) {
                if (i > 0) {
                    writer.write("\t");
                }
                writer.write("?");
                writer.write(vars.get(i));
            }
            writer.write("\n");

            // Write data lines
            for (final Tuple tuple : tuples) {
                for (int i = 0; i < numVars; ++i) {
                    if (i > 0) {
                        writer.write("\t");
                    }
                    writer.write(dictionary.stringFor(tuple.get(i)));
                }
                writer.write("\n");
                tracker.increment();
            }
        }

        // Signal completion
        tracker.end();
    }

    private static class Query {

        private final String name;

        private final File file;

        private final String string;

        public Query(final String name, final File file, final String string) {
            this.name = name;
            this.file = file;
            this.string = string;
        }

        public File download(final Session session) throws IOException, OperationException {

            if (!this.file.exists()) {

                final AtomicReference<Writer> writerToClose = new AtomicReference<Writer>(null);

                final Tracker tracker = new Tracker(LOGGER, null, //
                        "Evaluated query " + this.name + ": %d tuples (%d tuple/s avg)", //
                        "Evaluating query " + this.name
                                + ": %d tuples (%d tuple/s, %d tuple/s avg)");
                tracker.start();

                try (Stream<BindingSet> stream = session.sparql(this.string).timeout(3600 * 1000L)
                        .execTuples()) {

                    stream.toHandler(new Handler<BindingSet>() {

                        private Writer writer = null;

                        private List<String> variables;

                        @SuppressWarnings("unchecked")
                        @Override
                        public void handle(final BindingSet bindings) throws Throwable {
                            if (this.writer == null) {
                                this.writer = IO.utf8Writer(IO.buffer(IO.write(Query.this.file
                                        .getAbsolutePath())));
                                writerToClose.set(this.writer);
                                this.variables = stream.getProperty("variables", List.class);
                                for (int i = 0; i < this.variables.size(); ++i) {
                                    this.writer.write(i > 0 ? "\t?" : "?");
                                    this.writer.write(this.variables.get(i));
                                }
                                this.writer.write("\n");
                            }
                            if (bindings != null) {
                                this.writer.write(TestUtil.encode(this.variables, bindings));
                                this.writer.write("\n");
                                tracker.increment();
                            }
                        }

                    });

                } finally {
                    final Writer writer = writerToClose.get();
                    if (writer != null) {
                        writer.flush();
                        Util.closeQuietly(writer);
                        try {
                            // TODO: remove this hack, necessary for giving gzip enough time to
                            // complete writing the file (the fix should be added to IO.write())
                            Thread.sleep(250);
                        } catch (InterruptedException ex) {
                            // ignore
                        }
                    }
                }

                tracker.end();
            }

            return this.file;
        }

        public static List<Query> create(final Properties properties, final File basePath) {
            final List<Query> queries = Lists.newArrayList();
            for (final Map.Entry<String, Properties> entry : TestUtil.split(properties).entrySet()) {
                final String name = entry.getKey();
                final Properties props = entry.getValue();
                final String filename = props.getProperty("file");
                final String query = props.getProperty("query");
                if (filename != null && query != null) {
                    final File file = basePath.toPath().resolve(Paths.get(filename)).toFile();
                    queries.add(new Query(name, file, query));
                }
            }
            return queries;
        }

        @Override
        public String toString() {
            return this.name;
        }

    }

    private static class Dictionary {

        private static final int TABLE_SIZE = 32 * 1024 * 1024 - 1;

        private static final int MAX_COLLISIONS = 1024;

        private static final int BUFFER_BITS = 12;

        private static final int BUFFER_SIZE = 1 << BUFFER_BITS;

        private final int[] table;

        private int[] list;

        private final List<byte[]> buffers;

        private int offset;

        private int lastCode;

        Dictionary() {
            this.table = new int[Dictionary.TABLE_SIZE];
            this.list = new int[1024];
            this.buffers = Lists.newArrayList();
            this.offset = BUFFER_SIZE;
            this.lastCode = 0;
        }

        public int codeFor(final String string) {
            final byte[] bytes = string.getBytes(Charsets.UTF_8);
            int bucket = Math.abs(string.hashCode()) % TABLE_SIZE;
            for (int i = 0; i < MAX_COLLISIONS; ++i) {
                final int code = this.table[bucket];
                if (code != 0) {
                    final int pointer = this.list[code - 1];
                    if (match(pointer, bytes)) {
                        return code;
                    }
                } else {
                    final int pointer = store(bytes);
                    if (this.lastCode >= this.list.length) {
                        final int[] oldList = this.list;
                        this.list = Arrays.copyOf(oldList, this.list.length * 2);
                    }
                    this.list[this.lastCode++] = pointer;
                    this.table[bucket] = this.lastCode;

                    // if (lastCode % 100000 == 0) {
                    // System.out.println(buffers.size() * BUFFER_SIZE);
                    // }

                    return this.lastCode;
                }
                bucket = (bucket + 1) % TABLE_SIZE;
            }
            throw new Error("Max number of collisions exceeded - RDF vocabulary too large");
        }

        public String stringFor(final int code) {
            final int pointer = this.list[code - 1];
            return new String(load(pointer), Charsets.UTF_8);
        }

        private byte[] load(final int pointer) {
            final int index = pointer >>> BUFFER_BITS - 2;
            final int offset = pointer << 2 & BUFFER_SIZE - 1;
            final byte[] buffer = this.buffers.get(index);
            int end = offset;
            while (buffer[end] != 0) {
                ++end;
            }
            return Arrays.copyOfRange(buffer, offset, end);
        }

        private int store(final byte[] bytes) {
            if (this.offset + bytes.length + 1 > BUFFER_SIZE) {
                this.buffers.add(new byte[BUFFER_SIZE]);
                this.offset = 0;
            }
            final int index = this.buffers.size() - 1;
            final int pointer = this.offset >> 2 | index << BUFFER_BITS - 2;
            final byte[] buffer = this.buffers.get(index);
            System.arraycopy(bytes, 0, buffer, this.offset, bytes.length);
            this.offset += bytes.length;
            buffer[this.offset++] = 0;
            this.offset = this.offset + 3 & 0xFFFFFFFC;
            return pointer;
        }

        private boolean match(final int pointer, final byte[] bytes) {
            final int index = pointer >>> BUFFER_BITS - 2;
            final int offset = pointer << 2 & BUFFER_SIZE - 1;
            final byte[] buffer = this.buffers.get(index);
            for (int i = 0; i < bytes.length; ++i) {
                if (buffer[offset + i] != bytes[i]) {
                    return false;
                }
            }
            return true;
        }

    }

    private static abstract class Tuple implements Comparable<Tuple> {

        public static Tuple create(final int... codes) {
            switch (codes.length) {
            case 0:
                return Tuple0.INSTANCE;
            case 1:
                return new Tuple1(codes[0]);
            case 2:
                return new Tuple2(codes[0], codes[1]);
            case 3:
                return new Tuple3(codes[0], codes[1], codes[2]);
            case 4:
                return new Tuple4(codes[0], codes[1], codes[2], codes[3]);
            default:
                return new TupleN(codes.clone());
            }
        }

        public abstract int size();

        public abstract int get(int index);

        public boolean matches(final Tuple tuple) {
            final int size = size();
            for (int i = 0; i < size; ++i) {
                final int expected = tuple.get(i);
                if (expected != 0 && get(i) != expected) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int compareTo(final Tuple other) {
            final int thisSize = size();
            final int otherSize = other.size();
            final int minSize = Math.min(thisSize, otherSize);
            for (int i = 0; i < minSize; ++i) {
                final int result = get(i) - other.get(i);
                if (result != 0) {
                    return result;
                }
            }
            return thisSize - otherSize;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Tuple)) {
                return false;
            }
            final Tuple other = (Tuple) object;
            final int size = size();
            if (other.size() != size) {
                return false;
            }
            for (int i = 0; i < size; ++i) {
                if (get(i) != other.get(i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            final int size = size();
            int hash = size;
            for (int i = 0; i < size; ++i) {
                hash = 37 * hash + get(i);
            }
            return hash;
        }

        @Override
        public String toString() {
            final int size = size();
            final StringBuilder builder = new StringBuilder();
            builder.append('(');
            for (int i = 0; i < size; ++i) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(get(i));
            }
            builder.append(')');
            return builder.toString();
        }

        private static final class Tuple0 extends Tuple {

            static final Tuple0 INSTANCE = new Tuple0();

            @Override
            public int size() {
                return 0;
            }

            @Override
            public int get(final int index) {
                throw new IndexOutOfBoundsException("Invalid index " + index);
            }

        }

        private static final class Tuple1 extends Tuple {

            private final int code;

            Tuple1(final int code) {
                this.code = code;
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public int get(final int index) {
                Preconditions.checkElementIndex(index, 1);
                return this.code;
            }

        }

        private static final class Tuple2 extends Tuple {

            private final int code0;

            private final int code1;

            Tuple2(final int code0, final int code1) {
                this.code0 = code0;
                this.code1 = code1;
            }

            @Override
            public int size() {
                return 2;
            }

            @Override
            public int get(final int index) {
                Preconditions.checkElementIndex(index, 2);
                return index == 0 ? this.code0 : this.code1;
            }

        }

        private static final class Tuple3 extends Tuple {

            private final int code0;

            private final int code1;

            private final int code2;

            Tuple3(final int code0, final int code1, final int code2) {
                this.code0 = code0;
                this.code1 = code1;
                this.code2 = code2;
            }

            @Override
            public int size() {
                return 3;
            }

            @Override
            public int get(final int index) {
                switch (index) {
                case 0:
                    return this.code0;
                case 1:
                    return this.code1;
                case 2:
                    return this.code2;
                default:
                    throw new IndexOutOfBoundsException("Index " + index + ", size 3");
                }
            }

        }

        private static final class Tuple4 extends Tuple {

            private final int code0;

            private final int code1;

            private final int code2;

            private final int code3;

            Tuple4(final int code0, final int code1, final int code2, final int code3) {
                this.code0 = code0;
                this.code1 = code1;
                this.code2 = code2;
                this.code3 = code3;
            }

            @Override
            public int size() {
                return 4;
            }

            @Override
            public int get(final int index) {
                switch (index) {
                case 0:
                    return this.code0;
                case 1:
                    return this.code1;
                case 2:
                    return this.code2;
                case 3:
                    return this.code3;
                default:
                    throw new IndexOutOfBoundsException("Index " + index + ", size 4");
                }
            }

        }

        private static final class TupleN extends Tuple {

            private final int[] codes;

            TupleN(final int[] codes) {
                this.codes = codes;
            }

            @Override
            public int size() {
                return this.codes.length;
            }

            @Override
            public int get(final int index) {
                return this.codes[index];
            }

        }

    }

}
