package eu.fbk.knowledgestore.internal.rdf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.BasicQueryWriterSettings;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParser;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.openrdf.rio.helpers.JSONLDMode;
import org.openrdf.rio.helpers.JSONLDSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.RDFJSONParserSettings;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.helpers.TriXParserSettings;
import org.openrdf.rio.helpers.XMLParserSettings;
import org.openrdf.rio.helpers.XMLWriterSettings;
import org.slf4j.Logger;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.ParseException;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Compression;
import eu.fbk.knowledgestore.internal.Logging;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.rdfpro.jsonld.JSONLD;
import eu.fbk.rdfpro.tql.TQL;

// TODO: reorganize code in this class

public final class RDFUtil {

    public static final String PROPERTY_VARIABLES = "variables";

    private static boolean jsonldDisabled = false;

    public static void toHtml(final Value value, @Nullable final Map<String, String> prefixes,
            final Appendable sink) throws IOException {
        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            sink.append("<span");
            if (literal.getLanguage() != null) {
                sink.append(" title=\"@").append(literal.getLanguage()).append("\"");
            } else if (literal.getDatatype() != null) {
                sink.append(" title=\"&lt;").append(literal.getDatatype().stringValue())
                        .append("&gt;\"");
            }
            sink.append(">").append(value.stringValue()).append("</span>");
        } else if (value instanceof BNode) {
            sink.append("_:").append(((BNode) value).getID());
        } else if (value instanceof URI) {
            final URI uri = (URI) value;
            sink.append("<a href=\"").append(uri.stringValue()).append("\">");
            String prefix = null;
            if (prefixes != null) {
                prefix = prefixes.get(uri.getNamespace());
            }
            if (prefix == null) {
                prefix = Data.namespaceToPrefix(uri.getNamespace(), Data.getNamespaceMap());
            }
            if (prefix != null) {
                sink.append(prefix).append(':').append(uri.getLocalName());
            } else {
                final int index = uri.stringValue().lastIndexOf('/');
                if (index >= 0) {
                    sink.append("&lt;..").append(uri.stringValue().substring(index))
                            .append("&gt;");
                } else {
                    sink.append("&lt;").append(uri.stringValue()).append("&gt;");
                }
            }
            sink.append("</a>");
        }
    }

    public static Stream<Statement> toStatementStream(
            final Iteration<? extends BindingSet, ?> iteration) {

        Preconditions.checkNotNull(iteration);

        return Stream.create(iteration).transform(new Function<BindingSet, Statement>() {

            @Override
            @Nullable
            public Statement apply(final BindingSet bindings) {
                final Value subject = bindings.getValue("subject");
                final Value predicate = bindings.getValue("predicate");
                final Value object = bindings.getValue("object");
                final Value context = bindings.getValue("context");
                if (subject instanceof Resource && predicate instanceof URI && object != null) {
                    final Resource subj = (Resource) subject;
                    final URI pred = (URI) predicate;
                    if (context == null) {
                        return Data.getValueFactory().createStatement(subj, pred, object);
                    } else if (context instanceof Resource) {
                        final Resource ctx = (Resource) context;
                        return Data.getValueFactory().createStatement(subj, pred, object, ctx);
                    }
                }
                return null;
            }

        }, 0);
    }

    public static Stream<BindingSet> toBindingsStream(
            final CloseableIteration<BindingSet, QueryEvaluationException> iteration,
            final Iterable<? extends String> variables) {

        Preconditions.checkNotNull(iteration);

        final List<String> variableList = ImmutableList.copyOf(variables);
        final CompactBindingSet.Builder builder = CompactBindingSet.builder(variableList);

        return Stream.create(iteration).transform(new Function<BindingSet, BindingSet>() {

            @Override
            @Nullable
            public BindingSet apply(final BindingSet bindings) {
                final int variableCount = variableList.size();
                for (int i = 0; i < variableCount; ++i) {
                    final String variable = variableList.get(i);
                    builder.set(variable, bindings.getValue(variable));
                }
                return builder.build();
            }

        }, 0).setProperty(PROPERTY_VARIABLES, variableList);
    }

    public static int detectSparqlProlog(final String string) {
        final int length = string.length();
        int index = 0;
        while (index < length) {
            final char ch = string.charAt(index);
            if (ch == '#') { // comment
                while (index < length && string.charAt(index) != '\n') {
                    ++index;
                }
            } else if (ch == 'p' || ch == 'b' || ch == 'P' || ch == 'B') { // prefix or base
                while (index < length && string.charAt(index) != '>') {
                    ++index;
                }
            } else if (!Character.isWhitespace(ch)) { // found
                return index;
            }
            ++index;
        }
        throw new ParseException(string, "Cannot detect SPARQL prolog");
    }

    public static String detectSparqlForm(final String string) {
        final int start = detectSparqlProlog(string);
        for (int i = start; i < string.length(); ++i) {
            final char ch = string.charAt(i);
            if (Character.isWhitespace(ch)) {
                final String form = string.substring(start, i).toLowerCase();
                if (!form.equals("select") && !form.equals("construct")
                        && !form.equals("describe") && !form.equals("ask")) {
                    throw new ParseException(string, "Invalid query form: " + form);
                }
                return form;
            }
        }
        throw new ParseException(string, "Cannot detect query form");
    }

    public static long writeSparqlTuples(final TupleQueryResultFormat format,
            final OutputStream out, final Stream<? extends BindingSet> stream) {

        final TupleQueryResultWriter writer = RDFUtil.newSparqlTupleWriter(format, out);

        try {
            final AtomicLong result = new AtomicLong();
            stream.toHandler(new Handler<BindingSet>() {

                private boolean started = false;

                private long count = 0L;

                @Override
                public void handle(final BindingSet bindings) throws QueryResultHandlerException {
                    if (!this.started) {
                        @SuppressWarnings("unchecked")
                        final List<String> variables = (List<String>) stream.getProperty(
                                PROPERTY_VARIABLES, Object.class);
                        writer.startDocument();
                        writer.startHeader();
                        writer.startQueryResult(variables);
                        this.started = true;
                    }
                    if (bindings != null) {
                        writer.handleSolution(bindings);
                        ++this.count;
                    } else if (this.started) {
                        writer.endQueryResult();
                        result.set(this.count);
                    }
                }

            });
            return result.get();

        } catch (final Exception ex) {
            throw Throwables.propagate(ex);

        } finally {
            Util.closeQuietly(stream);
        }
    }

    public static Stream<BindingSet> readSparqlTuples(final TupleQueryResultFormat format,
            final InputStream in) {

        // Create a parser for the specified format
        final TupleQueryResultParser parser = newSparqlTupleParser(format);

        // Return a source over parsed bindings
        final Map<String, String> mdc = Logging.getMDC();
        return new Stream<BindingSet>() {

            @Override
            protected void doToHandler(final Handler<? super BindingSet> handler) throws Throwable {
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    parser.setQueryResultHandler(new TupleQueryResultHandlerBase() {

                        private CompactBindingSet.Builder builder;

                        @Override
                        public void startQueryResult(final List<String> vars)
                                throws TupleQueryResultHandlerException {
                            final List<String> variables = ImmutableList.copyOf(vars);
                            setProperty(PROPERTY_VARIABLES, variables);
                            this.builder = CompactBindingSet.builder(variables);
                        }

                        @Override
                        public void handleSolution(final BindingSet bindings)
                                throws TupleQueryResultHandlerException {
                            if (bindings != null) {
                                emit(bindings);
                            }
                        }

                        @Override
                        public void endQueryResult() throws TupleQueryResultHandlerException {
                            emit(null);
                        }

                        private void emit(final BindingSet bindings)
                                throws TupleQueryResultHandlerException {
                            try {
                                BindingSet compactBindings = bindings;
                                if (bindings != null) {
                                    this.builder.setAll(bindings);
                                    compactBindings = this.builder.build();
                                }
                                handler.handle(compactBindings);
                            } catch (final Throwable ex) {
                                Throwables.propagateIfPossible(ex,
                                        TupleQueryResultHandlerException.class);
                                throw new TupleQueryResultHandlerException(ex);
                            }
                        }

                    });
                    parser.parseQueryResult(in);
                } finally {
                    Logging.setMDC(oldMdc);
                }
            }

        };
    }

    public static void writeSparqlBoolean(final BooleanQueryResultFormat format,
            final OutputStream out, final boolean value) {

        final BooleanQueryResultWriter writer = newSparqlBooleanWriter(format, out);

        try {
            writer.startDocument();
            writer.startHeader();
            writer.handleBoolean(value);

        } catch (final Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static boolean readSparqlBoolean(final BooleanQueryResultFormat format,
            final InputStream in) {

        final BooleanQueryResultParser parser = newSparqlBooleanReader(format);

        try {
            final AtomicBoolean resultHolder = new AtomicBoolean();
            parser.setQueryResultHandler(new TupleQueryResultHandlerBase() {

                @Override
                public void handleBoolean(final boolean result) throws QueryResultHandlerException {
                    resultHolder.set(result);
                }

            });
            parser.parseQueryResult(in);
            return resultHolder.get();

        } catch (final Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    public static long writeRDF(final OutputStream out, final RDFFormat format,
            @Nullable final Map<String, String> namespaces,
            @Nullable final Map<? extends RioSetting<?>, ? extends Object> settings,
            final Stream<? extends Statement> stream) {

        final Map<RioSetting<?>, Object> actualSettings = Maps.newHashMap();
        if (settings != null) {
            actualSettings.putAll(settings);
        }
        final Object types = stream.getProperty("types", Object.class);
        if (types instanceof Set && !jsonldDisabled) {
            try {
                actualSettings.put(JSONLD.ROOT_TYPES, types);
            } catch (final Throwable ex) {
                jsonldDisabled = true; // rdfpro-jsonld not available
            }
        }

        try {
            final RDFHandler handler = writeRDF(out, format, namespaces, actualSettings);
            final AtomicLong result = new AtomicLong();
            stream.toHandler(new Handler<Statement>() {

                private boolean started = false;

                private long count = 0L;

                @Override
                public void handle(final Statement statement) throws RDFHandlerException {
                    if (!this.started) {
                        handler.startRDF();
                        this.started = true;
                    }
                    if (statement != null) {
                        handler.handleStatement(statement);
                        ++this.count;
                    } else if (this.started) {
                        handler.endRDF();
                        result.set(this.count);
                    }
                }

            });
            return result.get();

        } catch (final Exception ex) {
            throw Throwables.propagate(ex);

        } finally {
            Util.closeQuietly(stream);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static RDFHandler writeRDF(final OutputStream out, final RDFFormat format,
            @Nullable final Map<String, String> namespaces,
            @Nullable final Map<? extends RioSetting<?>, ? extends Object> settings)
            throws IOException, RDFHandlerException {

        final RDFWriter writer = Rio.createWriter(format, out);

        final WriterConfig config = writer.getWriterConfig();
        config.set(BasicWriterSettings.PRETTY_PRINT, true);
        config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
        config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);

        if (format.equals(RDFFormat.RDFXML)) {
            config.set(XMLWriterSettings.INCLUDE_XML_PI, true);
            config.set(XMLWriterSettings.INCLUDE_ROOT_RDF_TAG, true);
        }

        if (settings != null) {
            for (final Map.Entry entry : settings.entrySet()) {
                config.set((RioSetting) entry.getKey(), entry.getValue());
            }
        }

        return namespaces == null ? writer : newNamespaceHandler(writer, namespaces, null);
    }

    public static Stream<Statement> readRDF(final InputStream in, final RDFFormat format,
            @Nullable final Map<String, String> namespaces, @Nullable final String base,
            final boolean preserveBNodes) {

        final Map<String, String> mdc = Logging.getMDC();
        return new Stream<Statement>() {

            @Override
            protected void doToHandler(final Handler<? super Statement> handler) throws Throwable {
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    final RDFHandler rdfHandler = new RDFHandlerBase() {

                        @Override
                        public void handleStatement(final Statement statement)
                                throws RDFHandlerException {
                            emit(statement);
                        }

                        @Override
                        public void endRDF() throws RDFHandlerException {
                            emit(null);
                        }

                        private void emit(final Statement statement) throws RDFHandlerException {
                            try {
                                handler.handle(statement);
                            } catch (final Throwable ex) {
                                Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                                throw new RuntimeException(ex);
                            }
                        }

                    };
                    readRDF(in, format, namespaces, base, preserveBNodes, rdfHandler);
                } finally {
                    Logging.setMDC(oldMdc);
                }
            }

        };
    }

    public static void readRDF(final InputStream in, @Nullable final RDFFormat format,
            @Nullable final Map<String, String> namespaces, @Nullable final String base,
            final boolean preserveBNodes, final RDFHandler handler) throws IOException,
            RDFParseException, RDFHandlerException {

        final RDFParser parser = Rio.createParser(format);
        parser.setValueFactory(Data.getValueFactory());

        final ParserConfig config = parser.getParserConfig();
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, true);
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, true);
        config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, true);
        config.set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, true);
        config.set(BasicParserSettings.NORMALIZE_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.PRESERVE_BNODE_IDS, preserveBNodes);

        if (format.equals(RDFFormat.NTRIPLES)) {
            config.set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES, true);

        } else if (format.equals(RDFFormat.JSONLD)) {
            // following parameters are currently ignored by used library
            config.set(JSONLDSettings.COMPACT_ARRAYS, true);
            config.set(JSONLDSettings.OPTIMIZE, true);
            config.set(JSONLDSettings.USE_NATIVE_TYPES, false);
            config.set(JSONLDSettings.USE_RDF_TYPE, false);
            config.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);

        } else if (format.equals(RDFFormat.RDFJSON)) {
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_DATATYPES, true);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_LANGUAGES, true);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_TYPES, true);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_VALUES, true);
            config.set(RDFJSONParserSettings.FAIL_ON_UNKNOWN_PROPERTY, true);
            config.set(RDFJSONParserSettings.SUPPORT_GRAPHS_EXTENSION, true);

        } else if (format.equals(RDFFormat.TRIX)) {
            config.set(TriXParserSettings.FAIL_ON_TRIX_INVALID_STATEMENT, true);
            config.set(TriXParserSettings.FAIL_ON_TRIX_MISSING_DATATYPE, false);

        } else if (format.equals(RDFFormat.RDFXML)) {
            config.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, true);
            config.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, true);
            config.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, true);
            config.set(XMLParserSettings.FAIL_ON_MISMATCHED_TAGS, true);
            config.set(XMLParserSettings.FAIL_ON_NON_STANDARD_ATTRIBUTES, false);
            config.set(XMLParserSettings.FAIL_ON_SAX_NON_FATAL_ERRORS, false);
        }

        if (namespaces != null && parser instanceof RDFParserBase) {
            try {
                final Field field = RDFParserBase.class.getDeclaredField("namespaceTable");
                field.setAccessible(true);
                field.set(parser, Data.newNamespaceMap(Data.newNamespaceMap(), namespaces));
            } catch (final Throwable ex) {
                // ignore
                ex.printStackTrace();
            }
        }

        parser.setRDFHandler(handler);
        parser.parse(in, Strings.nullToEmpty(base));
    }

    public static void readRDF(final Map<File, ? extends RDFHandler> sources,
            @Nullable final RDFFormat format, @Nullable final Map<String, String> namespaces,
            @Nullable final String base, final boolean preserveBNodes,
            @Nullable final Compression compression, final int parallelism) throws IOException,
            RDFParseException, RDFHandlerException {

        // Sort files based on size, placing larger files first to better parallelism
        final Map<File, RDFHandler> actualSources = Maps.newHashMap(sources);
        final List<File> sortedFiles = Lists.newArrayList(sources.keySet());
        Collections.sort(sortedFiles, new Comparator<File>() {

            @Override
            public int compare(final File first, final File second) {
                if (first == null) {
                    return second == null ? 0 : -1;
                } else {
                    return second == null ? 1 : (int) (second.length() - first.length());
                }
            }

        });

        // Compute parallelism degree as minimum of supplied value and available files
        final int actualParallelism = Math.max(1, Math.min(parallelism, sortedFiles.size()));

        // If parallelism is not needed, just loop through the files using this thread
        if (actualParallelism == 1) {
            for (final File file : sortedFiles) {
                final RDFHandler handler = actualSources.get(file);
                readRDFHelper(file, format, namespaces, base, preserveBNodes, compression, handler);
            }
            return;
        }

        // Allocate a latch to wait for threads to finish, and a variable to store exceptions
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<Throwable>(null);
        final CountDownLatch latch = new CountDownLatch(actualParallelism);

        // Parse the files using multiple threads until the list is empty or an error occurs
        for (int i = 0; i < actualParallelism; ++i) {
            Data.getExecutor().execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        while (exceptionHolder.get() == null) {
                            final File file;
                            final RDFHandler handler;
                            synchronized (sortedFiles) {
                                if (sortedFiles.isEmpty() || exceptionHolder.get() != null) {
                                    break;
                                }
                                file = sortedFiles.remove(0);
                                handler = actualSources.get(file);
                            }
                            readRDFHelper(file, format, namespaces, base, preserveBNodes,
                                    compression, handler);
                        }
                    } catch (final Throwable ex) {
                        exceptionHolder.set(ex);
                    } finally {
                        latch.countDown();
                    }
                }

            });
        }

        try {
            latch.await();
        } catch (final InterruptedException ex) {
            // restore interrupted status
            Thread.currentThread().interrupt();
        }

        // Propagate an exception occurred during parsing
        final Throwable ex = exceptionHolder.get();
        if (ex != null) {
            Throwables.propagateIfPossible(ex, IOException.class);
            Throwables.propagateIfPossible(ex, RDFHandlerException.class);
            Throwables.propagateIfPossible(ex, RDFParseException.class);
            throw new RuntimeException(ex);
        }
    }

    private static void readRDFHelper(@Nullable final File file, @Nullable final RDFFormat format,
            @Nullable final Map<String, String> namespaces, @Nullable final String base,
            final boolean preserveBNodes, @Nullable final Compression compression,
            final RDFHandler handler) throws IOException, RDFParseException, RDFHandlerException {

        // Detect file format
        RDFFormat actualFormat = format;
        if (actualFormat == null) {
            if (file == null) {
                throw new IllegalArgumentException("Cannot detect RDF format of STDIN");
            }
            actualFormat = RDFFormat.forFileName(file.getName());
        }

        // Detect file compression
        Compression actualCompression = compression;
        if (actualCompression == null) {
            actualCompression = file == null ? Compression.NONE : Compression.forFileName(
                    file.getName(), Compression.NONE);
        }

        // Perform parsing, wrapping possible exceptions so to report the file name
        InputStream stream = null;
        try {
            stream = file == null ? System.in : actualCompression.read(Data.getExecutor(), file);
            readRDF(stream, actualFormat, namespaces, base, preserveBNodes, handler);

        } catch (final Throwable ex) {
            final String message = "Parsing of " + (file == null ? "STDIN" : file)
                    + " using format " + actualFormat + " and compression " + actualCompression
                    + " failed: " + ex.getMessage();
            if (ex instanceof IOException) {
                throw new IOException(message, ex);
            } else if (ex instanceof RDFParseException) {
                throw new RDFParseException(message, ex);
            } else if (ex instanceof RDFHandlerException) {
                throw new RDFHandlerException(message, ex);
            }
            throw new RuntimeException(message, ex);
        } finally {
            if (stream != System.in) {
                Util.closeQuietly(stream);
            }
        }
    }

    public static TupleQueryResultWriter newSparqlTupleWriter(final TupleQueryResultFormat format,
            final OutputStream stream) {

        final TupleQueryResultWriter writer = QueryResultIO.createWriter(format, stream);

        final WriterConfig config = writer.getWriterConfig();
        if (format.equals(TupleQueryResultFormat.JSON)) {
            config.set(BasicWriterSettings.PRETTY_PRINT, true);
            config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
            config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);

        } else if (format.equals(TupleQueryResultFormat.SPARQL)) {
            config.set(BasicWriterSettings.PRETTY_PRINT, true);
            config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
            config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
            config.set(BasicQueryWriterSettings.ADD_SESAME_QNAME, false);
        }

        return writer;
    }

    public static TupleQueryResultParser newSparqlTupleParser(final TupleQueryResultFormat format) {

        final TupleQueryResultParser parser = QueryResultIO.createParser(format);
        parser.setValueFactory(CompactValueFactory.getInstance());

        return parser;
    }

    public static BooleanQueryResultWriter newSparqlBooleanWriter(
            final BooleanQueryResultFormat format, final OutputStream stream) {

        final BooleanQueryResultWriter writer = QueryResultIO.createWriter(format, stream);

        final WriterConfig config = writer.getWriterConfig();
        if (format.equals(BooleanQueryResultFormat.JSON)) {
            config.set(BasicWriterSettings.PRETTY_PRINT, true);
            config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
            config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);

        } else if (format.equals(TupleQueryResultFormat.SPARQL)) {
            config.set(BasicWriterSettings.PRETTY_PRINT, true);
            config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
            config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
            config.set(BasicQueryWriterSettings.ADD_SESAME_QNAME, false);
        }

        return writer;
    }

    public static BooleanQueryResultParser newSparqlBooleanReader(
            final BooleanQueryResultFormat format) {

        final BooleanQueryResultParser parser = QueryResultIO.createParser(format);
        parser.setValueFactory(Data.getValueFactory());

        return parser;
    }

    public static RDFHandler newMergingHandler(final RDFHandler handler) {
        return new MergingHandler(handler);
    }

    public static RDFHandler newDecouplingHandler(final RDFHandler handler,
            @Nullable final Integer queueSize) {
        return new DecouplingHandler(handler, queueSize);
    }

    public static RDFHandler newNamespaceHandler(final RDFHandler handler,
            final Map<String, String> namespaces, @Nullable final Integer bufferSize) {
        return new NamespaceHandler(handler, namespaces, bufferSize);
    }

    public static RDFHandler newLoggingHandler(final RDFHandler handler, final Logger logger,
            @Nullable final String startMessage, @Nullable final String progressMessage,
            @Nullable final String endMessage) {

        Preconditions.checkNotNull(handler);
        Preconditions.checkNotNull(logger);
        if (startMessage == null && progressMessage == null && endMessage == null) {
            return handler;
        } else {
            return new LoggingHandler(handler, logger, startMessage, progressMessage, endMessage);
        }
    }

    private static final class MergingHandler implements RDFHandler {

        private final RDFHandler handler;

        private int depth;

        MergingHandler(final RDFHandler handler) {
            this.handler = Preconditions.checkNotNull(handler);
            this.depth = 0;
        }

        @Override
        public synchronized void startRDF() throws RDFHandlerException {
            if (this.depth == 0) {
                this.handler.startRDF();
            }
            ++this.depth;
        }

        @Override
        public synchronized void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public synchronized void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

        @Override
        public synchronized void endRDF() throws RDFHandlerException {
            --this.depth;
            if (this.depth == 0) {
                this.handler.endRDF();
            }
        }

    }

    private static final class DecouplingHandler implements RDFHandler {

        private static final int DEFAULT_QUEUE_SIZE = 1024;

        private static final Object EOF = new Object();

        private final RDFHandler handler;

        private final int queueSize;

        private BlockingQueue<Object> queue;

        private AtomicReference<Throwable> exception;

        private Future<?> future;

        private int depth;

        DecouplingHandler(final RDFHandler handler, @Nullable final Integer queueSize) {
            this.handler = Preconditions.checkNotNull(handler);
            this.queueSize = MoreObjects.firstNonNull(queueSize, DEFAULT_QUEUE_SIZE);
            this.queue = null;
            this.exception = null;
            this.future = null;
            this.depth = 0;
        }

        @Override
        public synchronized void startRDF() throws RDFHandlerException {

            // Accept nested startRDF/endRDF calls
            if (this.depth++ > 0) {
                return;
            }

            // Initialize queue and exception holder
            this.queue = new ArrayBlockingQueue<Object>(this.queueSize);
            this.exception = new AtomicReference<Throwable>(null);

            // Run a background task to move comments, namespaces and statements off the queue and
            // forward it to the wrapped handler
            this.future = Data.getExecutor().submit(new Runnable() {

                @Override
                public void run() {
                    Object object;
                    try {
                        DecouplingHandler.this.handler.startRDF();
                        while ((object = DecouplingHandler.this.queue.take()) != EOF) {
                            if (object instanceof Statement) {
                                DecouplingHandler.this.handler.handleStatement((Statement) object);
                            } else if (object instanceof Namespace) {
                                final Namespace ns = (Namespace) object;
                                DecouplingHandler.this.handler.handleNamespace(ns.getPrefix(),
                                        ns.getName());
                            } else if (object instanceof String) {
                                DecouplingHandler.this.handler.handleComment((String) object);
                            }
                        }
                        DecouplingHandler.this.handler.endRDF();
                    } catch (final Throwable ex) {
                        DecouplingHandler.this.exception.set(ex);
                    }
                }

            });
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {

            // Enqueue comment and propagate exceptions from background task, if any
            put(comment);
            propagateOnFailure();
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {

            // Enqueue namespace and propagate exceptions from background task, if any
            put(new NamespaceImpl(prefix, uri));
            propagateOnFailure();
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            // Enqueue statement and propagate exceptions from background task, if any
            put(statement);
            propagateOnFailure();
        }

        @Override
        public synchronized void endRDF() throws RDFHandlerException {

            // Accept nested startRDF/endRDF calls
            if (--this.depth > 0) {
                return;
            }

            // Signal end of RDF
            put(EOF);

            // Wait for the background task to complete
            try {
                this.future.get();
            } catch (final Throwable ex) {
                Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                Throwables.propagate(ex);
            }

            // Propagate exception from background task, if any
            propagateOnFailure();
        }

        private void put(final Object object) throws RDFHandlerException {
            try {
                this.queue.put(object);
            } catch (final InterruptedException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        private void propagateOnFailure() throws RDFHandlerException {
            final Throwable ex = this.exception.get();
            if (ex != null) {
                Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                Throwables.propagate(ex);
            }
        }

    }

    private static final class NamespaceHandler implements RDFHandler {

        private static final int DEFAULT_BUFFER_SIZE = 1024;

        private final RDFHandler handler;

        private final Map<String, String> namespaces;

        private final int bufferSize;

        private List<Statement> buffer;

        private boolean buffering;

        private Map<String, String> bindings;

        NamespaceHandler(final RDFHandler handler, final Map<String, String> namespaces,
                @Nullable final Integer bufferSize) {
            this.handler = Preconditions.checkNotNull(handler);
            this.namespaces = Preconditions.checkNotNull(namespaces);
            this.bufferSize = MoreObjects.firstNonNull(bufferSize, DEFAULT_BUFFER_SIZE);
            this.buffer = null;
            this.buffering = false;
            this.bindings = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.bindings = Maps.newHashMap();
            this.buffer = Lists.newArrayListWithCapacity(this.bufferSize);
            this.buffering = true;
            this.handler.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            flush();
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            if (this.buffering) {
                this.bindings.put(uri, prefix);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            if (this.buffering) {
                extractNamespace(statement.getSubject());
                extractNamespace(statement.getPredicate());
                extractNamespace(statement.getObject());
                extractNamespace(statement.getContext());
                this.buffer.add(statement);
                if (this.buffer.size() == this.bufferSize) {
                    flush();
                }
            } else {
                this.handler.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            flush();
            this.handler.endRDF();
        }

        private void extractNamespace(final Value value) {
            if (value instanceof URI) {
                final String ns = ((URI) value).getNamespace();
                this.bindings.put(ns, this.bindings.get(ns));
            } else if (value instanceof Literal) {
                extractNamespace(((Literal) value).getDatatype());
            }
        }

        private void flush() throws RDFHandlerException {
            if (!this.buffering) {
                return;
            }
            for (final String namespace : Ordering.natural().sortedCopy(this.bindings.keySet())) {
                String prefix = this.bindings.get(namespace);
                if (prefix == null) {
                    prefix = Data.namespaceToPrefix(namespace, this.namespaces);
                }
                if (prefix != null) {
                    this.handler.handleNamespace(prefix, namespace);
                }
            }
            for (final Statement statement : this.buffer) {
                this.handler.handleStatement(statement);
            }
            this.bindings = null;
            this.buffer = null;
            this.buffering = false;
        }

    }

    private static final class LoggingHandler implements RDFHandler {

        private final RDFHandler handler;

        @Nullable
        private final Logger logger;

        @Nullable
        private final String startMessage;

        @Nullable
        private final String progressMessage;

        @Nullable
        private final String endMessage;

        private long totalTs;

        private long totalCounter = 0;

        private long lastTs;

        private long lastCounter = 0;

        LoggingHandler(final RDFHandler handler, final Logger logger,
                @Nullable final String startMessage, @Nullable final String progressMessage,
                @Nullable final String endMessage) {
            this.handler = Preconditions.checkNotNull(handler);
            this.logger = logger;
            this.startMessage = startMessage;
            this.progressMessage = progressMessage;
            this.endMessage = endMessage;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
            this.totalTs = System.currentTimeMillis();
            this.lastTs = this.totalTs;
            if (this.startMessage != null) {
                this.logger.info(this.startMessage);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.handler.handleStatement(statement);
            ++this.totalCounter;
            if (this.progressMessage != null && this.totalCounter % 1000 == 0) {
                final long ts = System.currentTimeMillis();
                if (ts - this.lastTs >= 1000) {
                    final long throughput = (this.totalCounter - this.lastCounter) * 1000
                            / (ts - this.lastTs);
                    final long avgThroughput = this.totalCounter * 1000 / (ts - this.totalTs);
                    this.lastTs = ts;
                    this.lastCounter = this.totalCounter;
                    this.logger.info(String.format(this.progressMessage, this.totalCounter,
                            throughput, avgThroughput));
                }
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (this.endMessage != null) {
                final long ts = System.currentTimeMillis();
                final long avgThroughput = this.totalCounter * 1000 / (ts - this.totalTs + 1);
                this.logger.info(String.format(this.endMessage, this.totalCounter, avgThroughput));
            }
            this.handler.endRDF();
        }

    }

    private RDFUtil() {
    }

    {
        TQL.register();
        System.setProperty("entityExpansionLimit", "" + Integer.MAX_VALUE);
    }

}
