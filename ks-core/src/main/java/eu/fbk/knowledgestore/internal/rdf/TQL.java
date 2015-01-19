package eu.fbk.knowledgestore.internal.rdf;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.helpers.RDFWriterBase;

/**
 * Parser and writer factory for the Turtle Quads (TQL) format.
 * <p>
 * This factory provides support to the parsing and generation of TQL RDF data via the Sesame RIO
 * API. The factory registers itself in the Sesame RIO API via files
 * {@code META-INF/services/org.openrdf.rio.RDFParserFactory} and
 * {@code META-INF/services/org.openrdf.rio.RDFWriterFactory}.
 * </p>
 * <p>
 * The Turtle Quads format is defined by constant {@link #FORMAT}. As this constant is not part of
 * the predefined set of formats in {@link RDFFormat}, it is necessary to register it. This can be
 * done either via {@link RDFFormat#register(RDFFormat)}, or by simply calling method
 * {@link #register()} on this class, which is implemented in a way that multiple calls will
 * result in only a single registration.
 * </p>
 */
public final class TQL implements RDFParserFactory, RDFWriterFactory {

    /** RDFFormat constant for the Turtle Quads (TQL) format). */
    public static final RDFFormat FORMAT = new RDFFormat("Turtle Quads", "application/x-tql",
            Charsets.UTF_8, "tql", false, true);

    private static final int BUFFER_SIZE = 64 * 1024;

    static {
        RDFFormat.register(FORMAT);
    }

    /**
     * Registers the Turtle Quads format in the RIO registry. Calling this method multiple times
     * results in a single registration. Note that registration is also done transparently the
     * first time this class is accessed.
     */
    public static void register() {
        // calling this method will cause the static initializer to run once
    }

    @Override
    public RDFFormat getRDFFormat() {
        return FORMAT;
    }

    @Override
    public RDFParser getParser() {
        return new TQLParser();
    }

    @Override
    public RDFWriter getWriter(final OutputStream out) {
        return getWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    }

    @Override
    public RDFWriter getWriter(final Writer writer) {
        return new TQLWriter(writer);
    }

    private static boolean isLetterOrNumber(final int c) {
        return isLetter(c) || isNumber(c);
    }

    private static boolean isLetter(final int c) {
        return c >= 65 && c <= 90 || c >= 97 && c <= 122;
    }

    private static boolean isNumber(final int c) {
        return c >= 48 && c <= 57;
    }

    private static final class TQLParser extends RDFParserBase {

        private final char[] buffer = new char[BUFFER_SIZE];

        private int bufferSize = 0;

        private int bufferIndex = 0;

        private Reader reader;

        private int lineNo;

        private StringBuilder builder;

        private Value value;

        @Override
        public RDFFormat getRDFFormat() {
            return FORMAT;
        }

        @Override
        public void parse(final InputStream stream, final String baseURI) throws IOException,
                RDFParseException, RDFHandlerException {
            parse(new InputStreamReader(stream, Charsets.UTF_8), baseURI);
        }

        @Override
        public void parse(final Reader reader, final String baseURI) throws IOException,
                RDFParseException, RDFHandlerException {

            Preconditions.checkNotNull(reader);
            Preconditions.checkNotNull(baseURI);

            if (this.rdfHandler != null) {
                this.rdfHandler.startRDF();
            }

            this.reader = reader;
            this.lineNo = 1;
            this.builder = new StringBuilder(1024);
            this.value = null;

            reportLocation(this.lineNo, 1);

            try {
                char c = read();
                c = skipWhitespace(c);
                while (c != 0) {
                    if (c == '#') {
                        c = skipLine(c);
                    } else if (c == '\r' || c == '\n') {
                        c = skipLine(c);
                    } else {
                        c = parseQuad(c);
                    }
                    c = skipWhitespace(c);
                }
            } finally {
                clear();
                this.reader = null;
                this.builder = null;
                this.value = null;
            }

            if (this.rdfHandler != null) {
                this.rdfHandler.endRDF();
            }
        }

        private char read() throws IOException {
            if (this.bufferIndex < this.bufferSize) {
                return this.buffer[this.bufferIndex++];
            }
            this.bufferSize = this.reader.read(this.buffer);
            if (this.bufferSize == -1) {
                return 0;
            }
            this.bufferIndex = 1;
            return this.buffer[0];
        }

        private char skipLine(final char ch) throws IOException {
            char c = ch;
            while (c != 0 && c != '\r' && c != '\n') {
                c = read();
            }
            if (c == '\n') {
                c = read();
                this.lineNo++;
                reportLocation(this.lineNo, 1);
            } else if (c == '\r') {
                c = read();
                if (c == '\n') {
                    c = read();
                }
                this.lineNo++;
                reportLocation(this.lineNo, 1);
            }
            return c;
        }

        private char skipWhitespace(final char ch) throws IOException {
            char c = ch;
            while (c == ' ' || c == '\t') {
                c = read();
            }
            return c;
        }

        private char parseQuad(final char ch) throws IOException, RDFParseException,
                RDFHandlerException {

            char c = ch;
            try {
                c = parseResource(c);
                c = skipWhitespace(c);
                final Resource subject = (Resource) this.value;

                c = parseURI(c);
                c = skipWhitespace(c);
                final URI predicate = (URI) this.value;

                c = parseValue(c);
                c = skipWhitespace(c);
                final Value object = this.value;

                Resource context = null;
                if (c != '.') {
                    c = parseResource(c);
                    c = skipWhitespace(c);
                    context = (Resource) this.value;
                }

                if (c == 0) {
                    throwEOFException();
                } else if (c != '.') {
                    reportError("Expected '.', found: " + c,
                            NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
                }
                c = read();
                c = skipWhitespace(c);
                if (c != 0 && c != '\r' && c != '\n') {
                    reportFatalError("Content after '.' is not allowed");
                }

                if (this.rdfHandler != null) {
                    final Statement statement;
                    if (context == null || context.equals(SESAME.NIL)) {
                        statement = createStatement(subject, predicate, object);
                    } else {
                        statement = createStatement(subject, predicate, object, context);
                    }
                    this.rdfHandler.handleStatement(statement);
                }

            } catch (final RDFParseException ex) {
                if (getParserConfig().isNonFatalError(
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)) {
                    reportError(ex, this.lineNo, -1,
                            NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
                } else {
                    throw ex;
                }
            }

            c = skipLine(c);
            return c;
        }

        private char parseValue(final char ch) throws IOException, RDFParseException {
            char c = ch;
            if (c == '<') {
                c = parseURI(c);
            } else if (c == '_') {
                c = parseBNode(c);
            } else if (c == '"') {
                c = parseLiteral(c);
            } else if (c == 0) {
                throwEOFException();
            } else {
                reportFatalError("Expected '<', '_' or '\"', found: " + c + "");
            }
            return c;
        }

        private char parseResource(final char ch) throws IOException, RDFParseException {
            char c = ch;
            if (c == '<') {
                c = parseURI(c);
            } else if (c == '_') {
                c = parseBNode(c);
            } else if (c == 0) {
                throwEOFException();
            } else {
                reportFatalError("Expected '<' or '_', found: " + c);
            }
            return c;
        }

        private char parseURI(final char ch) throws IOException, RDFParseException {
            char c = ch;
            if (c != '<') {
                reportError("Supplied char should be a '<', is: " + c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            this.builder.setLength(0);
            c = read();
            while (c != '>') {
                switch (c) {
                case 0:
                    throwEOFException();
                    break;
                case '\\':
                    c = read();
                    if (c == 0) {
                        throwEOFException();
                    }
                    this.builder.append(c);
                    break;
                default:
                    this.builder.append(c);
                    break;
                }
                c = read();
            }
            this.value = createURI(this.builder.toString());
            c = read();
            return c;
        }

        private char parseBNode(final char ch) throws IOException, RDFParseException {
            char c = ch;
            if (c != '_') {
                reportError("Supplied char should be a '_', is: " + c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            c = read();
            if (c == 0) {
                throwEOFException();
            } else if (c != ':') {
                reportError("Expected ':', found: " + c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            c = read();
            if (c == 0) {
                throwEOFException();
            } else if (!isLetter(c)) {
                reportError("Expected a letter, found: " + c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            this.builder.setLength(0);
            this.builder.append(c);
            c = read();
            while (c != 0 && isLetterOrNumber(c)) {
                this.builder.append(c);
                c = read();
            }
            this.value = createBNode(this.builder.toString());
            return c;
        }

        private char parseLiteral(final char ch) throws IOException, RDFParseException {
            char c = ch;
            if (c != '"') {
                reportError("Supplied char should be a '\"', is: " + c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            this.builder.setLength(0);
            c = read();
            while (c != '"') {
                if (c == 0) {
                    throwEOFException();
                } else if (c == '\\') {
                    c = read();
                    switch (c) {
                    case 0:
                        throwEOFException();
                        break;
                    case 't':
                        this.builder.append('\t');
                        break;
                    case 'n':
                        this.builder.append('\n');
                        break;
                    case 'r':
                        this.builder.append('\r');
                        break;
                    default:
                        this.builder.append(c);
                        break;
                    }
                } else {
                    this.builder.append(c);
                }
                c = read();
            }
            c = read();
            final String label = this.builder.toString();
            if (c == '@') {
                this.builder.setLength(0);
                c = read();
                while (isLetter(c)) {
                    this.builder.append(c);
                    c = read();
                }
                final String language = this.builder.toString();
                this.value = createLiteral(label, language, null, this.lineNo, -1);
            } else if (c == '^') {
                c = read();
                if (c == 0) {
                    throwEOFException();
                } else if (c != '^') {
                    reportError("Expected '^', found: " + c,
                            NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
                }
                c = read();
                if (c == 0) {
                    throwEOFException();
                } else if (c != '<') {
                    reportError("Expected '<', found: " + c,
                            NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
                }
                c = parseURI(c);
                final URI datatype = (URI) this.value;
                this.value = createLiteral(label, null, datatype, this.lineNo, -1);
            } else {
                this.value = createLiteral(label, null, null, this.lineNo, -1);
            }
            return c;
        }

        private static void throwEOFException() throws RDFParseException {
            throw new RDFParseException("Unexpected end of file");
        }

    }

    private static final class TQLWriter extends RDFWriterBase {

        private final char[] buffer = new char[BUFFER_SIZE];

        private int bufferIndex = 0;

        private final Writer writer;

        TQLWriter(final Writer writer) {
            this.writer = Preconditions.checkNotNull(writer);
        }

        @Override
        public RDFFormat getRDFFormat() {
            return FORMAT;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            // nothing to do
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // nothing to do
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            // nothing to do
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            try {
                emitResource(statement.getSubject());
                write(' ');
                emitURI(statement.getPredicate());
                write(' ');
                emitValue(statement.getObject());
                final Resource ctx = statement.getContext();
                if (ctx != null) {
                    write(' ');
                    emitResource(statement.getContext());
                }
                write(' ');
                write('.');
                write('\n');
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                flush();
                this.writer.flush();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        private void emitValue(final Value value) throws IOException, RDFHandlerException {
            if (value instanceof URI) {
                emitURI((URI) value);
            } else if (value instanceof BNode) {
                emitBNode((BNode) value);
            } else if (value instanceof Literal) {
                emitLiteral((Literal) value);
            }
        }

        private void emitResource(final Resource resource) throws IOException, RDFHandlerException {
            if (resource instanceof URI) {
                emitURI((URI) resource);
            } else if (resource instanceof BNode) {
                emitBNode((BNode) resource);
            }
        }

        private void emitURI(final URI uri) throws IOException, RDFHandlerException {
            final String string = uri.stringValue();
            final int length = string.length();
            write('<');
            for (int i = 0; i < length; ++i) {
                final char ch = string.charAt(i);
                switch (ch) {
                case '>':
                    write('\\');
                    write('>');
                    break;
                case '\\':
                    write('\\');
                    write('\\');
                    break;
                default:
                    write(ch);
                }
            }
            write('>');
        }

        private void emitBNode(final BNode bnode) throws IOException, RDFHandlerException {
            final String id = bnode.getID();
            write('_');
            write(':');
            final int length = id.length();
            for (int i = 0; i < length; ++i) {
                final char ch = id.charAt(i);
                if (!isLetterOrNumber(ch)) {
                    throw new RDFHandlerException("Illegal BNode ID: " + id);
                }
                write(ch);
            }
        }

        private void emitLiteral(final Literal literal) throws IOException, RDFHandlerException {
            final String label = literal.getLabel();
            final int length = label.length();
            write('"');
            for (int i = 0; i < length; ++i) {
                final char ch = label.charAt(i);
                switch (ch) {
                case '\\':
                    write('\\');
                    write('\\');
                    break;
                case '\t':
                    write('\\');
                    write('t');
                    break;
                case '\n':
                    write('\\');
                    write('n');
                    break;
                case '\r':
                    write('\\');
                    write('r');
                    break;
                case '\"':
                    write('\\');
                    write('\"');
                    break;
                default:
                    write(ch);
                }
            }
            write('"');
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                write('^');
                write('^');
                emitURI(datatype);
            } else {
                final String language = literal.getLanguage();
                if (language != null) {
                    write('@');
                    final int l = language.length();
                    for (int i = 0; i < l; ++i) {
                        final char ch = language.charAt(i);
                        if (!isLetter(ch)) {
                            throw new RDFHandlerException("Illegal language: " + language);
                        }
                        write(ch);
                    }
                }
            }
        }

        private void write(final char ch) throws IOException {
            if (this.bufferIndex >= BUFFER_SIZE) {
                flush();
            }
            this.buffer[this.bufferIndex++] = ch;
        }

        private void flush() throws IOException {
            this.writer.write(this.buffer, 0, this.bufferIndex);
            this.bufferIndex = 0;
        }

    }

}
