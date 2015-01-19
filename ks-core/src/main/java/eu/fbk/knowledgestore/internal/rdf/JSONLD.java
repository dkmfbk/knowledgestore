package eu.fbk.knowledgestore.internal.rdf;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.helpers.RDFWriterBase;
import org.openrdf.rio.helpers.RioSettingImpl;
import org.semarglproject.jsonld.JsonLdParser;
import org.semarglproject.rdf.ParseException;
import org.semarglproject.sink.CharSink;
import org.semarglproject.sink.QuadSink;

/**
 * Parser and writer factory for the JSON Linked Data (JSONLD) format.
 * <p>
 * This factory provides support to the parsing and generation of JSONLD RDF data via the Sesame
 * RIO API. The factory registers itself in the Sesame RIO API via files
 * {@code META-INF/services/org.openrdf.rio.RDFParserFactory} and
 * {@code META-INF/services/org.openrdf.rio.RDFWriterFactory}.
 * </p>
 * <p>
 * Parsing depends on the {@code Semargl} JSONLD parser (dependency
 * {@code org.semarglproject:semargl-jsonld} must be included), while writing is implemented
 * directly by this factory and can be configured via setting {@code #ROOT_TYPES}, which specifies
 * the types of RDF resources to be emitted as top level JSONLD nodes.
 * <p>
 */
public class JSONLD implements RDFParserFactory, RDFWriterFactory {

    /**
     * Optional setting specifying the {@code rdf:type}(s) of RDF resources to be emitted as top
     * level JSONLD nodes.
     */
    public static final RioSetting<Set<URI>> ROOT_TYPES = new RioSettingImpl<Set<URI>>(
            "eu.fbk.jsonld.roottypes", "The rdf:type(s) of RDF resources to be emitted "
                    + "as root (top level) nodes in the produced JSONLD", ImmutableSet.<URI>of());

    @Override
    public RDFFormat getRDFFormat() {
        return RDFFormat.JSONLD;
    }

    @Override
    public RDFParser getParser() {
        return new JSONLDParser();
    }

    @Override
    public RDFWriter getWriter(final OutputStream out) {
        return getWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    }

    @Override
    public RDFWriter getWriter(final Writer writer) {
        Preconditions.checkNotNull(writer);
        return new JSONLDWriter(writer);
    }

    private static class JSONLDParser extends RDFParserBase {

        @Override
        public RDFFormat getRDFFormat() {
            return RDFFormat.JSONLD;
        }

        @Override
        public void parse(final InputStream in, final String baseURI) throws IOException,
                RDFParseException, RDFHandlerException {
            parse(new InputStreamReader(in, Charsets.UTF_8), baseURI);
        }

        @Override
        public void parse(final Reader reader, final String baseURI) throws IOException,
                RDFParseException, RDFHandlerException {

            final QuadSink sink = new SesameSink(this.rdfHandler, this.valueFactory);
            try {
                final CharSink parser = JsonLdParser.connect(sink);
                parser.startStream();
                final char[] buffer = new char[4096];
                while (true) {
                    final int length = reader.read(buffer);
                    if (length < 0) {
                        break;
                    }
                    parser.process(buffer, 0, length);
                }
                parser.endStream();
            } catch (final ParseException ex) {
                throw new RDFParseException(ex.getMessage(), ex);
            }
        }

    }

    private static class JSONLDWriter extends RDFWriterBase {

        private static final int WINDOW = 32 * 1024;

        private final Writer writer;

        private final Map<String, String> prefixes; // namespace-to-prefix map

        private final Map<Resource, Map<Resource, Node>> nodes; // context-to-id-to-node map

        private Node lrsHead; // head of least recently seen (LRS) linked list

        private Node lrsTail; // tail of least recently seen (LRS) linked list

        private long counter; // statement counter;

        private int indent; // current indentation level

        private Resource emitContext; // context being currently emitted

        private Map<Resource, Node> emitContextNodes; // id-to-node map for current context

        @Nullable
        private Set<URI> rootTypes;

        JSONLDWriter(final Writer writer) {
            this.writer = writer;
            this.prefixes = Maps.newLinkedHashMap();
            this.nodes = Maps.newHashMap();
            this.lrsHead = null;
            this.lrsTail = null;
            this.counter = 0;
            this.indent = 1;
            this.rootTypes = null;
        }

        @Override
        public RDFFormat getRDFFormat() {
            return RDFFormat.JSONLD;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.rootTypes = getWriterConfig().get(ROOT_TYPES);
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            try {
                // comments cannot be emitted in JSONLD, but still we use them to flush output
                flush(true);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {

            // add only if emission to writer not started yet
            if (this.emitContextNodes == null) {
                this.prefixes.put(uri, prefix);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            // retrieve or create a node map for the statement context
            final Resource context = statement.getContext();
            Map<Resource, Node> nodes = this.nodes.get(context);
            if (nodes == null) {
                nodes = Maps.newHashMap();
                this.nodes.put(context, nodes);
            }

            // retrieve or create a node for the statement subject in the statement context
            final Resource subject = statement.getSubject();
            Node node = nodes.get(subject);
            if (node != null) {
                detach(node);
            } else {
                node = new Node(subject, context);
                nodes.put(subject, node);
            }
            attach(node, this.lrsTail); // move node at the end of LRS list
            node.counter = this.counter++; // update LRS statement counter
            node.statements.add(statement);
            if (statement.getPredicate().equals(RDF.TYPE)
                    && this.rootTypes.contains(statement.getObject())) {
                node.root = true;
            }

            try {
                flush(false); // emit nodes not seen in last WINDOW statement
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                flush(true);
                this.writer.append("]\n}");
                this.writer.flush();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        private void flush(final boolean force) throws IOException {

            // Emit preamble of JSONLD document if necessary and select context
            if (this.emitContextNodes == null
                    && (force || this.counter - this.lrsHead.counter >= WINDOW)) {
                this.writer.append("{\n\t\"@context\": {");
                if (!this.prefixes.isEmpty()) {
                    String separator = "\n\t\t";
                    for (final String namespace : Ordering.natural().sortedCopy(
                            this.prefixes.keySet())) {
                        final String prefix = this.prefixes.get(namespace);
                        this.writer.append(separator);
                        this.writer.append('\"');
                        emitString(prefix);
                        this.writer.append("\": \"");
                        emitString(namespace);
                        this.writer.append('\"');
                        separator = ",\n\t\t";
                    }
                    // this.writer.append("\n\t},");
                }
                this.writer.append("},\n\t\"@graph\": [");
            }

            // Emit all the nodes if force=true, otherwise limit to old nodes
            while (this.lrsHead != null
                    && (force || this.counter - this.lrsHead.counter >= WINDOW)) {

                // detect change of context
                final boolean sameContext = Objects.equal(this.lrsHead.context, this.emitContext);

                // otherwise, close old context if necessary, and add required comma
                if (this.emitContextNodes == null) {
                    this.emitContextNodes = this.nodes.get(this.lrsHead.context);
                } else {
                    if (!sameContext && this.emitContext != null) {
                        this.writer.append("]\n\t}");
                        --this.indent;
                    }
                    this.writer.append(',');
                    this.writer.append(' ');
                }

                // open new context if necessary
                if (!sameContext) {
                    if (this.lrsHead.context != null) {
                        this.writer.append("{\n\t\t\"@id\": ");
                        emit(this.lrsHead.context, false);
                        this.writer.append(",\n\t\t\"@graph\": [");
                        ++this.indent;
                    }
                    this.emitContext = this.lrsHead.context;
                    this.emitContextNodes = this.nodes.get(this.lrsHead.context);
                }

                // emit the node
                emitNode(this.emitContextNodes.get(this.lrsHead.id));
            }

            // if force=true, close the context if necessary
            if (force && this.emitContext != null) {
                this.writer.append("]\n\t}");
                --this.indent;
                this.emitContext = null;
            }
        }

        private void emit(final Value value, final boolean expand) throws IOException {

            if (value instanceof Literal) {
                emitLiteral((Literal) value);
            } else {
                final Node node = expand ? this.emitContextNodes.get(value) : null;
                if (node != null && !node.root) {
                    emitNode(node);
                } else {
                    if (expand) {
                        this.writer.append("{\"@id\": ");
                    }
                    if (value instanceof BNode) {
                        emitBNode((BNode) value);
                    } else if (value instanceof URI) {
                        emitURI((URI) value);
                    }
                    if (expand) {
                        this.writer.append('}');
                    }
                }
            }
        }

        private void emitNode(final Node node) throws IOException {

            this.emitContextNodes.remove(node.id);
            detach(node);

            ++this.indent;
            this.writer.append('{');
            emitNewline();
            this.writer.append("\"@id\": ");
            emit(node.id, false);

            boolean startProperty = true;
            boolean isTypeProperty = true;
            boolean insideArray = false;

            Collections.sort(node.statements, StatementComparator.INSTANCE);
            final int statementCount = node.statements.size();
            for (int i = 0; i < statementCount; ++i) {

                final Statement statement = node.statements.get(i);
                final URI property = statement.getPredicate();
                final boolean last = i == statementCount - 1
                        || !property.equals(node.statements.get(i + 1).getPredicate());

                if (startProperty) {
                    this.writer.append(',');
                    emitNewline();
                    isTypeProperty = property.equals(RDF.TYPE);
                    if (isTypeProperty) {
                        this.writer.append("\"@type\"");
                    } else {
                        emit(property, false);
                    }
                    this.writer.append(": ");
                    insideArray = !last;
                    if (insideArray) {
                        this.writer.append('[');
                    }
                } else {
                    this.writer.append(", ");
                }

                emit(statement.getObject(), !isTypeProperty);

                startProperty = last;
                if (startProperty && insideArray) {
                    this.writer.append(']');
                }
            }

            --this.indent;
            emitNewline();
            this.writer.append('}');
        }

        private void emitBNode(final BNode bnode) throws IOException {
            this.writer.append("\"_:");
            emitString(bnode.getID());
            this.writer.append('\"');
        }

        private void emitURI(final URI uri) throws IOException {
            final String prefix = this.prefixes.get(uri.getNamespace());
            this.writer.append('\"');
            if (prefix != null) {
                emitString(prefix);
                this.writer.append(':');
                emitString(uri.getLocalName());
            } else {
                emitString(uri.stringValue());
            }
            this.writer.append('\"');
        }

        private void emitLiteral(final Literal literal) throws IOException {
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                this.writer.append("{\"@type\": ");
                emit(datatype, false);
                this.writer.append(", \"@value\": \"");
            } else {
                final String language = literal.getLanguage();
                if (language != null) {
                    this.writer.append("{\"@language\": \"");
                    emitString(language);
                    this.writer.append("\", \"@value\": \"");
                } else {
                    this.writer.append("{\"@value\": \"");
                }
            }
            emitString(literal.getLabel());
            this.writer.append("\"}");
        }

        private void emitString(final String string) throws IOException {
            final int length = string.length();
            for (int i = 0; i < length; ++i) {
                final char ch = string.charAt(i);
                if (ch == '\"' || ch == '\\') {
                    this.writer.append('\\').append(ch);
                } else if (Character.isISOControl(ch)) {
                    if (ch == '\n') {
                        this.writer.append('\\').append('n');
                    } else if (ch == '\r') {
                        this.writer.append('\\').append('r');
                    } else if (ch == '\t') {
                        this.writer.append('\\').append('t');
                    } else if (ch == '\b') {
                        this.writer.append('\\').append('b');
                    } else if (ch == '\f') {
                        this.writer.append('\\').append('f');
                    } else {
                        this.writer.append(String.format("\\u%04x", (int) ch));
                    }
                } else {
                    this.writer.append(ch);
                }
            }
        }

        private void emitNewline() throws IOException {
            this.writer.append('\n');
            for (int i = 0; i < this.indent; ++i) {
                this.writer.append('\t');
            }
        }

        private void detach(final Node node) {
            final Node prev = node.lrsPrev;
            final Node next = node.lrsNext;
            if (prev != null) {
                prev.lrsNext = next;
            } else {
                this.lrsHead = next;
            }
            if (next != null) {
                next.lrsPrev = prev;
            } else {
                this.lrsTail = prev;
            }
        }

        private void attach(final Node node, final Node prev) {
            Node next;
            if (prev == null) {
                next = this.lrsHead;
                this.lrsHead = node;
            } else {
                next = prev.lrsNext;
                prev.lrsNext = node;
            }
            if (next == null) {
                this.lrsTail = node;
            } else {
                next.lrsPrev = node;
            }
            node.lrsPrev = prev;
            node.lrsNext = next;
        }

        private static final class Node {

            final Resource id; // node identifier (statement subject)

            final Resource context; // node context

            final List<Statement> statements; // node statements

            long counter; // last recently seen (LRS) counter

            Node lrsPrev; // pointer to prev node in LRS linked list

            Node lrsNext; // pointer to next node in LRS linked list

            boolean root;

            Node(final Resource id, final Resource context) {
                this.id = id;
                this.context = context;
                this.statements = Lists.newArrayList();
            }

        }

    }

    private static final class SesameSink implements QuadSink {

        private final RDFHandler handler;

        private final ValueFactory factory;

        SesameSink(final RDFHandler handler, final ValueFactory factory) {
            this.handler = handler;
            this.factory = factory;
        }

        @Override
        public void setBaseUri(final String baseUri) {
        }

        @Override
        public boolean setProperty(final String key, final Object value) {
            return false;
        }

        @Override
        public void startStream() throws ParseException {
            try {
                this.handler.startRDF();
            } catch (final RDFHandlerException e) {
                throw new ParseException(e);
            }
        }

        @Override
        public void addNonLiteral(final String subj, final String pred, final String obj) {
            emit(subj, pred, obj, false, null, null, null);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang) {
            emit(subj, pred, obj, true, lang, null, null);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt) {
            emit(subj, pred, obj, true, null, dt, null);
        }

        @Override
        public void addNonLiteral(final String subj, final String pred, final String obj,
                final String ctx) {
            emit(subj, pred, obj, false, null, null, ctx);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang, final String ctx) {
            emit(subj, pred, obj, true, lang, null, ctx);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt, final String ctx) {
            emit(subj, pred, obj, true, null, dt, ctx);
        }

        @Override
        public void endStream() throws ParseException {
            try {
                this.handler.endRDF();
            } catch (final RDFHandlerException e) {
                throw new ParseException(e);
            }
        }

        private void emit(final String subj, final String pred, final String obj,
                final boolean literal, @Nullable final String lang, @Nullable final String dt,
                @Nullable final String ctx) {

            final Resource s = subj.startsWith("_:") ? this.factory.createBNode(subj.substring(2))
                    : this.factory.createURI(subj);

            final URI p = this.factory.createURI(pred);

            final Value o;
            if (!literal) {
                o = obj.startsWith("_:") ? this.factory.createBNode(obj.substring(2))
                        : this.factory.createURI(obj);
            } else if (lang != null) {
                o = this.factory.createLiteral(obj, lang);
            } else if (dt != null) {
                o = this.factory.createLiteral(obj, this.factory.createURI(dt));
            } else {
                o = this.factory.createLiteral(obj);
            }

            Statement stmt = null;
            if (ctx == null) {
                stmt = this.factory.createStatement(s, p, o);
            } else {
                final Resource c = ctx.startsWith("_:") ? this.factory.createBNode(ctx
                        .substring(2)) : this.factory.createURI(ctx);
                stmt = this.factory.createStatement(s, p, o, c);
            }

            try {
                this.handler.handleStatement(stmt);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    private static final class StatementComparator implements Comparator<Statement> {

        private static final StatementComparator INSTANCE = new StatementComparator();

        @Override
        public int compare(final Statement first, final Statement second) {
            int result = compare(first.getPredicate(), second.getPredicate());
            if (result == 0) {
                result = compare(first.getObject(), second.getObject());
            }
            return result;
        }

        private int compare(final Value first, final Value second) {

            if (first instanceof Literal) {
                if (second instanceof Literal) {
                    int result = first.stringValue().compareTo(second.stringValue());
                    if (result == 0) {
                        final Literal firstLit = (Literal) first;
                        final Literal secondLit = (Literal) second;
                        final URI firstDt = firstLit.getDatatype();
                        final URI secondDt = secondLit.getDatatype();
                        result = firstDt == null ? secondDt == null ? 0 : -1
                                : secondDt == null ? 1 : firstDt.stringValue().compareTo(
                                        secondDt.stringValue());
                        if (result == 0) {
                            final String firstLang = firstLit.getLanguage();
                            final String secondLang = secondLit.getLanguage();
                            result = firstLang == null ? secondLang == null ? 0 : -1
                                    : secondLang == null ? 1 : firstLang.compareTo(secondLang);
                        }
                    }
                    return result;
                } else {
                    return -1;
                }

            } else if (first instanceof URI) {
                if (second instanceof URI) {
                    int result = first.stringValue().compareTo(second.stringValue());
                    if (result != 0) {
                        if (first.equals(RDF.TYPE)) { // rdf:type always first
                            result = -1;
                        } else if (second.equals(RDF.TYPE)) {
                            result = 1;
                        }
                    }
                    return result;
                } else if (second instanceof Literal) {
                    return 1;
                } else {
                    return -1;
                }

            } else if (first instanceof BNode) {
                if (second instanceof BNode) {
                    return first.stringValue().compareTo(second.stringValue());
                } else {
                    return 1;
                }
            }

            throw new IllegalArgumentException("Invalid arguments: " + first + ", " + second);
        }
    }

}
