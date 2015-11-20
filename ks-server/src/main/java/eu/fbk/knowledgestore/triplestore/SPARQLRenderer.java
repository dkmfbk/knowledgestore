package eu.fbk.knowledgestore.triplestore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.Add;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.BinaryValueOperator;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Bound;
import org.openrdf.query.algebra.Clear;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.CompareAll;
import org.openrdf.query.algebra.CompareAny;
import org.openrdf.query.algebra.Copy;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.Create;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.query.algebra.DeleteData;
import org.openrdf.query.algebra.DescribeOperator;
import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.GroupConcat;
import org.openrdf.query.algebra.GroupElem;
import org.openrdf.query.algebra.IRIFunction;
import org.openrdf.query.algebra.If;
import org.openrdf.query.algebra.In;
import org.openrdf.query.algebra.InsertData;
import org.openrdf.query.algebra.Intersection;
import org.openrdf.query.algebra.IsBNode;
import org.openrdf.query.algebra.IsLiteral;
import org.openrdf.query.algebra.IsNumeric;
import org.openrdf.query.algebra.IsResource;
import org.openrdf.query.algebra.IsURI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Label;
import org.openrdf.query.algebra.Lang;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Like;
import org.openrdf.query.algebra.ListMemberOperator;
import org.openrdf.query.algebra.Load;
import org.openrdf.query.algebra.LocalName;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.MathExpr.MathOp;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Modify;
import org.openrdf.query.algebra.Move;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Namespace;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.Sample;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.queryrender.QueryRenderer;

final class SPARQLRenderer implements QueryRenderer {

    private static final Map<String, String> NAMES;

    static {
        final Map<String, String> names = Maps.newHashMap();
        names.put("RAND", "RAND");
        names.put("TZ", "TZ");
        names.put("NOW", "NOW");
        names.put("UUID", "UUID");
        names.put("STRUUID", "STRUUID");
        names.put("MD5", "MD5");
        names.put("SHA1", "SHA1");
        names.put("SHA256", "SHA256");
        names.put("SHA384", "SHA384");
        names.put("SHA512", "SHA512");
        names.put("STRLANG", "STRLANG");
        names.put("STRDT", "STRDT");
        names.put(FN.STRING_LENGTH.stringValue(), "STRLEN");
        names.put(FN.SUBSTRING.stringValue(), "SUBSTR");
        names.put(FN.UPPER_CASE.stringValue(), "UCASE");
        names.put(FN.LOWER_CASE.stringValue(), "LCASE");
        names.put(FN.STARTS_WITH.stringValue(), "STRSTARTS");
        names.put(FN.ENDS_WITH.stringValue(), "STRENDS");
        names.put(FN.CONTAINS.stringValue(), "CONTAINS");
        names.put(FN.SUBSTRING_BEFORE.stringValue(), "STRBEFORE");
        names.put(FN.SUBSTRING_AFTER.stringValue(), "STRAFTER");
        names.put(FN.ENCODE_FOR_URI.stringValue(), "ENCODE_FOR_URI");
        names.put(FN.CONCAT.stringValue(), "CONCAT");
        names.put(FN.NAMESPACE + "matches", "REGEX");
        names.put(FN.REPLACE.stringValue(), "REPLACE");
        names.put(FN.NUMERIC_ABS.stringValue(), "ABS");
        names.put(FN.NUMERIC_ROUND.stringValue(), "ROUND");
        names.put(FN.NUMERIC_CEIL.stringValue(), "CEIL");
        names.put(FN.NUMERIC_FLOOR.stringValue(), "FLOOR");
        names.put(FN.YEAR_FROM_DATETIME.stringValue(), "YEAR");
        names.put(FN.MONTH_FROM_DATETIME.stringValue(), "MONTH");
        names.put(FN.DAY_FROM_DATETIME.stringValue(), "DAY");
        names.put(FN.HOURS_FROM_DATETIME.stringValue(), "HOURS");
        names.put(FN.MINUTES_FROM_DATETIME.stringValue(), "MINUTES");
        names.put(FN.SECONDS_FROM_DATETIME.stringValue(), "SECONDS");
        names.put(FN.TIMEZONE_FROM_DATETIME.stringValue(), "TIMEZONE");
        NAMES = Collections.unmodifiableMap(names);

    }

    private final Map<String, String> prefixes;

    private final boolean forceSelect;

    public SPARQLRenderer(@Nullable final Map<String, String> prefixes,
            @Nullable final boolean forceSelect) {
        this.prefixes = prefixes != null ? prefixes : Collections.<String, String>emptyMap();
        this.forceSelect = forceSelect;
    }

    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.SPARQL;
    }

    @Override
    public String render(final ParsedQuery query) {
        return render(query.getTupleExpr(), query.getDataset());
    }

    public String render(final TupleExpr expr, final Dataset dataset) {
        final Rendering rendering = new Rendering(expr, dataset);
        final StringBuilder builder = new StringBuilder();
        boolean newline = false;
        if (!rendering.namespaces.isEmpty()) {
            for (final String namespace : Ordering.natural().sortedCopy(rendering.namespaces)) {
                final String prefix = this.prefixes.get(namespace);
                if ("bif".equals(prefix) && "http://www.openlinksw.com/schema/sparql/extensions#" //
                        .equals(namespace)) {
                    continue; // do not emit Virtuoso bif: binding, as Virtuoso will complain
                }
                builder.append("PREFIX ").append(prefix).append(": <");
                escape(namespace, builder);
                builder.append(">\n");
                newline = true;
            }
        }
        if (rendering.base != null) {
            builder.append("BASE <");
            escape(rendering.base, builder);
            builder.append(">\n");
            newline = true;
        }
        if (newline) {
            builder.append("\n");
        }
        builder.append(rendering.body);
        return builder.toString();
    }

    // Helper functions

    private static void escape(final String label, final StringBuilder builder) {
        final int length = label.length();
        for (int i = 0; i < length; ++i) {
            final char c = label.charAt(i);
            if (c == '\\') {
                builder.append("\\\\");
            } else if (c == '"') {
                builder.append("\\\"");
            } else if (c == '\n') {
                builder.append("\\n");
            } else if (c == '\r') {
                builder.append("\\r");
            } else if (c == '\t') {
                builder.append("\\t");
            }
            // TODO: not accepted by Virtuoso :-(
            // else if (c >= 0x0 && c <= 0x8 || c == 0xB || c == 0xC || c >= 0xE && c <= 0x1F
            // || c >= 0x7F && c <= 0xFFFF) {
            // builder.append("\\u").append(
            // Strings.padStart(Integer.toHexString(c).toUpperCase(), 4, '0'));
            // } else if (c >= 0x10000 && c <= 0x10FFFF) {
            // builder.append("\\U").append(
            // Strings.padStart(Integer.toHexString(c).toUpperCase(), 8, '0'));
            // }
            else {
                builder.append(Character.toString(c));
            }
        }
    }

    private static String sanitize(final String string) {
        final int length = string.length();
        final StringBuilder builder = new StringBuilder(length + 10);
        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            if (Character.isLetter(ch) || ch == '_' || i > 0 && Character.isDigit(ch)) {
                builder.append(ch);
            } else {
                builder.append("_");
            }
        }
        return builder.toString();
    }

    private static <T> boolean equalOrNull(@Nullable final T first, @Nullable final T second) {
        return first != null && first.equals(second) || first == null && second == null;
    }

    private static <T> T defaultIfNull(@Nullable final T value, @Nullable final T defaultValue) {
        return value != null ? value : defaultValue;
    }

    private static List<StatementPattern> getBGP(final QueryModelNode n) {
        if (n instanceof StatementPattern) {
            return Collections.singletonList((StatementPattern) n);
        } else if (!(n instanceof Join)) {
            return null;
        }
        final Join j = (Join) n;
        final List<StatementPattern> l = getBGP(j.getLeftArg());
        final List<StatementPattern> r = getBGP(j.getRightArg());
        if (l == null || r == null) {
            return null;
        }
        if (l.isEmpty()) {
            return r;
        } else if (r.isEmpty()) {
            return l;
        } else if (!equalOrNull(l.get(0).getContextVar(), r.get(0).getContextVar())) {
            return null;
        } else {
            final List<StatementPattern> s = new ArrayList<StatementPattern>(l.size() + r.size());
            s.addAll(l);
            s.addAll(r);
            return s;
        }
    }

    private static int getVarRefs(final QueryModelNode node, final String name) {
        final AtomicInteger count = new AtomicInteger(0);
        node.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                if (var.getName().equals(name)) {
                    count.set(count.get() + 1);
                }
            }

        });
        return count.get();
    }

    private static ValueExpr getVarExpr(final QueryModelNode node, final String name) {
        final AtomicReference<ValueExpr> result = new AtomicReference<ValueExpr>(null);
        node.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException {
                if (result.get() == null) {
                    super.meetNode(node);
                }
            }

            @Override
            public void meet(final Var var) {
                if (var.getName().equals(name) && var.getValue() != null) {
                    result.set(new ValueConstant(var.getValue()));
                }
            }

            @Override
            public void meet(final ExtensionElem node) throws RuntimeException {
                if (node.getName().equals(name)) {
                    result.set(node.getExpr());
                } else {
                    super.meet(node);
                }
            }

        });
        return result.get();
    }

    private final class Rendering implements QueryModelVisitor<RuntimeException> {

        final TupleExpr root;

        @Nullable
        final Dataset dataset;

        final String body;

        String base;

        private final StringBuilder builder;

        private final Set<String> namespaces;

        private int indent;

        private Rendering(final TupleExpr node, @Nullable final Dataset dataset) {
            this.root = new QueryRoot(Preconditions.checkNotNull(node));
            this.dataset = dataset;
            this.builder = new StringBuilder();
            this.namespaces = Sets.newHashSet();
            this.indent = 0;
            emit(Query.create(this.root, this.dataset, SPARQLRenderer.this.forceSelect));
            this.body = this.builder.toString();
            this.builder.setLength(0);
        }

        // BASIC RENDERING METHODS (STRING, VALUES, CONDITIONALS, NEWLINE AND BRACES, ERRORS)

        private Rendering emitIf(final boolean condition, final Object object) {
            if (condition) {
                emit(object);
            }
            return this;
        }

        private Rendering emit(final Iterable<?> values, final String separator) {
            boolean first = true;
            for (final Object value : values) {
                if (!first) {
                    emit(separator);
                }
                emit(value);
                first = false;
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        private Rendering emit(final Object value) {
            if (value instanceof String) {
                return emit((String) value);
            } else if (value instanceof QueryModelNode) {
                emit((QueryModelNode) value);
            } else if (value instanceof BNode) {
                emit((BNode) value);
            } else if (value instanceof URI) {
                emit((URI) value);
            } else if (value instanceof Literal) {
                emit((Literal) value);
            } else if (value instanceof List<?>) {
                emit((List<StatementPattern>) value);
            } else if (value instanceof Query) {
                emit((Query) value);
            }
            return this;
        }

        private Rendering emit(final String string) {
            this.builder.append(string);
            return this;
        }

        private Rendering emit(final Literal literal) {
            if (XMLSchema.INTEGER.equals(literal.getDatatype())) {
                this.builder.append(literal.getLabel());
            } else {
                this.builder.append("\"");
                escape(literal.getLabel(), this.builder);
                this.builder.append("\"");
                // Differences between string management in SPARQL 1.1 and RDF 1.1
                if (literal.getDatatype() != null && !literal.getDatatype().equals(XMLSchema.STRING)) {
                    this.builder.append("^^");
                    emit(literal.getDatatype());
                } else if (literal.getLanguage() != null) {
                    this.builder.append("@").append(literal.getLanguage());
                }
            }
            return this;
        }

        private Rendering emit(final BNode bnode) {
            this.builder.append("_:").append(bnode.getID());
            return this;
        }

        private Rendering emit(final URI uri) {
            if (uri.getNamespace().equals("http://www.openlinksw.com/schema/sparql/extensions#")) {
                this.builder.append("bif:").append(uri.getLocalName()); // for Virtuoso builtins
            } else {
                final String prefix = SPARQLRenderer.this.prefixes.get(uri.getNamespace());
                if (prefix != null) {
                    if (this.namespaces != null) {
                        this.namespaces.add(uri.getNamespace());
                    }
                    this.builder.append(prefix).append(':').append(uri.getLocalName());
                } else {
                    this.builder.append("<");
                    escape(uri.toString(), this.builder);
                    this.builder.append(">");
                }
            }
            return this;
        }

        private Rendering emit(final List<StatementPattern> bgp) {
            if (bgp.isEmpty()) {
                return this;
            }
            final Var c = bgp.get(0).getContextVar();
            if (c != null) {
                emit("GRAPH ").emit(c).emit(" ").openBrace();
            }
            StatementPattern l = null;
            for (final StatementPattern n : bgp) {
                final Var s = n.getSubjectVar();
                final Var p = n.getPredicateVar();
                final Var o = n.getObjectVar();
                if (l == null) {
                    emit(s).emit(" ").emit(p).emit(" ").emit(o); // s p o
                } else if (!l.getSubjectVar().equals(n.getSubjectVar())) {
                    emit(" .").newline().emit(s).emit(" ").emit(p).emit(" ").emit(o); // .\n s p o
                } else if (!l.getPredicateVar().equals(n.getPredicateVar())) {
                    emit(" ;").newline().emit("\t").emit(p).emit(" ").emit(o); // ;\n\t p o
                } else if (!l.getObjectVar().equals(n.getObjectVar())) {
                    emit(" , ").emit(o); // , o
                }
                l = n;
            }
            emit(" .");
            if (c != null) {
                closeBrace();
            }
            return this;
        }

        private Rendering emit(final Query query) {
            if (query.root != this.root) {
                openBrace();
            }
            if (query.form == Form.ASK) {
                emit("ASK");
            } else if (query.form == Form.CONSTRUCT) {
                emit("CONSTRUCT ").openBrace().emit(query.construct).closeBrace();
            } else if (query.form == Form.DESCRIBE) {
                emit("DESCRIBE");
                for (final ProjectionElem p : query.select) {
                    final ExtensionElem e = p.getSourceExpression();
                    emit(" ").emit(
                            e != null && e.getExpr() instanceof ValueConstant ? e.getExpr() : p);
                }
            } else if (query.form == Form.SELECT) {
                emit("SELECT");
                if (query.modifier != null) {
                    emit(" ").emit(query.modifier.toString().toUpperCase());
                }
                if (query.select.isEmpty()) {
                    int count = 0;
                    for (final String var : query.where.getBindingNames()) {
                        final ValueExpr expr = getVarExpr(query.where, var);
                        if (!var.startsWith("-")) {
                            if (expr == null) {
                                emit(" ?").emit(var);
                            } else {
                                emit(" (").emit(expr).emit(" AS ?").emit(var).emit(")");
                            }
                            ++count;
                        }
                    }
                    if (count == 0) {
                        emit(" *");
                    }
                } else {
                    emit(" ").emit(query.select, " ");
                }
            }
            if (query.from != null) {
                for (final URI uri : query.from.getDefaultGraphs()) {
                    newline().emit("FROM ").emit(uri);
                }
                for (final URI uri : query.from.getNamedGraphs()) {
                    newline().emit("FROM NAMED ").emit(uri);
                }
            }
            if (query.form != Form.DESCRIBE || !(query.where instanceof SingletonSet)) {
                newline().emit("WHERE ").openBrace().emit(query.where).closeBrace();
            }
            if (!query.groupBy.isEmpty()) {
                newline().emit("GROUP BY");
                for (final ProjectionElem n : query.groupBy) {
                    emit(" ?").emit(n.getTargetName());
                }
            }
            if (query.having != null) {
                newline().emit("HAVING (").emit(query.having).emit(")");
            }
            if (!query.orderBy.isEmpty()) {
                newline().emit("ORDER BY ").emit(query.orderBy, " ");
            }
            if (query.form != Form.ASK) {
                if (query.offset != null) {
                    newline().emit("OFFSET " + query.offset);
                }
                if (query.limit != null) {
                    newline().emit("LIMIT " + query.limit);
                    // newline().emit("LIMIT " + (query.limit + 1)); // TODO Virtuoso fix :-(
                }
            }
            if (query.root != this.root) {
                closeBrace();
            }
            return this;
        }

        private Rendering emit(final QueryModelNode n) {
            final QueryModelNode p = n.getParentNode();
            final boolean braces = n instanceof TupleExpr && p != null
                    && !(p instanceof TupleExpr);
            if (braces) {
                openBrace();
            }
            n.visit(this);
            if (braces) {
                closeBrace();
            }
            return this;
        }

        private Rendering emit(final QueryModelNode node, final boolean parenthesis) { // TODO
            if (parenthesis) {
                if (node instanceof TupleExpr) {
                    openBrace();
                } else {
                    emit("(");
                }
            }
            emit(node);
            if (parenthesis) {
                if (node instanceof TupleExpr) {
                    closeBrace();
                } else {
                    emit(")");
                }
            }
            return this;
        }

        private Rendering openBrace() {
            emit("{");
            ++this.indent;
            newline();
            return this;
        }

        private Rendering closeBrace() {
            --this.indent;
            newline();
            emit("}");
            return this;
        }

        private Rendering newline() {
            emit("\n");
            for (int i = 0; i < this.indent; ++i) {
                emit("\t");
            }
            return this;
        }

        private Rendering fail(final String message, final QueryModelNode node) {
            throw new IllegalArgumentException("SPARQL rendering failed. " + message
                    + (node == null ? "null" : node.getClass().getSimpleName() + "\n" + node));
        }

        // TupleExpr: root query nodes

        @Override
        public void meet(final OrderElem n) {
            emit(n.isAscending() ? "ASC(" : "DESC(").emit(n.getExpr()).emit(")");
        }

        @Override
        public void meet(final ProjectionElemList node) {
            emit(node.getElements(), " ");
        }

        @Override
        public void meet(final ProjectionElem n) {

            final String source = n.getSourceName();
            final String target = n.getTargetName();
            final ValueExpr expr = n.getSourceExpression() == null ? null : n
                    .getSourceExpression().getExpr();

            if (target.startsWith("-")) {
                if (expr != null) {
                    emit("(").emit(expr).emit(" AS ?").emit(sanitize(target)).emit(")");
                }
            } else if (expr != null) {
                emit("(").emit(expr).emit(" AS ?").emit(target).emit(")");
            } else if (!equalOrNull(source, target)) {
                emit("(?").emit(source).emit(" AS ?").emit(target).emit(")");
            } else {
                emit("?").emit(target);
            }
        }

        @Override
        public void meet(final GroupElem n) {
            final ProjectionElem e = new ProjectionElem();
            e.setTargetName(n.getName());
            e.setSourceName(n.getName());
            e.setSourceExpression(new ExtensionElem(n.getOperator(), n.getName()));
            meet(e);
        }

        @Override
        public void meet(final DescribeOperator n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final QueryRoot n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Projection n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final MultiProjection n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Distinct n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Reduced n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Group n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Order n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Slice n) {
            emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        // TupleExpr: leaf nodes

        @Override
        public void meet(final EmptySet n) {
            final QueryModelNode p = n.getParentNode();
            if (p.getParentNode() != null && !(p.getParentNode() instanceof QueryRoot)) {
                throw new IllegalArgumentException(
                        "Cannot translate EmptySet inside the body of a query / update operation");
            }
            emit("CONSTRUCT {} WHERE {}");
        }

        @Override
        public void meet(final SingletonSet n) {
            // nothing to do: braces, if necessary, are emitted by parent
        }

        @Override
        public void meet(final BindingSetAssignment n) {

            final Set<String> names = n.getBindingNames();

            if (names.isEmpty()) {
                emit("VALUES {}");

            } else if (names.size() == 1) {
                final String name = names.iterator().next();
                emit("VALUES ?").emit(name).emit(" ").openBrace();
                boolean first = true;
                for (final BindingSet bindings : n.getBindingSets()) {
                    emitIf(!first, " ").emit(defaultIfNull(bindings.getValue(name), "UNDEF"));
                    first = false;
                }
                closeBrace();

            } else {
                emit("VALUES (?").emit(names, " ?").emit(") ").openBrace();
                boolean firstBinding = true;
                for (final BindingSet bindings : n.getBindingSets()) {
                    if (!firstBinding) {
                        newline();
                    }
                    emit("(");
                    boolean first = true;
                    for (final String name : names) {
                        emitIf(!first, " ").emit(defaultIfNull(bindings.getValue(name), "UNDEF"));
                        first = false;
                    }
                    emit(")");
                    firstBinding = false;
                }
                closeBrace();
            }
        }

        @Override
        public void meet(final StatementPattern n) {
            emit(getBGP(n));
        }

        // TupleExpr: unary

        @Override
        public void meet(final Extension n) {
            emit(n.getArg());
            if (!(n.getArg() instanceof SingletonSet)) {
                newline();
            }
            boolean first = true;
            for (final ExtensionElem e : n.getElements()) {
                final ValueExpr expr = e.getExpr();
                if (!(expr instanceof Var) || !((Var) expr).getName().equals(e.getName())) {
                    if (!first) {
                        newline();
                    }
                    emit("BIND (").emit(expr).emit(" AS ?").emit(e.getName()).emit(")");
                    first = false;
                }
            }
        }

        @Override
        public void meet(final ExtensionElem n) {
            throw new Error("Should not be directly called");
        }

        @Override
        public void meet(final Filter n) {
            final ValueExpr cond = n.getCondition();
            final boolean nopar = cond instanceof Exists || cond instanceof Not
                    && ((Not) cond).getArg() instanceof Exists;
            emit(n.getArg());
            if (!(n.getArg() instanceof SingletonSet)) {
                newline();
            }
            emit("FILTER ").emit(cond, !nopar);
        }

        @Override
        public void meet(final Service n) {
            newline().emit("SERVICE ").emitIf(n.isSilent(), "SILENT ").openBrace()
                    .emit(n.getServiceExpr()).closeBrace().emit(" ").emit(n.getServiceRef());
        }

        // TupleExpr: binary

        @Override
        public void meet(final Join n) {
            final List<StatementPattern> bgp = getBGP(n);
            if (bgp != null) {
                emit(bgp);
            } else {
                final TupleExpr l = n.getLeftArg();
                final TupleExpr r = n.getRightArg();
                final boolean norpar = r instanceof Join || r instanceof StatementPattern
                        || r instanceof SingletonSet || r instanceof Service || r instanceof Union
                        || r instanceof BindingSetAssignment || r instanceof ArbitraryLengthPath;
                emit(l).newline().emit(r, !norpar);
            }
        }

        @Override
        public void meet(final LeftJoin n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getCondition() == null ? n.getRightArg() : //
                    new Filter(n.getRightArg(), n.getCondition());
            emit(l);
            if (!(l instanceof SingletonSet)) {
                newline();
            }
            emit("OPTIONAL ").emit(r, true);
        }

        @Override
        public void meet(final Union n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getRightArg();
            final ZeroLengthPath p = l instanceof ZeroLengthPath ? (ZeroLengthPath) l
                    : r instanceof ZeroLengthPath ? (ZeroLengthPath) r : null;
            if (p == null) {
                emit(l, !(l instanceof Union)).emit(" UNION ").emit(r, !(r instanceof Union));
            } else {
                final Var s = p.getSubjectVar();
                final Var o = p.getObjectVar();
                final Var c = p.getContextVar();
                if (c != null) {
                    emit("GRAPH ").emit(c).emit(" ").openBrace();
                }
                emit(s).emit(" ").emitPropertyPath(n, s, o).emit(" ").emit(o);
                if (c != null) {
                    closeBrace();
                }
            }
        }

        @Override
        public void meet(final Difference n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getRightArg();
            emit(l, true).emit(" MINUS ").emit(r, true);
        }

        // TupleExpr: paths

        @Override
        public void meet(final ArbitraryLengthPath n) {
            final Var s = n.getSubjectVar();
            final Var o = n.getObjectVar();
            final Var c = n.getContextVar();
            if (c != null) {
                emit("GRAPH ").emit(c).openBrace();
            }
            emit(s).emit(" ").emitPropertyPath(n, s, o).emit(" ").emit(o).emit(" .");
            if (c != null) {
                closeBrace();
            }
        }

        @Override
        public void meet(final ZeroLengthPath node) {
            throw new Error("Should not be directly called");
        }

        private Rendering emitPropertyPath(final TupleExpr node, final Var start, final Var end) {

            // Note: elt1 / elt2 and ^(complex exp) do not occur in Sesame algebra

            final boolean parenthesis = !(node instanceof StatementPattern)
                    && (node.getParentNode() instanceof ArbitraryLengthPath || node
                            .getParentNode() instanceof Union);

            emitIf(parenthesis, "(");

            if (node instanceof StatementPattern) {
                // handles iri, ^iri
                final StatementPattern pattern = (StatementPattern) node;
                final boolean inverse = isInversePath(pattern, start, end);
                if (!pattern.getPredicateVar().hasValue()
                        || !pattern.getPredicateVar().isAnonymous()) {
                    fail("Unsupported path expression. Check node: ", node);
                }
                emitIf(inverse, "^").emit(pattern.getPredicateVar().getValue());

            } else if (node instanceof Join) {
                final Join j = (Join) node;
                final TupleExpr l = j.getLeftArg();
                final TupleExpr r = j.getRightArg();
                final StatementPattern s = l instanceof StatementPattern ? (StatementPattern) l
                        : r instanceof StatementPattern ? (StatementPattern) r : null;
                if (s == null) {
                    return fail("Cannot process property path", node);
                }
                final Var m = s.getSubjectVar().equals(start) || s.getSubjectVar().equals(end) ? s
                        .getObjectVar() : s.getSubjectVar();
                emitPropertyPath(l, start, m);
                emit("/");
                emitPropertyPath(r, m, end);

            } else if (node instanceof ArbitraryLengthPath) {
                // handles elt*, elt+
                final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
                Preconditions.checkArgument(path.getMinLength() <= 1, "Invalid path length");
                emitPropertyPath(path.getPathExpression(), start, end).emit(
                        path.getMinLength() == 0 ? "*" : "+");

            } else if (node instanceof Union) {
                // handles elt?, elt1|elt2|...
                final Union union = (Union) node;
                if (union.getLeftArg() instanceof ZeroLengthPath) {
                    emitPropertyPath(union.getRightArg(), start, end).emit("?");
                } else if (union.getRightArg() instanceof ZeroLengthPath) {
                    emitPropertyPath(union.getLeftArg(), start, end).emit("?");
                } else {
                    emitPropertyPath(union.getLeftArg(), start, end);
                    emit("|");
                    emitPropertyPath(union.getRightArg(), start, end);
                }

            } else if (node instanceof Filter) {
                // handles !iri, !(iri1,iri2,...) with possibly inverse properties
                final Filter filter = (Filter) node;

                Preconditions.checkArgument(filter.getArg() instanceof StatementPattern);
                final StatementPattern pattern = (StatementPattern) filter.getArg();
                final boolean inverse = isInversePath(pattern, start, end);
                Preconditions.checkArgument(!pattern.getPredicateVar().hasValue()
                        && pattern.getPredicateVar().isAnonymous());

                final Set<URI> negatedProperties = Sets.newLinkedHashSet();
                extractNegatedProperties(filter.getCondition(), negatedProperties);

                if (negatedProperties.size() == 1) {
                    emit("!").emitIf(inverse, "^").emit(negatedProperties.iterator().next());

                } else {
                    emit("!(");
                    boolean first = true;
                    for (final URI negatedProperty : negatedProperties) {
                        emitIf(!first, "|").emitIf(inverse, "^").emit(negatedProperty);
                        first = false;
                    }
                    emit(")");
                }

            } else {
                fail("Unsupported path expression node", node);
            }

            return emitIf(parenthesis, ")");
        }

        private void extractNegatedProperties(final ValueExpr condition,
                final Set<URI> negatedProperties) {
            if (condition instanceof And) {
                final And and = (And) condition;
                extractNegatedProperties(and.getLeftArg(), negatedProperties);
                extractNegatedProperties(and.getRightArg(), negatedProperties);

            } else if (condition instanceof Compare) {
                final Compare compare = (Compare) condition;
                Preconditions.checkArgument(compare.getOperator() == CompareOp.NE);
                if (compare.getLeftArg() instanceof ValueConstant) {
                    Preconditions.checkArgument(compare.getRightArg() instanceof Var);
                    negatedProperties.add((URI) ((ValueConstant) compare.getLeftArg()).getValue());
                } else if (compare.getRightArg() instanceof ValueConstant) {
                    Preconditions.checkArgument(compare.getLeftArg() instanceof Var);
                    negatedProperties
                            .add((URI) ((ValueConstant) compare.getRightArg()).getValue());
                } else {
                    fail("Unsupported path expression. Check condition node: ", condition);
                }
            }
        }

        private boolean isInversePath(final StatementPattern node, final Var start, final Var end) {
            if (node.getSubjectVar().equals(start)) {
                Preconditions.checkArgument(node.getObjectVar().equals(end));
                return false;
            } else if (node.getObjectVar().equals(start)) {
                Preconditions.checkArgument(node.getSubjectVar().equals(end));
                return true;
            } else {
                fail("Unsupported path expression. Check node: ", node);
                return false;
            }
        }

        // TupleExpr: unsupported

        @Override
        public void meet(final Intersection n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        // === SPARQL UPDATE ===

        @Override
        public void meet(final Add add) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Clear clear) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Copy copy) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Create create) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final DeleteData deleteData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final InsertData insertData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Load load) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Modify modify) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Move move) {
            throw new UnsupportedOperationException();
        }

        // === VALUE EXPR ===

        // ValueExpr: variables and constants

        @Override
        public void meet(final ValueConstant n) {
            emit(n.getValue());
        }

        @Override
        public void meet(final Var n) {
            final String name = n.getName();
            if (n.getValue() != null) {
                emit(n.getValue());
            } else if (!n.isAnonymous()) {
                emit("?" + n.getName());
            } else {
                final ValueExpr expr = getVarExpr(this.root, n.getName());
                if (expr != null) {
                    emit(expr);
                } else if (getVarRefs(this.root, n.getName()) <= 1) {
                    emit("[]");
                } else {
                    emit("?").emit(sanitize(name));
                }
            }
        }

        // ValueExpr: comparison, math and boolean operators

        @Override
        public void meet(final Compare n) {
            final QueryModelNode p = n.getParentNode();
            final boolean par = p instanceof Not || p instanceof MathExpr;
            emitIf(par, "(").emit(n.getLeftArg()).emit(" ").emit(n.getOperator().getSymbol())
                    .emit(" ").emit(n.getRightArg()).emitIf(par, ")");
        }

        @Override
        public void meet(final ListMemberOperator n) {
            final QueryModelNode p = n.getParentNode();
            final boolean par = p instanceof Not || p instanceof MathExpr;
            final List<ValueExpr> args = n.getArguments();
            emitIf(par, "(").emit(args.get(0)).emit(" in (")
                    .emit(args.subList(1, args.size()), ", ").emit(")").emitIf(par, ")");
        }

        @Override
        public void meet(final MathExpr n) {
            final QueryModelNode p = n.getParentNode();
            final MathOp op = n.getOperator();
            final MathOp pop = p instanceof MathExpr ? ((MathExpr) p).getOperator() : null;
            final boolean r = p instanceof BinaryValueOperator
                    && n == ((BinaryValueOperator) p).getRightArg();
            final boolean par = p instanceof Not //
                    || (op == MathOp.PLUS || op == MathOp.MINUS)
                    && (pop == MathOp.MINUS && r //
                            || pop == MathOp.DIVIDE || pop == MathOp.MULTIPLY)
                    || (op == MathOp.MULTIPLY || op == MathOp.DIVIDE) && pop == MathOp.DIVIDE && r;
            emitIf(par, "(").emit(n.getLeftArg()).emit(" ").emit(op.getSymbol()).emit(" ")
                    .emit(n.getRightArg()).emitIf(par, ")");
        }

        @Override
        public void meet(final And n) {
            final QueryModelNode p = n.getParentNode();
            final boolean needPar = p instanceof Not || p instanceof MathExpr
                    || p instanceof ListMemberOperator || p instanceof Compare;
            emitIf(needPar, "(").emit(n.getLeftArg()).emit(" && ").emit(n.getRightArg())
                    .emitIf(needPar, ")");
        }

        @Override
        public void meet(final Or n) {
            final QueryModelNode p = n.getParentNode();
            final boolean needPar = p instanceof Not || p instanceof And || p instanceof MathExpr
                    || p instanceof ListMemberOperator || p instanceof Compare;
            emitIf(needPar, "(").emit(n.getLeftArg()).emit(" || ").emit(n.getRightArg())
                    .emitIf(needPar, ")");
        }

        @Override
        public void meet(final Not n) {
            final String op = n.getArg() instanceof Exists ? "NOT " : "!";
            emit(op).emit(n.getArg());
        }

        // ValueExpr: aggregates

        @Override
        public void meet(final Count node) {
            emit("COUNT(").emitIf(node.isDistinct(), "DISTINCT ")
                    .emit(defaultIfNull(node.getArg(), "*")).emit(")");
        }

        @Override
        public void meet(final Sum node) {
            emit("SUM(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Min node) {
            emit("MIN(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Max node) {
            emit("MAX(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Avg node) {
            emit("AVG(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Sample node) {
            emit("SAMPLE(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final GroupConcat node) {
            emit("GROUP_CONCAT(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg());
            if (node.getSeparator() != null) {
                emit(" ; separator=").emit(node.getSeparator());
            }
            emit(")");
        }

        // ValueExpr: function calls

        @Override
        public void meet(final Str n) {
            emit("STR(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Lang n) {
            emit("LANG(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final LangMatches n) {
            emit("LANGMATCHES(").emit(n.getLeftArg()).emit(", ").emit(n.getRightArg()).emit(")");
        }

        @Override
        public void meet(final Datatype n) {
            emit("DATATYPE(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Bound n) {
            emit("BOUND(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IRIFunction n) {
            emit("IRI(").emit(n.getArg()).emit(")");
            if (n.getBaseURI() != null) {
                this.base = n.getBaseURI();
            }
        }

        @Override
        public void meet(final BNodeGenerator n) {
            final ValueExpr expr = n.getNodeIdExpr();
            emit("BNODE(").emitIf(expr != null, expr).emit(")");
        }

        @Override
        public void meet(final FunctionCall n) {
            final String uri = n.getURI();
            String name = NAMES.get(uri);
            if (name == null && NAMES.values().contains(uri.toUpperCase())) {
                name = n.getURI().toUpperCase();
            }
            emit(name != null ? name : new URIImpl(uri)).emit("(").emit(n.getArgs(), ", ")
                    .emit(")");
        }

        @Override
        public void meet(final Coalesce n) {
            emit("COALESCE(").emit(n.getArguments(), ", ").emit(")");
        }

        @Override
        public void meet(final If n) {
            emit("IF(").emit(n.getCondition()).emit(", ").emit(n.getResult()).emit(", ")
                    .emit(n.getAlternative()).emit(")");
        }

        @Override
        public void meet(final SameTerm n) {
            emit("sameTerm(").emit(n.getLeftArg()).emit(", ").emit(n.getRightArg()).emit(")");
        }

        @Override
        public void meet(final IsURI n) {
            emit("isIRI(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsBNode n) {
            emit("isBLANK(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsLiteral n) {
            emit("isLITERAL(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsNumeric n) {
            emit("isNUMERIC(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Regex n) {
            emit("REGEX(").emit(n.getArg()).emit(", ").emit(n.getPatternArg());
            if (n.getFlagsArg() != null) {
                emit(", ").emit(n.getFlagsArg());
            }
            emit(")");
        }

        @Override
        public void meet(final Exists node) {
            emit("EXISTS ").emit(node.getSubQuery());
        }

        // ValueExpr: unsupported nodes

        @Override
        public void meet(final IsResource n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Label n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Like n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final LocalName n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Namespace n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final In n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final CompareAll n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final CompareAny n) {
            fail("Not a SPARQL 1.1 node", n);
        }

        // OTHER

        @Override
        public void meetOther(final QueryModelNode n) {
            fail("Unknown node", n);
        }

    }

    private enum Form {
        SELECT, CONSTRUCT, ASK, DESCRIBE
    }

    private enum Modifier {
        DISTINCT, REDUCED
    }

    private static final class Query {

        final QueryModelNode root;

        final Form form;

        @Nullable
        final Modifier modifier;

        final List<ProjectionElem> select;

        @Nullable
        final TupleExpr construct;

        @Nullable
        final Dataset from;

        final TupleExpr where;

        final List<ProjectionElem> groupBy;

        @Nullable
        final ValueExpr having;

        final List<OrderElem> orderBy;

        @Nullable
        final Long offset;

        @Nullable
        final Long limit;

        static Query create(final TupleExpr rootNode, @Nullable final Dataset dataset,
                final boolean forceSelect) {

            Preconditions.checkNotNull(rootNode);

            // Handle special trivial case
            if (rootNode instanceof EmptySet) {
                return new Query(rootNode, Form.CONSTRUCT, null, null, rootNode, dataset,
                        rootNode, null, null, null, null, null);
            }

            // Local variables
            Form form = null;
            Modifier modifier = null;
            final List<ProjectionElem> select = Lists.newArrayList();
            TupleExpr construct = null;
            TupleExpr where = null;
            final List<ProjectionElem> groupBy = Lists.newArrayList();
            ValueExpr having = null;
            final List<OrderElem> orderBy = Lists.newArrayList();
            Long offset = null;
            Long limit = null;

            final List<UnaryTupleOperator> nodes = extractQueryNodes(rootNode, false);

            where = nodes.size() > 0 ? nodes.get(nodes.size() - 1).getArg() : rootNode;

            for (final UnaryTupleOperator node : nodes) {

                if (node instanceof DescribeOperator) {
                    form = Form.DESCRIBE;

                } else if (node instanceof Distinct) {
                    modifier = Modifier.DISTINCT;

                } else if (node instanceof Reduced) {
                    modifier = Modifier.REDUCED;

                } else if (node instanceof Projection) {
                    final Map<String, ExtensionElem> extensions = extractExtensions(node);
                    final List<ProjectionElem> projections = ((Projection) node)
                            .getProjectionElemList().getElements();
                    final boolean isConstruct = projections.size() >= 3
                            && "subject".equals(projections.get(0).getTargetName())
                            && "predicate".equals(projections.get(1).getTargetName())
                            && "object".equals(projections.get(2).getTargetName())
                            && (projections.size() == 3 || projections.size() == 4
                                    && "context".equals(projections.get(3).getTargetName()));
                    if (isConstruct && !forceSelect) {
                        form = Form.CONSTRUCT;
                        construct = extractConstructExpression(extensions,
                                Collections.singleton(((Projection) node) //
                                        .getProjectionElemList()));
                    } else {
                        form = form == null ? Form.SELECT : form;
                        for (final ProjectionElem projection : projections) {
                            final String variable = projection.getTargetName();
                            ExtensionElem extension = extensions.get(variable);
                            if (extension == null && projection.getSourceName() != null) {
                                extension = extensions.get(projection.getSourceName());
                            }
                            final ProjectionElem newProjection = new ProjectionElem();
                            newProjection.setTargetName(variable);
                            newProjection.setSourceExpression(extension);
                            newProjection.setSourceName(extension == null
                                    || !(extension.getExpr() instanceof Var) ? projection
                                    .getSourceName() : ((Var) extension.getExpr()).getName());
                            select.add(newProjection);
                        }
                    }

                } else if (node instanceof MultiProjection) {
                    form = Form.CONSTRUCT;
                    construct = extractConstructExpression(extractExtensions(node),
                            ((MultiProjection) node).getProjections());

                } else if (node instanceof Group) {
                    final Group group = (Group) node;
                    final Map<String, ExtensionElem> extensions = extractExtensions(group.getArg());
                    for (final String variableName : group.getGroupBindingNames()) {
                        final ExtensionElem extension = extensions.get(variableName);
                        final ProjectionElem projection = new ProjectionElem();
                        projection.setTargetName(variableName);
                        projection.setSourceExpression(extension);
                        projection.setSourceName(extension == null
                                || !(extension.getExpr() instanceof Var) ? variableName
                                : ((Var) extension.getExpr()).getName());
                        groupBy.add(projection);
                    }

                } else if (node instanceof Order) {
                    orderBy.addAll(((Order) node).getElements());

                } else if (node instanceof Slice) {
                    final Slice slice = (Slice) node;
                    offset = slice.getOffset() < 0 ? null : slice.getOffset();
                    limit = slice.getLimit() < 0 ? null : slice.getLimit();
                    if (form == null && slice.getOffset() == 0 && slice.getLimit() == 1) {
                        if (forceSelect) {
                            form = Form.SELECT;
                            limit = 1L;
                            // limit = 2L; // TODO: workaround for Virtuoso
                        } else {
                            form = Form.ASK;
                        }
                    }

                } else if (node instanceof Filter) {
                    having = ((Filter) node).getCondition();
                }
            }

            form = defaultIfNull(form, Form.SELECT);
            if (form == Form.CONSTRUCT && construct == null) {
                construct = new SingletonSet();
            }

            return new Query(rootNode, form, modifier, select, construct, dataset, where, groupBy,
                    having, orderBy, offset, limit);
        }

        private static List<UnaryTupleOperator> extractQueryNodes(final TupleExpr rootNode,
                final boolean haltOnGroup) {
            final List<UnaryTupleOperator> nodes = Lists.newArrayList();

            TupleExpr queryNode = rootNode;
            while (queryNode instanceof UnaryTupleOperator) {
                nodes.add((UnaryTupleOperator) queryNode);
                queryNode = ((UnaryTupleOperator) queryNode).getArg();
            }

            boolean describeFound = false;
            boolean modifierFound = false;
            boolean projectionFound = false;
            boolean groupFound = false;
            boolean orderFound = false;
            boolean sliceFound = false;
            boolean extensionFound = false;

            int index = 0;
            while (index < nodes.size()) {
                final UnaryTupleOperator node = nodes.get(index);
                if (node instanceof DescribeOperator && !describeFound) {
                    describeFound = true;

                } else if ((node instanceof Distinct || node instanceof Reduced) && !modifierFound
                        && !projectionFound) {
                    modifierFound = true;

                } else if ((node instanceof Projection || node instanceof MultiProjection)
                        && !projectionFound) {
                    projectionFound = true;

                } else if (node instanceof Group && !groupFound && !haltOnGroup) {
                    groupFound = true;

                } else if (node instanceof Order && !orderFound) {
                    orderFound = true;

                } else if (node instanceof Slice && !sliceFound) {
                    sliceFound = true;

                } else if (node instanceof Filter && !groupFound && !haltOnGroup) {
                    int i = index + 1;
                    for (; i < nodes.size() && nodes.get(i) instanceof Extension;) {
                        ++i;
                    }
                    if (i < nodes.size() && nodes.get(i) instanceof Group) {
                        groupFound = true;
                        index = i;
                    } else {
                        break;
                    }

                } else if (node instanceof Extension && !extensionFound) {
                    extensionFound = true;

                } else if (!(node instanceof QueryRoot) || index > 0) {
                    break;
                }
                ++index;
            }

            return nodes.subList(0, index);
        }

        private static Map<String, ExtensionElem> extractExtensions(final TupleExpr rootNode) {
            final Map<String, ExtensionElem> map = Maps.newHashMap();
            for (final UnaryTupleOperator node : extractQueryNodes(rootNode, true)) {
                if (node instanceof Extension) {
                    for (final ExtensionElem elem : ((Extension) node).getElements()) {
                        final String variable = elem.getName();
                        final ValueExpr expression = elem.getExpr();
                        if (!(expression instanceof Var)
                                || !((Var) expression).getName().equals(variable)) {
                            map.put(variable, elem);
                        }
                    }
                }
            }
            return map;
        }

        private static TupleExpr extractConstructExpression(
                final Map<String, ExtensionElem> extensions,
                final Iterable<? extends ProjectionElemList> multiProjections) {
            TupleExpr expression = null;
            for (final ProjectionElemList projections : multiProjections) {
                final Var subj = extractConstructVar(extensions, projections.getElements().get(0));
                final Var pred = extractConstructVar(extensions, projections.getElements().get(1));
                final Var obj = extractConstructVar(extensions, projections.getElements().get(2));
                final Var ctx = projections.getElements().size() < 4 ? null : extractConstructVar(
                        extensions, projections.getElements().get(3));
                final StatementPattern pattern = new StatementPattern(
                        ctx == null ? Scope.DEFAULT_CONTEXTS : Scope.NAMED_CONTEXTS, subj, pred,
                        obj, ctx);
                expression = expression == null ? pattern : new Join(expression, pattern);
            }
            return expression;
        }

        private static Var extractConstructVar(final Map<String, ExtensionElem> extensions,
                final ProjectionElem projection) {
            final ExtensionElem extension = extensions.get(projection.getSourceName());
            String name = projection.getSourceName();
            if (name.startsWith("-anon-")) {
                name += "-construct";
            }
            if (extension == null || extension.getExpr() instanceof BNodeGenerator) {
                final Var var = new Var(name);
                var.setAnonymous(name.startsWith("-anon-"));
                return var;
            } else if (extension.getExpr() instanceof ValueConstant) {
                final ValueConstant constant = (ValueConstant) extension.getExpr();
                return new Var(name, constant.getValue());
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported extension in construct query: " + extension);
            }
        }

        private Query(//
                final QueryModelNode root, //
                final Form form, //
                @Nullable final Modifier modifier, //
                @Nullable final Iterable<? extends ProjectionElem> selectist, //
                @Nullable final TupleExpr construct, //
                @Nullable final Dataset from, //
                final TupleExpr where, //
                @Nullable final Iterable<? extends ProjectionElem> groupByt, //
                @Nullable final ValueExpr having, //
                @Nullable final Iterable<? extends OrderElem> orderBy, //
                @Nullable final Long offset, //
                @Nullable final Long limit) {

            this.root = Preconditions.checkNotNull(root);
            this.form = Preconditions.checkNotNull(form);
            this.modifier = modifier;
            this.select = selectist == null ? ImmutableList.<ProjectionElem>of() : ImmutableList
                    .copyOf(selectist);
            this.construct = construct;
            this.from = from;
            this.where = Preconditions.checkNotNull(where);
            this.groupBy = groupByt == null ? ImmutableList.<ProjectionElem>of() : ImmutableList
                    .copyOf(groupByt);
            this.having = having;
            this.orderBy = orderBy == null ? ImmutableList.<OrderElem>of() : ImmutableList
                    .copyOf(orderBy);
            this.offset = offset;
            this.limit = limit;
        }

    }

}
