package eu.fbk.knowledgestore.data;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.jaxen.Context;
import org.jaxen.ContextSupport;
import org.jaxen.JaxenException;
import org.jaxen.JaxenHandler;
import org.jaxen.NamespaceContext;
import org.jaxen.SimpleVariableContext;
import org.jaxen.VariableContext;
import org.jaxen.expr.AllNodeStep;
import org.jaxen.expr.BinaryExpr;
import org.jaxen.expr.DefaultNameStep;
import org.jaxen.expr.DefaultXPathFactory;
import org.jaxen.expr.Expr;
import org.jaxen.expr.FilterExpr;
import org.jaxen.expr.FunctionCallExpr;
import org.jaxen.expr.LiteralExpr;
import org.jaxen.expr.LocationPath;
import org.jaxen.expr.NameStep;
import org.jaxen.expr.NumberExpr;
import org.jaxen.expr.PathExpr;
import org.jaxen.expr.Step;
import org.jaxen.expr.UnaryExpr;
import org.jaxen.expr.XPathFactory;
import org.jaxen.saxpath.XPathReader;
import org.jaxen.saxpath.helpers.XPathReaderFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An XPath-like expression that computes/extracts a list of objects starting from an object of
 * the data model.
 * <p>
 * This class allows to express and evaluate generic XPath-like expressions on nodes of the data
 * model. An {@code XPath} expression can be created in three ways:
 * </p>
 * <ul>
 * <li>with methods {@link #parse(String, Object...)} and {@link #parse(Map, String, Object...)},
 * which parse an {@code XPath} string, possibly using a map of namespace declarations replacing
 * supplied values to optional <tt>$$</tt> placeholders in the string;</li>
 * <li>with method {@link #constant(Object...)} that produces the {@code XPath} expression
 * returning a supplied constant value or sequence of values;</li>
 * <li>with method {@link #compose(String, Object...)} that composes multiple {@code XPath} or
 * scalar sequence operands using a supplied operator.</li>
 * </ul>
 * Parsed expression strings are validated and then stored in the created {@code XPath} object,
 * accessible via {@link #toString()}, {@link #getHead()} and {@link #getBody()}. The syntax of
 * the expression string is reported in the package documentation. ### TODO ### </p>
 * <p>
 * Evaluation of an {@code XPath} expression is performed via a number of {@code eval} methods
 * that all accept an object as input but differ in their output and additional arguments:
 * <ul>
 * <li>{@link #eval(Object)} and {@link #eval(Object, Class)} return a list result consisting
 * either of objects of the data model or of objects obtained from their conversion to a specific
 * type {@code T};</li>
 * <li>{@link #evalUnique(Object)} and {@link #evalUnique(Object, Class)} return a unique result
 * consisting either in a generic object or an object converted to a specific type; their outcome
 * is null if the evaluation produced no result, while an {@link IllegalArgumentException} is
 * thrown if multiple results are produced;</li>
 * <li>{@link #evalBoolean(Object)} return a boolean result.</li>
 * </ul>
 * <p>
 * Note that the evaluation may fail for a specific input object for a number of reasons, e.g.,
 * because it performs some arithmetic operation on data that is of an incompatible type. Failure
 * is reported by throwing an {@code IllegalArgumentException} if the {@code XPath} expression is
 * not lenient (see {@link #isLenient()}), or by returning a default value otherwise (respectively
 * an empty list, null or false for the three classes of {@code eval()} methods). An {@code XPath}
 * expression is created not-lenient by default, but a lenient version of it can be obtained by
 * calling method {@link #lenient(boolean)}.
 * </p>
 * <p>
 * Apart from evaluation, it is possible to query an {@code XPath} object for the set of
 * properties accessed by the expression, using method {@link #getProperties()}, and for the
 * prefix-to-namespace mappings referenced by the expression string. Methods
 * {@link #asPredicate()}, {@link #asFunction(Class)} and {@link #asFunctionUnique(Class)}
 * generate respectively a {@link Predicate}, single-valued and multi-valued {@link Function} view
 * of an {@code XPath} expression, thus supporting interoperability with the utility classes and
 * methods of the Guava library. Another method {@link #decompose(Map)} attempts to decompose a
 * boolean {@code XPath} expression into a conjunction of property restrictions of the form
 * {@code property in rangeset} plus an optional remaining {@code XPath} expression, where
 * {@code rangesets} are scalar sets of {@link Range}s s (e.g., (-1, 5], (6, 9)). This kind of
 * decomposition allows to extract simple restrictions on individual properties that can be
 * efficiently evaluated using indexes.
 * </p>
 * <p>
 * {@code XPath} objects are immutable and thread safe. Methods {@link #equals(Object)} and
 * {@link #hashCode()} are based exclusively on the expression string (accessible via
 * {@link #toString()}) and on the lenient mode of the {@code XPath} object.
 * </p>
 */
@SuppressWarnings("deprecation")
public abstract class XPath implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(XPath.class);

    private static final Cache<String, XPath> CACHE = CacheBuilder.newBuilder().maximumSize(1024)
            .build();

    private static final Pattern PATTERN_PLACEHOLDER = Pattern
            .compile("(?:\\A|[^\\\\])([$][$])(?:\\z|.)");

    private static final Pattern PATTERN_WITH = Pattern.compile("\\s*with\\s+");

    private static final Pattern PATTERN_MAPPING = Pattern
            .compile("\\s*(\\w+)\\:\\s*\\<([^\\>]+)\\>\\s*([\\:\\,])\\s*");

    private static final VariableContext VARIABLES = new SimpleVariableContext();

    private static final XPathFactory FACTORY = new DefaultXPathFactory();

    private final transient Support support;

    /**
     * Creates an {@code XPath} expression returning a sequence with the constant value(s)
     * specified. Values must be scalars, arrays or iterables; the latter two are recursively
     * exploded and their elements added to the sequence produced by the returned {@code XPath}
     * expression.
     *
     * @param values
     *            the values
     * @return the produced {@code XPath} expression
     */
    public static XPath constant(final Object... values) {
        return parse(encode(values, true));
    }

    /**
     * Creates an {@code XPath} expression by composing a number operands with the operator
     * specified. Operands can be either {@code XPath} expressions, scalars, arrays or iterables
     * of scalars. Supported operators are {@code not}, {@code and}, {@code or}, {@code =},
     * {@code !=}, {@code <}, {@code >}, {@code <=}, {@code >=}, {@code +}, {@code -}, {@code *},
     * {@code mod}, {@code div}, {@code |}.
     *
     * @param operator
     *            the operator
     * @param operands
     *            the operands
     * @return the produced {@code XPath} expression, or null if no operand was supplied
     * @throws IllegalArgumentException
     *             in case multiple {@code XPath} expressions using incompatible namespaces are
     *             composed.
     */
    @Nullable
    public static XPath compose(final String operator, final Object... operands)
            throws IllegalArgumentException {

        final String op = operator.toLowerCase();

        try {
            if (operands.length == 0) {
                return null;
            }

            if (operands.length == 1) {
                final XPath xpath = operands[0] instanceof XPath ? (XPath) operands[0]
                        : constant(operands[0]);
                if ("not".equals(op)) {
                    final Expr expr = FACTORY.createFunctionCallExpr(null, "not");
                    ((FunctionCallExpr) expr).addParameter(xpath.support.expr);
                    return new StrictXPath(new Support(expr,
                            expr.getText().replace("child::", ""), xpath.support.properties,
                            xpath.support.namespaces));
                } else if ("=".equals(op) || "!=".equals(op) || "<".equals(op) || ">".equals(op)
                        || "<=".equals(op) || ">=".equals(op)) {
                    throw new IllegalArgumentException(
                            "At least two arguments required for operator " + op);
                }
                return xpath;
            }

            final List<Expr> expressions = Lists.newArrayListWithCapacity(operands.length);
            final Set<URI> properties = Sets.newHashSet();
            final Map<String, String> namespaces = Maps.newHashMap();

            for (final Object operand : operands) {
                final XPath xpath = operand instanceof XPath ? (XPath) operand : constant(operand);
                expressions.add(xpath.support.expr);
                properties.addAll(xpath.support.properties);
                for (final Map.Entry<String, String> entry : xpath.support.namespaces.entrySet()) {
                    final String oldNamespace = namespaces.put(entry.getKey(), entry.getValue());
                    Preconditions.checkArgument(
                            oldNamespace == null || oldNamespace.equals(entry.getValue()),
                            "Namespace conflict for prefix '" + entry.getKey() + "': <"
                                    + entry.getValue() + "> vs <" + oldNamespace + ">");
                }
            }

            Expr lhs = expressions.get(0);
            for (int i = 1; i < expressions.size(); ++i) {
                final Expr rhs = expressions.get(i);
                if ("and".equals(op)) {
                    lhs = FACTORY.createAndExpr(lhs, rhs);
                } else if ("or".equals(op)) {
                    lhs = FACTORY.createOrExpr(lhs, rhs);
                } else if ("=".equals(op)) {
                    lhs = FACTORY.createEqualityExpr(lhs, rhs, 1); // 1 stands for =
                } else if ("!=".equals(op)) {
                    lhs = FACTORY.createEqualityExpr(lhs, rhs, 2); // 2 stands for !=
                } else if ("<".equals(op)) {
                    lhs = FACTORY.createRelationalExpr(lhs, rhs, 3); // 3 stands for <
                } else if (">".equals(op)) {
                    lhs = FACTORY.createRelationalExpr(lhs, rhs, 4); // 4 stands for >
                } else if ("<=".equals(op)) {
                    lhs = FACTORY.createRelationalExpr(lhs, rhs, 5); // 5 stands for <=
                } else if (">=".equals(op)) {
                    lhs = FACTORY.createRelationalExpr(lhs, rhs, 6); // 6 stands for >=
                } else if ("+".equals(op)) {
                    lhs = FACTORY.createAdditiveExpr(lhs, rhs, 7); // 7 stands for +
                } else if ("-".equals(op)) {
                    lhs = FACTORY.createAdditiveExpr(lhs, rhs, 8); // 8 stands for -
                } else if ("*".equals(op)) {
                    lhs = FACTORY.createMultiplicativeExpr(lhs, rhs, 9); // 9 stands for *
                } else if ("mod".equals(op)) {
                    lhs = FACTORY.createMultiplicativeExpr(lhs, rhs, 10); // 10 = mod
                } else if ("div".equals(op)) {
                    lhs = FACTORY.createMultiplicativeExpr(lhs, rhs, 11); // 11 = div
                } else if ("|".equals(op)) {
                    lhs = FACTORY.createUnionExpr(lhs, rhs);
                } else {
                    throw new IllegalArgumentException("Unsupported operator " + op);
                }
            }

            return new StrictXPath(new Support(lhs, lhs.getText().replace("child::", ""),
                    properties, namespaces));

        } catch (final JaxenException ex) {
            throw new IllegalArgumentException("Could not compose operands " + operands
                    + " of operator " + op + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * Creates a new {@code XPath} expression based on the expression string specified with
     * optional <tt>$$</tt> placeholders replaced by supplied values. Note that i-th placeholder
     * is replaced with i-th value of the {@code values} vararg array, while namespaces referenced
     * in the string must be defined in the {@code WITH} clause of the string itself.
     *
     * @param string
     *            the expression string
     * @param values
     *            the values for optional placeholders appearing in {@code string}
     * @return the created {@code XPath} expression object, on success
     * @throws ParseException
     *             if the expression string supplied is not syntactically correct, or if it
     *             references a namespace not defined in the string itself or in common
     *             (prefix.cc) namespaces
     */
    public static XPath parse(final String string, final Object... values) throws ParseException {
        return parse(Data.getNamespaceMap(), string, values);
    }

    /**
     * Creates a new {@code XPath} expression based on the namespace mappings and the expression
     * string specified, with optional <tt>$$</tt> placeholders replaced by supplied values. Note
     * that i-th placeholder is replaced with i-th value of the {@code values} vararg array, while
     * namespaces occurring in the string can be defined either in the {@code WITH} clause of the
     * string or in the supplied namespace {@code Map}.
     *
     * @param namespaces
     *            the namespace map
     * @param expression
     *            the expression string
     * @param values
     *            the values for optional placeholders appearing in {@code string}
     * @return the created {@code XPath} expression object, on success
     * @throws ParseException
     *             if the expression string supplied is not syntactically correct, or if it
     *             references a namespace not defined in the string itself or in the supplied
     *             namespace map
     */
    public static XPath parse(final Map<String, String> namespaces, final String expression,
            final Object... values) throws ParseException {

        Preconditions.checkNotNull(namespaces);
        Preconditions.checkNotNull(expression);

        final Map<String, String> baseNamespaces = Data.newNamespaceMap(namespaces,
                Data.getNamespaceMap());

        final String expandedString = expand(expression, values);

        XPath xpath = CACHE.getIfPresent(expandedString);
        if (xpath != null) {
            for (final Map.Entry<String, String> entry : xpath.getNamespaces().entrySet()) {
                if (!entry.getValue().equals(baseNamespaces.get(entry.getKey()))) {
                    xpath = null;
                    break;
                }
            }
            if (xpath != null) {
                return xpath;
            }
        }

        final Map<String, String> usedNamespaces = Maps.newHashMap();
        final Map<String, String> declaredNamespaces = Maps.newHashMap();
        final Map<String, String> combinedNamespaces = Data.newNamespaceMap(declaredNamespaces,
                baseNamespaces);

        final String xpathString;

        Expr expr;
        final Set<URI> properties = Sets.newHashSet();
        try {
            xpathString = extractNamespaces(expandedString, declaredNamespaces);
            String rewrittenXpathString = rewriteLiterals(xpathString, combinedNamespaces,
                    usedNamespaces);
            rewrittenXpathString = rewriteEscapedURIs(rewrittenXpathString, combinedNamespaces,
                    usedNamespaces);
            LOGGER.debug("XPath '{}' rewritten to '{}'", expression, rewrittenXpathString);
            final JaxenHandler handler = new JaxenHandler();
            final XPathReader reader = XPathReaderFactory.createReader();
            reader.setXPathHandler(handler);
            reader.parse(rewrittenXpathString);
            expr = handler.getXPathExpr().getRootExpr();
            assert expr != null;
            analyzeExpr(expr, combinedNamespaces, usedNamespaces, properties, true);
        } catch (final Exception ex) {
            throw new ParseException(expression, "Invalid XPath expression - " + ex.getMessage(),
                    ex);
        }

        xpath = new StrictXPath(new Support(expr, xpathString, properties, usedNamespaces));
        CACHE.put(expression, xpath);
        return xpath;
    }

    private static String expand(final String expression, final Object... arguments)
            throws ParseException {
        int expansions = 0;
        String result = expression;
        final Matcher matcher = PATTERN_PLACEHOLDER.matcher(expression);
        try {
            if (matcher.find()) {
                final StringBuilder builder = new StringBuilder();
                int last = 0;
                do {
                    builder.append(expression.substring(last, matcher.start(1)));
                    builder.append(encode(arguments[expansions++], true));
                    last = matcher.end(1);
                } while (matcher.find());
                builder.append(expression.substring(last, expression.length()));
                result = builder.toString();
            }
        } catch (final IndexOutOfBoundsException ex) {
            throw new ParseException(expression, "No argument supplied for placeholder #"
                    + expansions);
        }
        if (expansions != arguments.length) {
            throw new ParseException(expression, "XPath expression contains " + expansions
                    + " placholders, but " + arguments.length + " arguments where supplied");
        }
        return result;
    }

    private static String extractNamespaces(final String expression,
            final Map<String, String> namespaces) throws IllegalArgumentException {
        String xpathString = expression;
        final Matcher matcher = PATTERN_WITH.matcher(expression);
        if (matcher.lookingAt()) {
            matcher.usePattern(PATTERN_MAPPING);
            while (true) {
                matcher.region(matcher.end(), expression.length());
                if (!matcher.lookingAt()) {
                    throw new IllegalArgumentException("Invalid WITH clause");
                }
                final String prefix = matcher.group(1);
                final String uri = matcher.group(2);
                namespaces.put(prefix, uri);
                if (matcher.group(3).equals(":")) {
                    break;
                }
            }
            xpathString = expression.substring(matcher.end());
        }
        return xpathString;
    }

    private static String rewriteEscapedURIs(final String string,
            final Map<String, String> inNamespaces, final Map<String, String> outNamespaces) {

        final StringBuilder builder = new StringBuilder();
        final int length = string.length();
        int i = 0;

        try {
            while (i < length) {
                char c = string.charAt(i);
                if (c == '\\') {
                    c = string.charAt(++i);
                    if (c == '\'' || c == '\"' || c == '<') {
                        final char d = c == '<' ? '>' : c; // delimiter;
                        final int start = i + 1;
                        do {
                            c = string.charAt(++i);
                        } while (c != d || string.charAt(i - 1) == '\\');
                        builder.append("uri(\"" + string.substring(start, i) + "\")");
                        ++i;
                    } else {
                        final int start = i;
                        while (i < length && (string.charAt(i) == ':' //
                                || string.charAt(i) == '_' //
                                || string.charAt(i) == '-' //
                                || string.charAt(i) == '.' //
                        || Character.isLetterOrDigit(string.charAt(i)))) {
                            ++i;
                        }
                        final String qname = string.substring(start, i);
                        final String prefix = qname.substring(0, qname.indexOf(':'));
                        final URI uri = (URI) Data.parseValue(qname, inNamespaces);
                        outNamespaces.put(prefix, uri.getNamespace());
                        builder.append("uri(\"" + uri + "\")");
                    }
                } else if (c == '\'' || c == '\"') {
                    final char d = c; // delimiter
                    builder.append(d);
                    do {
                        c = string.charAt(++i);
                        builder.append(c);
                    } while (c != d || string.charAt(i - 1) == '\\');
                    ++i;

                } else {
                    builder.append(c);
                    ++i;
                }
            }
        } catch (final Exception ex) {
            throw new IllegalArgumentException("Illegal URI escaping near offset " + i, ex);
        }

        return builder.toString();
    }

    private static String rewriteLiterals(final String string,
            final Map<String, String> inNamespaces, final Map<String, String> outNamespaces) {

        final StringBuilder builder = new StringBuilder();
        final int length = string.length();
        int i = 0;

        try {
            while (i < length) {
                char c = string.charAt(i);
                if (c == '\'' || c == '\"') {
                    final char d = c; // delimiter
                    if (i > 0 && string.charAt(i - 1) == '\\' //
                            || i >= 4 && string.startsWith("uri(", i - 4) //
                            || i >= 4 && string.startsWith("str(", i - 4) //
                            || i >= 6 && string.startsWith("strdt(", i - 6) //
                            || i >= 8 && string.startsWith("strlang(", i - 8)) {
                        do {
                            builder.append(c);
                            c = string.charAt(++i);
                        } while (c != d || string.charAt(i - 1) == '\\');
                        builder.append(c);
                        ++i;
                    } else {
                        int start = i + 1;
                        do {
                            c = string.charAt(++i);
                        } while (c != d || string.charAt(i - 1) == '\\');
                        final String label = string.substring(start, i);
                        String lang = null;
                        String dt = null;
                        ++i;
                        if (i < length) {
                            if (string.charAt(i) == '@') {
                                start = ++i;
                                do {
                                    c = string.charAt(i++);
                                } while (i < length && Character.isLetter(c));
                                lang = string.substring(start, i);
                            } else if (string.charAt(i) == '^' && i + 1 < length
                                    && string.charAt(i + 1) == '^') {
                                i += 2;
                                if (string.charAt(i) == '<') {
                                    start = i + 1;
                                    do {
                                        c = string.charAt(++i);
                                    } while (c != '>');
                                    dt = string.substring(start, i);
                                } else {
                                    start = i;
                                    while (i < length && (string.charAt(i) == ':' //
                                            || string.charAt(i) == '_' //
                                            || string.charAt(i) == '-' //
                                            || string.charAt(i) == '.' //
                                    || Character.isLetterOrDigit(string.charAt(i)))) {
                                        ++i;
                                    }
                                    final String qname = string.substring(start, i);
                                    final String prefix = qname.substring(0, qname.indexOf(':'));
                                    final URI uri = (URI) Data.parseValue(qname, inNamespaces);
                                    outNamespaces.put(prefix, uri.getNamespace());
                                    dt = uri.stringValue();
                                }
                            }
                        }
                        if (lang != null) {
                            builder.append("strlang(\"").append(label).append("\", \"")
                                    .append(lang).append("\")");
                        } else if (dt != null) {
                            builder.append("strdt(\"").append(label).append("\", uri(\"")
                                    .append(dt).append("\"))");
                        } else {
                            builder.append("str(\"").append(label).append("\")");
                        }
                    }
                } else if (c == 't'
                        && string.startsWith("true", i)
                        && (i == 0 || !Character.isLetterOrDigit(string.charAt(i - 1)))
                        && (i + 4 == length || !Character.isLetterOrDigit(string.charAt(i + 4))
                                && string.charAt(i + 4) != '(')) {
                    builder.append("strdt(\"true\", uri(\"")
                            .append(XMLSchema.BOOLEAN.stringValue()).append("\"))");
                    i += 4;

                } else if (c == 'f'
                        && string.startsWith("false", i)
                        && (i == 0 || !Character.isLetterOrDigit(string.charAt(i - 1)))
                        && (i + 5 == length || !Character.isLetterOrDigit(string.charAt(i + 5))
                                && string.charAt(i + 5) != '(')) {
                    builder.append("strdt(\"false\", uri(\"")
                            .append(XMLSchema.BOOLEAN.stringValue()).append("\"))");
                    i += 5;

                } else {
                    builder.append(c);
                    ++i;
                }
            }
        } catch (final Exception ex) {
            throw new IllegalArgumentException("Illegal URI escaping near offset " + i, ex);
        }

        return builder.toString();
    }

    private static void analyzeExpr(final Expr expr, final Map<String, String> inNamespaces,
            final Map<String, String> outNamespaces, final Set<URI> outProperties,
            final boolean root) {

        if (expr instanceof UnaryExpr) {
            analyzeExpr(((UnaryExpr) expr).getExpr(), inNamespaces, outNamespaces, outProperties,
                    root);

        } else if (expr instanceof BinaryExpr) {
            final BinaryExpr binary = (BinaryExpr) expr;
            analyzeExpr(binary.getLHS(), inNamespaces, outNamespaces, outProperties, root);
            analyzeExpr(binary.getRHS(), inNamespaces, outNamespaces, outProperties, root);

        } else if (expr instanceof FilterExpr) {
            final FilterExpr filter = (FilterExpr) expr;
            analyzeExpr(filter.getExpr(), inNamespaces, outNamespaces, outProperties, root);
            for (final Object predicate : filter.getPredicates()) {
                analyzeExpr(((org.jaxen.expr.Predicate) predicate).getExpr(), inNamespaces,
                        outNamespaces, outProperties, evalToRoot(filter.getExpr(), root));
            }

        } else if (expr instanceof FunctionCallExpr) {
            for (final Object parameter : ((FunctionCallExpr) expr).getParameters()) {
                analyzeExpr((Expr) parameter, inNamespaces, outNamespaces, outProperties, root);
            }

        } else if (expr instanceof PathExpr) {
            final PathExpr path = (PathExpr) expr;
            if (path.getFilterExpr() != null) {
                analyzeExpr(path.getFilterExpr(), inNamespaces, outNamespaces, outProperties, true);
            }
            if (path.getLocationPath() != null) {
                final Expr filter = path.getFilterExpr();
                analyzeExpr(path.getLocationPath(), inNamespaces, outNamespaces, outProperties,
                        evalToRoot(filter, root));
            }

        } else if (expr instanceof LocationPath) {
            final LocationPath l = (LocationPath) expr;
            @SuppressWarnings("unchecked")
            final List<Step> steps = l.getSteps();
            for (int i = 0; i < l.getSteps().size(); ++i) {
                if (steps.get(i) instanceof DefaultNameStep) {
                    final DefaultNameStep step = (DefaultNameStep) steps.get(i);
                    final String prefix = step.getPrefix();
                    final String namespace = inNamespaces.get(prefix);
                    if (namespace == null) {
                        throw new IllegalArgumentException("Unknown prefix '" + step.getPrefix()
                                + ":'");
                    }
                    outNamespaces.put(prefix, namespace);
                    if ((i == 0 || steps.get(i - 1) instanceof AllNodeStep)
                            && (l.isAbsolute() || root)) {
                        final URI uri = Data.getValueFactory().createURI(namespace,
                                step.getLocalName());
                        outProperties.add(uri);
                    }
                }
                for (final Object predicate : steps.get(i).getPredicates()) {
                    analyzeExpr(((org.jaxen.expr.Predicate) predicate).getExpr(), inNamespaces,
                            outNamespaces, outProperties, false);
                }
            }
        }
    }

    private static boolean evalToRoot(final Expr expr, final boolean root) {

        if (expr instanceof LocationPath) {
            final LocationPath l = (LocationPath) expr;
            return (root || l.isAbsolute()) && l.getSteps().size() == 1
                    && l.getSteps().get(0) instanceof AllNodeStep;

        } else if (expr instanceof FilterExpr) {
            return evalToRoot(((FilterExpr) expr).getExpr(), root);

        } else if (expr instanceof PathExpr) {
            final PathExpr p = (PathExpr) expr;
            final boolean atRoot = evalToRoot(p.getFilterExpr(), root);
            return evalToRoot(p.getLocationPath(), atRoot);

        } else {
            return false;
        }
    }

    private static String encode(@Nullable final Object object, final boolean canEmitSequence) {

        if (object == null) {
            return "sequence()";

        } else if (object.equals(Boolean.TRUE)) {
            return "true()";

        } else if (object.equals(Boolean.FALSE)) {
            return "false()";

        } else if (object instanceof Number) {
            return object.toString();

        } else if (object instanceof Date || object instanceof GregorianCalendar
                || object instanceof XMLGregorianCalendar) {
            return "dateTime(\'" + Data.convert(object, XMLGregorianCalendar.class) + "\')";

        } else if (object instanceof URI) {
            return "\\'" + object.toString().replace("\'", "\\\'") + "\'";

        } else if (object.getClass().isArray()) {
            final int size = Array.getLength(object);
            if (size == 0) {
                return "sequence()";
            } else if (size == 1) {
                return encode(Array.get(object, 0), canEmitSequence);
            } else {
                final StringBuilder builder = new StringBuilder(canEmitSequence ? "sequence(" : "");
                for (int i = 0; i < size; ++i) {
                    builder.append(i == 0 ? "" : ", ").append(encode(Array.get(object, i), false));
                }
                builder.append(canEmitSequence ? ")" : "");
                return builder.toString();
            }

        } else if (object instanceof Iterable<?>) {
            final Iterable<?> iterable = (Iterable<?>) object;
            final int size = Iterables.size(iterable);
            if (size == 0) {
                return "sequence()";
            } else if (size == 1) {
                return encode(Iterables.get(iterable, 0), canEmitSequence);
            } else {
                final StringBuilder builder = new StringBuilder(canEmitSequence ? "sequence(" : "");
                String separator = "";
                for (final Object element : iterable) {
                    builder.append(separator).append(encode(element, false));
                    separator = ", ";
                }
                builder.append(canEmitSequence ? ")" : "");
                return builder.toString();
            }

        } else {
            return '\'' + object.toString().replace("\'", "\\\'") + '\'';
        }
    }

    private XPath(final Support support) {

        this.support = support;
    }

    /**
     * Returns the head of the {@code XPath} expression, i.e., the content of the {@code WITH}
     * clause. An empty string is returned in case the with clause is not used.
     *
     * @return the head
     */
    public final String getHead() {
        return this.support.head;
    }

    /**
     * Returns the body of the {@code XPath} expression, i.e., the {@code XPath} string without
     * the {@code WITH} clause.
     *
     * @return the body
     */
    public final String getBody() {
        return this.support.body;
    }

    /**
     * Returns the namespace declarations referenced by this {@code XPath} expression.
     *
     * @return an immutable bidirectional map of {@code prefix - namespace URI} mappings
     */
    public final Map<String, String> getNamespaces() {
        return this.support.namespaces;
    }

    /**
     * Returns the properties referenced by this {@code XPath} expression. Note that this method
     * returns only the properties accessed on the <i>root</i> record the condition is evaluated
     * on, ignoring properties of nested records that can be reached via property traversal
     * starting from the root record.
     *
     * @return an immutable set with the referenced properties, possibly empty
     */
    public final Set<URI> getProperties() {
        return this.support.properties;
    }

    /**
     * Returns true if this {@code XPath} expression is lenient, i.e., evaluation never throws
     * exceptions. {@code XPath} expressions are non-lenient by default; a lenient version of an
     * expression can be obtained by calling method {@link #lenient(boolean)}.
     *
     * @return true if this expression is lenient
     */
    public abstract boolean isLenient();

    /**
     * Returns a lenient / not lenient version of this {@code XPath} expression.
     *
     * @param lenient
     *            the requested lenient mode
     * @return an {@code XPath} expression with the same xpath string of this expression but the
     *         requested lenient mode; note that this {@code XPath} instance is returned in case
     *         the requested lenient mode matches the mode of this expression
     */
    public final XPath lenient(final boolean lenient) {
        if (lenient == isLenient()) {
            return this;
        } else if (!lenient) {
            return new StrictXPath(this.support);
        } else {
            return new LenientXPath(this.support);
        }
    }

    /**
     * Returns a predicate view of this {@code XPath} expression that accept an object input. If
     * this {@code XPath} is lenient, evaluation of the predicate will return false on failure,
     * rather than throwing an {@link IllegalArgumentException}.
     *
     * @return the requested predicate view
     */
    public final Predicate<Object> asPredicate() {

        return new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable final Object object) {
                Preconditions.checkNotNull(object);
                return evalBoolean(object);
            }

        };
    }

    /**
     * Returns a function view of this {@code XPath} expression that produces a {@code List<T>}
     * result given an input object. If this {@code XPath} is lenient, evaluation of the function
     * will return an empty list on failure, rather than throwing an
     * {@link IllegalArgumentException}.
     *
     * @param resultClass
     *            the {@code Class} object for the list elements of the expected function result
     * @param <T>
     *            the type of result list element
     * @return the requested function view
     */
    public final <T> Function<Object, List<T>> asFunction(final Class<T> resultClass) {

        Preconditions.checkNotNull(resultClass);

        return new Function<Object, List<T>>() {

            @Override
            public List<T> apply(@Nullable final Object object) {
                Preconditions.checkNotNull(object);
                return eval(object, resultClass);
            }

        };
    }

    /**
     * Returns a function view of this {@code XPath} expression that produces a unique {@code T}
     * result given an input object. If this {@code XPath} is lenient, evaluation of the function
     * will return null on failure, rather than throwing an {@link IllegalArgumentException}.
     *
     * @param resultClass
     *            the {@code Class} object for the expected function result
     * @param <T>
     *            the type of result
     * @return the requested function view
     */
    public final <T> Function<Object, T> asFunctionUnique(final Class<T> resultClass) {

        Preconditions.checkNotNull(resultClass);

        return new Function<Object, T>() {

            @Override
            public T apply(@Nullable final Object object) {
                Preconditions.checkNotNull(object);
                return evalUnique(object, resultClass);
            }

        };
    }

    /**
     * Evaluates this {@code XPath} expression on the object supplied, producing as result a list
     * of objects.
     *
     * @param object
     *            the object to evaluate this expression on
     * @return the list of objects produced as result, on success; an empty list on failure if on
     *         lenient mode
     * @throws IllegalArgumentException
     *             if this {@code XPath} expression is not lenient and evaluation fails for the
     *             object supplied
     */
    public final List<Object> eval(final Object object) throws IllegalArgumentException {
        return eval(object, Object.class);
    }

    /**
     * Evaluates this {@code XPath} expression on the object supplied, producing as result a list
     * of objects of the type {@code T} specified.
     *
     * @param object
     *            the object to evaluate this expression on
     * @param resultClass
     *            the {@code Class} object for the elements of the result list
     * @param <T>
     *            the type of element of the result list
     * @return the list of objects of the requested type produced by the evaluation, on success;
     *         an empty list on failure if on lenient mode
     * @throws IllegalArgumentException
     *             if this {@code XPath} expression is not lenient and evaluation fails for the
     *             object supplied
     */
    public final <T> List<T> eval(final Object object, final Class<T> resultClass)
            throws IllegalArgumentException {

        Preconditions.checkNotNull(object);
        Preconditions.checkNotNull(resultClass);

        try {
            return toList(doEval(object), resultClass);

        } catch (final Exception ex) {
            if (isLenient()) {
                return ImmutableList.of();
            }
            throw new IllegalArgumentException("Evaluation of XPath failed: " + ex.getMessage()
                    + "\nXPath is: " + this.support.string + "\nInput is: " + object
                    + "\nExpected result is: List<" + resultClass.getSimpleName() + ">", ex);
        }
    }

    /**
     * Evaluates this {@code XPath} expression on the object supplied, producing as result a
     * unique object.
     *
     * @param object
     *            the object to evaluate this expression on
     * @return on success, the unique object resulting from the evaluation, or null if evaluation
     *         produced no results; on failure, null is returned if this {@code XPath} expression
     *         is lenient
     * @throws IllegalArgumentException
     *             if this {@code XPath} expression is not lenient and evaluation fails for the
     *             object supplied
     */
    @Nullable
    public final Object evalUnique(final Object object) throws IllegalArgumentException {
        return evalUnique(object, Object.class);
    }

    /**
     * Evaluates this {@code XPath} expression on the object supplied, producing as result a
     * unique object of the type {@code T} specified.
     *
     * @param object
     *            the object to evaluate this expression on
     * @param resultClass
     *            the {@code Class} object for the result object
     * @param <T>
     *            the type of result
     * @return on success, the unique object of the requested type resulting from the evaluation,
     *         or null if evaluation produced no results; on failure, null is returned if this
     *         {@code XPath} expression is lenient
     * @throws IllegalArgumentException
     *             if this {@code XPath} expression is not lenient and evaluation fails for the
     *             object supplied
     */
    @Nullable
    public final <T> T evalUnique(final Object object, final Class<T> resultClass)
            throws IllegalArgumentException {

        Preconditions.checkNotNull(object);
        Preconditions.checkNotNull(resultClass);

        try {
            return toUnique(doEval(object), resultClass);

        } catch (final Exception ex) {
            if (isLenient()) {
                return null;
            }
            throw new IllegalArgumentException("Evaluation of XPath failed: " + ex.getMessage()
                    + "\nXPath is: " + this.support.string + "\nInput is: " + object
                    + "\nExpected result is: " + resultClass.getSimpleName(), ex);
        }
    }

    /**
     * Evaluates this {@code XPath} expression on the object supplied, producing a boolean value
     * as result.
     *
     * @param object
     *            the object to evaluate this expression on
     * @return the boolean value resulting from the evaluation, on success; on failure, false is
     *         returned if this {@code XPath} expression is lenient
     * @throws IllegalArgumentException
     *             if this {@code XPath} expression is not lenient and evaluation fails for the
     *             object supplied
     */
    public final boolean evalBoolean(final Object object) throws IllegalArgumentException {
        final Boolean result = evalUnique(object, Boolean.class);
        return result == null ? false : result;
    }

    private Object doEval(final Object object) {
        try {
            final Context context = new Context(this.support);
            context.setNodeSet(ImmutableList.of(XPathNavigator.INSTANCE.wrap(object)));
            return this.support.expr.evaluate(context);

        } catch (final JaxenException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }

    private static <T> List<T> toList(final Object object, final Class<T> resultClass) {

        if (object == null) {
            return ImmutableList.of();

        } else if (object instanceof List<?>) {
            final List<?> list = (List<?>) object;
            final int size = list.size();
            if (size == 0) {
                return ImmutableList.of();
            } else if (size == 1) {
                return ImmutableList.of(toUnique(list.get(0), resultClass));
            } else {
                final List<T> result = Lists.newArrayListWithCapacity(list.size());
                for (final Object element : list) {
                    result.add(toUnique(element, resultClass));
                }
                return result;
            }

        } else {
            return ImmutableList.of(toUnique(object, resultClass));
        }
    }

    @Nullable
    private static <T> T toUnique(final Object object, final Class<T> resultClass) {

        if (object == null) {
            return null;

        } else if (object instanceof List<?>) {
            final List<?> list = (List<?>) object;
            final int size = list.size();
            if (size == 0) {
                return null;
            } else if (size == 1) {
                return toUnique(list.get(0), resultClass);
            } else {
                throw new IllegalArgumentException("Expected unique "
                        + resultClass.getSimpleName() + " object, found: " + list);
            }
        } else {
            return Data.convert(XPathNavigator.INSTANCE.unwrap(object), resultClass);
        }
    }

    /**
     * Attempts at decomposing the {@code XPath} expression into the conjunction of a number of
     * property restrictions and (optionally) a remaining {@code XPath} expression. More in
     * details, the method tries to transform the condition in the following form:
     * {@code p1 in restriction1 AND ... AND pN in restrictionN AND remainingXPath}, where
     * {@code p1 ... pN} are property URIs, {@code restriction1 ... restrictionN} are sets of
     * scalar values or scalar {@link Range}s (e.g., {@code (1, 5], [7, 9]})), whose union should
     * be taken, and {@code remainingXPath} contains all the clauses of the original {@code XPath}
     * that cannot be decomposed in property restrictions.
     *
     * @param restrictions
     *            a modifiable map where to store the {@code property in restriction} restrictions
     * @return the remaining {@code XPath} expression, if any, or null if it was possible to
     *         completely express this expression as a conjunction of property restrictions
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Nullable
    public final XPath decompose(final Map<URI, Set<Object>> restrictions) {

        Preconditions.checkNotNull(restrictions);

        try {
            Expr remaining = null;

            for (final Expr node : toCNF(this.support.expr)) {
                URI property = null;
                Object valueOrRange = null;

                if (node instanceof LocationPath) {
                    property = extractProperty(node);
                    valueOrRange = Boolean.TRUE;

                } else if (node instanceof FunctionCallExpr) {
                    final FunctionCallExpr call = (FunctionCallExpr) node;
                    if ("not".equals(call.getFunctionName())) {
                        property = extractProperty((Expr) call.getParameters().get(0));
                        valueOrRange = Boolean.FALSE;
                    }

                } else if (node instanceof BinaryExpr) {
                    final BinaryExpr binary = (BinaryExpr) node;

                    property = extractProperty(binary.getLHS());
                    Object value = extractValue(binary.getRHS());
                    boolean swap = false;

                    if (property == null || value == null) {
                        property = extractProperty(binary.getRHS());
                        value = extractValue(binary.getLHS());
                        swap = true;
                    }

                    if (value instanceof Literal) {
                        final Literal lit = (Literal) value;
                        final URI dt = lit.getDatatype();
                        if (dt == null || dt.equals(XMLSchema.STRING)) {
                            value = lit.stringValue();
                        } else if (dt.equals(XMLSchema.BOOLEAN)) {
                            value = lit.booleanValue();
                        } else if (dt.equals(XMLSchema.DATE) || dt.equals(XMLSchema.DATETIME)) {
                            value = lit.calendarValue().toGregorianCalendar().getTime();
                        } else if (dt.equals(XMLSchema.INT) || dt.equals(XMLSchema.LONG)
                                || dt.equals(XMLSchema.DOUBLE) || dt.equals(XMLSchema.FLOAT)
                                || dt.equals(XMLSchema.SHORT) || dt.equals(XMLSchema.BYTE)
                                || dt.equals(XMLSchema.DECIMAL) || dt.equals(XMLSchema.INTEGER)
                                || dt.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                                || dt.equals(XMLSchema.NON_POSITIVE_INTEGER)
                                || dt.equals(XMLSchema.NEGATIVE_INTEGER)
                                || dt.equals(XMLSchema.POSITIVE_INTEGER)) {
                            value = lit.doubleValue();
                        } else if (dt.equals(XMLSchema.NORMALIZEDSTRING)
                                || dt.equals(XMLSchema.TOKEN) || dt.equals(XMLSchema.NMTOKEN)
                                || dt.equals(XMLSchema.LANGUAGE) || dt.equals(XMLSchema.NAME)
                                || dt.equals(XMLSchema.NCNAME)) {
                            value = lit.getLabel();
                        }
                    }

                    if (property != null && value != null) {
                        if ("=".equals(binary.getOperator())) {
                            valueOrRange = value;
                        } else if (value instanceof Comparable<?>) {
                            final Comparable<?> comp = (Comparable<?>) value;
                            if (">".equals(binary.getOperator())) {
                                valueOrRange = swap ? Range.lessThan(comp) : Range
                                        .greaterThan(comp);
                            } else if (">=".equals(binary.getOperator())) {
                                valueOrRange = swap ? Range.atMost(comp) : Range.atLeast(comp);
                            } else if ("<".equals(binary.getOperator())) {
                                valueOrRange = swap ? Range.greaterThan(comp) : Range
                                        .lessThan(comp);
                            } else if ("<=".equals(binary.getOperator())) {
                                valueOrRange = swap ? Range.atLeast(comp) : Range.atMost(comp);
                            }
                        }
                    }
                }

                boolean processed = false;
                if (property != null && valueOrRange != null) {
                    Set<Object> set = restrictions.get(property);
                    if (set == null) {
                        set = ImmutableSet.of(valueOrRange);
                        restrictions.put(property, set);
                        processed = true;
                    } else {
                        final Object oldValue = set.iterator().next();
                        if (oldValue instanceof Range) {
                            final Range oldRange = (Range) oldValue;
                            if (valueOrRange instanceof Range) {
                                final Range newRange = (Range) valueOrRange;
                                if (oldRange.isConnected(newRange)) {
                                    restrictions.put(property,
                                            ImmutableSet.of(oldRange.intersection(newRange)));
                                    processed = true;
                                }
                            } else if (valueOrRange instanceof Comparable) {
                                if (oldRange.contains((Comparable) valueOrRange)) {
                                    restrictions.put(property, ImmutableSet.of(valueOrRange));
                                    processed = true;
                                }
                            }
                        }
                    }
                }

                if (!processed) {
                    remaining = remaining == null ? node : FACTORY.createAndExpr(remaining, node);
                }
            }

            return remaining == null ? null : parse(this.support.namespaces, remaining.getText());

        } catch (final JaxenException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("rawtypes")
    @Nullable
    private Object extractValue(final Expr node) {
        if (node instanceof LiteralExpr) {
            return ((LiteralExpr) node).getLiteral();
        } else if (node instanceof NumberExpr) {
            final Number number = ((NumberExpr) node).getNumber();
            return number instanceof Double || number instanceof Float ? number.doubleValue()
                    : number.longValue(); // always return a Double
        } else if (node instanceof FunctionCallExpr) {
            final FunctionCallExpr function = (FunctionCallExpr) node;
            final String name = function.getFunctionName();
            String arg0 = null;
            String arg1 = null;
            final List params = function.getParameters();
            final int numParams = params.size();
            if (numParams > 0 && params.get(0) instanceof LiteralExpr) {
                arg0 = ((LiteralExpr) params.get(0)).getLiteral();
            }
            if (numParams > 1 && params.get(1) instanceof LiteralExpr) {
                arg1 = ((LiteralExpr) params.get(1)).getLiteral();
            }
            if (name.equals("uri") && numParams == 1 && arg0 != null) {
                return new URIImpl(arg0);
            } else if (name.equals("dateTime") && numParams == 1 && arg0 != null) {
                return Data.convert(arg0, Date.class);
            } else if (name.equals("true") && numParams == 0) {
                return true;
            } else if (name.equals("false") && numParams == 0) {
                return false;
            } else if (name.equals("str") && numParams == 1 && arg0 != null) {
                return Data.getValueFactory().createLiteral(arg0);
            } else if (name.equals("strdt") && numParams == 2 && arg0 != null) {
                final Object dt = extractValue((Expr) params.get(1));
                if (dt instanceof URI) {
                    return Data.getValueFactory().createLiteral(arg0, (URI) dt);
                }
            } else if (name.equals("strlang") && numParams == 2 && arg0 != null && arg1 != null) {
                return Data.getValueFactory().createLiteral(arg0, arg1);

            }
        }
        return null;
    }

    @Nullable
    private URI extractProperty(final Expr node) {
        if (!(node instanceof LocationPath)) {
            return null;
        }
        final LocationPath path = (LocationPath) node;
        Step step = null;
        if (path.getSteps().size() == 1) {
            step = (Step) path.getSteps().get(0);
        } else if (path.getSteps().size() == 2) {
            if (path.getSteps().get(0) instanceof AllNodeStep) {
                step = (Step) path.getSteps().get(1);
            }
        }
        if (!(step instanceof NameStep) || !step.getPredicates().isEmpty()) {
            return null;
        }
        return new URIImpl(this.support.translateNamespacePrefixToUri(((NameStep) step)
                .getPrefix()) + ((NameStep) step).getLocalName());
    }

    private static List<Expr> toCNF(final Expr node) throws JaxenException {

        if (node instanceof BinaryExpr) {

            final BinaryExpr binary = (BinaryExpr) node;
            if ("and".equals(binary.getOperator())) {
                // toCNF(A and B) = toCNF(A) and toCNF(B)
                final List<Expr> result = Lists.newArrayList();
                result.addAll(toCNF(binary.getLHS()));
                result.addAll(toCNF(binary.getRHS()));
                return result;

            } else if ("or".equals(binary.getOperator())) {
                // toCNF(A or B) = and of {x or y | x in toCNF(A), y in toCNF(B)}
                final List<Expr> result = Lists.newArrayList();
                final List<Expr> leftArgs = toCNF(binary.getLHS());
                final List<Expr> rightArgs = toCNF(binary.getRHS());
                for (final Expr leftArg : leftArgs) {
                    for (final Expr rightArg : rightArgs) {
                        result.add(FACTORY.createOrExpr(leftArg, rightArg));
                    }
                }
                return result;
            }

        } else if (node instanceof FunctionCallExpr
                && "not".equals(((FunctionCallExpr) node).getFunctionName())
                && ((FunctionCallExpr) node).getParameters().get(0) instanceof BinaryExpr) {

            final FunctionCallExpr call = (FunctionCallExpr) node;
            final BinaryExpr binary = (BinaryExpr) call.getParameters().get(0);
            if ("or".equals(binary.getOperator())) {
                // toCNF(not(A or B)) = toCNF(not(A)) and toCNF(not(B))
                final FunctionCallExpr leftNot = FACTORY.createFunctionCallExpr(null, "not");
                leftNot.addParameter(binary.getLHS());
                final FunctionCallExpr rightNot = FACTORY.createFunctionCallExpr(null, "not");
                rightNot.addParameter(binary.getRHS());
                return ImmutableList.<Expr>builder().addAll(toCNF(leftNot))
                        .addAll(toCNF(rightNot)).build();

            } else if ("and".equals(binary.getOperator())) {
                // toCNF(not(A and B)) = toCNF(not(A) or not(B))
                final FunctionCallExpr leftNot = FACTORY.createFunctionCallExpr(null, "not");
                leftNot.addParameter(binary.getLHS());
                final FunctionCallExpr rightNot = FACTORY.createFunctionCallExpr(null, "not");
                rightNot.addParameter(binary.getRHS());
                return toCNF(FACTORY.createOrExpr(leftNot, rightNot));
            }
        }

        return ImmutableList.of(node);
    }

    /**
     * {@inheritDoc} Two {@code XPath} expressions are equal if they have the same string and
     * lenient mode.
     */
    @Override
    public final boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || object.getClass() != getClass()) {
            return false;
        }
        final XPath other = (XPath) object;
        return this.support.string.equals(other.support.string);
    }

    /**
     * {@inheritDoc} The returned hash code is based exclusively on the string and lenient mode of
     * this {@code XPath} expression.
     */
    @Override
    public final int hashCode() {
        return Objects.hashCode(this.support.string, this.getClass().getSimpleName());
    }

    /**
     * {@inheritDoc} The method returns the {@code XPath} string. Note this is not the string
     * originally supplied, but a string obtained from it during the validation step with a
     * {@code WITH} clause that contains all the namespace declarations necessary to make this
     * expression independent from external mappings.
     */
    @Override
    public final String toString() {
        return this.support.string;
    }

    /**
     * Returns the {@code XPath} string corresponding to the sequence of values specified. This
     * utility method can be used when programmatically composing an {@code XPath} string.
     *
     * @param values
     *            the values to format
     * @return the corresponding {@code XPath} string
     */
    public static String toString(final Object... values) {
        return encode(values, true);
    }

    public static Object unwrap(final Object object) {
        return XPathNavigator.INSTANCE.unwrap(object);
    }

    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(toString(), isLenient());
    }

    private static final class SerializedForm {

        private final String string;

        private final boolean lenient;

        SerializedForm(final String string, final boolean lenient) {
            this.string = string;
            this.lenient = lenient;
        }

        private Object readResolve() throws ObjectStreamException {
            return parse(this.string).lenient(this.lenient);
        }

    }

    private static final class LenientXPath extends XPath {

        LenientXPath(final Support support) {
            super(support);
        }

        @Override
        public boolean isLenient() {
            return true;
        }

    }

    private static final class StrictXPath extends XPath {

        StrictXPath(final Support support) {
            super(support);
        }

        @Override
        public boolean isLenient() {
            return false;
        }

    }

    private static final class Support extends ContextSupport implements NamespaceContext {

        private static final long serialVersionUID = -2960336999829855818L;

        final String string;

        final String head;

        final String body;

        final Expr expr;

        final Set<URI> properties;

        final Map<String, String> namespaces;

        Support(final Expr expr, final String body, final Set<URI> properties,
                final Map<String, String> namespaces) {

            super(null, XPathFunction.CONTEXT, VARIABLES, XPathNavigator.INSTANCE);

            final StringBuilder builder = new StringBuilder();
            for (final String prefix : Ordering.natural().sortedCopy(namespaces.keySet())) {
                final String namespace = namespaces.get(prefix);
                if (!namespace.equals(Data.getNamespaceMap().get(prefix))) {
                    builder.append(builder.length() == 0 ? "" : ", ").append(prefix).append(": ")
                            .append("<").append(namespace).append(">");
                }
            }
            final String head = builder.toString();

            this.string = head.isEmpty() ? body : "with " + head + " : " + body;
            this.head = head.isEmpty() ? "" : this.string.substring(5, 5 + head.length());
            this.body = head.isEmpty() ? body : this.string.substring(8 + head.length());
            this.expr = expr;
            this.properties = properties;
            this.namespaces = ImmutableBiMap.copyOf(namespaces);
        }

        @Override
        public String translateNamespacePrefixToUri(final String prefix) {
            return this.namespaces.get(prefix);
        }

    }

}
