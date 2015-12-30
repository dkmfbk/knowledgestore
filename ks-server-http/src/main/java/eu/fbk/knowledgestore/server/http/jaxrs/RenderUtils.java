package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;
import com.google.common.net.UrlEscapers;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.server.http.UIConfig;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;

/**
 * Collection of utility methods for rendering various kinds of object to HTML.
 */
public final class RenderUtils {

    private static final boolean CHAR_OFFSET_HACK = Boolean.parseBoolean(System.getProperty(
            "ks.charOffsetHack", "false"))
            || Boolean.parseBoolean(MoreObjects.firstNonNull(System.getenv("KS_CHAR_OFFSET_HACK"),
                    "false"));

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    /**
     * Render a generic object, returning the corresponding HTML string. Works for null objects,
     * RDF {@code Value}s, {@code Record}s, {@code BindingSet}s and {@code Iterable}s of the
     * former.
     *
     * @param object
     *            the object to render.
     * @return the rendered HTML string
     */
    public static String render(final Object object) {
        try {
            final StringBuilder builder = new StringBuilder();
            render(object, builder);
            return builder.toString();
        } catch (final IOException ex) {
            throw new Error(ex); // should not happen
        }
    }

    /**
     * Render a generic object, emitting the corresponding HTML string to the supplied
     * {@code Appendable} object. Works for null objects, RDF {@code Value}s, {@code Record}s,
     * {@code BindingSet}s and {@code Iterable}s of the former.
     *
     * @param object
     *            the object to render.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Appendable> T render(final Object object, final T out)
            throws IOException {

        if (object instanceof URI) {
            render((URI) object, null, out);

        } else if (object instanceof Literal) {
            final Literal literal = (Literal) object;
            out.append("<span");
            if (literal.getLanguage() != null) {
                out.append(" title=\"@").append(literal.getLanguage()).append("\"");
            } else if (literal.getDatatype() != null) {
                out.append(" title=\"&lt;").append(literal.getDatatype().stringValue())
                        .append("&gt;\"");
            }
            out.append(">").append(literal.stringValue()).append("</span>");

        } else if (object instanceof BNode) {
            final BNode bnode = (BNode) object;
            out.append("_:").append(bnode.getID());

        } else if (object instanceof Record) {
            final Record record = (Record) object;
            out.append("<table class=\"record table table-condensed\"><tbody>\n<tr><td>ID</td><td>");
            render(record.getID(), out);
            out.append("</td></tr>\n");
            for (final URI property : Ordering.from(Data.getTotalComparator()).sortedCopy(
                    record.getProperties())) {
                out.append("<tr><td>");
                render(property, out);
                out.append("</td><td>");
                final List<Object> values = record.get(property);
                if (values.size() == 1) {
                    render(values.get(0), out);
                } else {
                    out.append("<div class=\"scroll\">");
                    String separator = "";
                    for (final Object value : Ordering.from(Data.getTotalComparator()).sortedCopy(
                            record.get(property))) {
                        out.append(separator);
                        render(value, out);
                        separator = "<br/>";
                    }
                    out.append("</div>");
                }
                out.append("</td></tr>\n");
            }
            out.append("</tbody></table>");

        } else if (object instanceof BindingSet) {
            render(ImmutableSet.of(object));

        } else if (object instanceof Iterable<?>) {
            final Iterable<?> iterable = (Iterable<?>) object;
            boolean isEmpty = true;
            boolean isIterableOfSolutions = true;
            for (final Object element : iterable) {
                isEmpty = false;
                if (!(element instanceof BindingSet)) {
                    isIterableOfSolutions = false;
                    break;
                }
            }
            if (!isEmpty) {
                if (!isIterableOfSolutions) {
                    String separator = "";
                    for (final Object element : (Iterable<?>) object) {
                        out.append(separator);
                        render(element, out);
                        separator = "<br/>";
                    }
                } else {
                    Joiner.on("").appendTo(out,
                            renderSolutionTable(null, (Iterable<BindingSet>) object).iterator());
                }
            }

        } else if (object != null) {
            out.append(object.toString());
        }

        return out;
    }

    public static <T extends Appendable> T render(final URI uri, @Nullable final URI selection,
            final T out) throws IOException {
        out.append("<a href=\"").append(RenderUtils.escapeHtml(uri.stringValue())).append("\"");
        if (selection != null) {
            out.append(" data-sel=\"").append(RenderUtils.escapeHtml(selection)).append("\"");
        }
        out.append(" class=\"uri\">").append(RenderUtils.shortenURI(uri)).append("</a>");
        return out;
    }

    public static <T extends Appendable> T renderText(final String text, final String contentType,
            final T out) throws IOException {
        if (contentType.equals("text/plain")) {
            out.append("<div class=\"text\">\n").append(RenderUtils.escapeHtml(text))
                    .append("\n</div>\n");
        } else {
            // TODO: only XML enabled by default - should be generalized / made more robust
            out.append("<pre class=\"text-pre pre-scrollable prettyprint linenums lang-xml\">")
                    .append(RenderUtils.escapeHtml(text)).append("</pre>");
        }
        return out;
    }

    public static <T extends Appendable> T renderText(final String text,
            final List<Record> mentions, @Nullable final URI selection, final boolean canSelect,
            final boolean onlyMention, final UIConfig config, final T out) throws IOException {

        final List<String> lines = Lists.newArrayList(Splitter.on('\n').split(text));
        if (CHAR_OFFSET_HACK) {
            for (int i = 0; i < lines.size(); ++i) {
                lines.set(i, lines.get(i).replaceAll("\\s+", " ") + " ");
            }
        }

        int lineStart = CHAR_OFFSET_HACK ? 0 : -1;
        int lineOffset = 0;
        int mentionIndex = 0;

        boolean anchorAdded = false;

        out.append("<div class=\"text\">\n");
        for (final String l : lines) {
            final String line = CHAR_OFFSET_HACK ? l.trim() : l;
            lineStart += CHAR_OFFSET_HACK ? 0 : 1;
            boolean mentionFound = false;
            while (mentionIndex < mentions.size()) {
                final Record mention = mentions.get(mentionIndex);
                final Integer begin = mention.getUnique(NIF.BEGIN_INDEX, Integer.class);
                final Integer end = mention.getUnique(NIF.END_INDEX, Integer.class);
                String cssStyle = null;
                for (final UIConfig.Category category : config.getMentionCategories()) {
                    if (category.getCondition().evalBoolean(mention)) {
                        cssStyle = category.getStyle();
                        break;
                    }
                }
                if (cssStyle == null || begin == null || end == null
                        || begin < lineStart + lineOffset) {
                    ++mentionIndex;
                    continue;
                }
                if (end > lineStart + line.length()) {
                    break;
                }
                final boolean selected = mention.getID().equals(selection)
                        || mention.get(KS.REFERS_TO, URI.class).contains(selection);
                if (!mentionFound) {
                    out.append("<p>");
                }
                out.append(RenderUtils.escapeHtml(line.substring(lineOffset, begin - lineStart)));
                out.append("<a href=\"#\"");
                if (selected && !anchorAdded) {
                    out.append(" id=\"selection\"");
                    anchorAdded = true;
                }
                if (canSelect) {
                    out.append(" onclick=\"select('").append(RenderUtils.escapeJavaScriptString(mention.getID()))
                            .append("')\"");
                }
                out.append(" class=\"mention").append(selected ? " selected" : "")
                        .append("\" style=\"").append(cssStyle).append("\" title=\"");
                String separator = "";
                for (final URI property : config.getMentionOverviewProperties()) {
                    final List<Value> values = mention.get(property, Value.class);
                    if (!values.isEmpty()) {
                        out.append(separator)
                                .append(Data.toString(property, Data.getNamespaceMap()))
                                .append(" = ");
                        for (final Value value : values) {
                            if (!KS.MENTION.equals(value)
                                    && !NWR.TIME_OR_EVENT_MENTION.equals(value)
                                    && !NWR.ENTITY_MENTION.equals(value)) {
                                out.append(" ").append(
                                        Data.toString(value, Data.getNamespaceMap()));
                            }
                        }
                        separator = "\n";
                    }
                }
                out.append("\">");
                out.append(RenderUtils.escapeHtml(line.substring(begin - lineStart, end
                        - lineStart)));
                out.append("</a>");
                lineOffset = end - lineStart;
                ++mentionIndex;
                mentionFound = true;
            }
            if (mentionFound || !onlyMention) {
                if (!mentionFound) {
                    out.append("<p>\n");
                }
                out.append(RenderUtils.escapeHtml(line.substring(lineOffset, line.length())));
                out.append("</p>\n");
            }
            lineStart += line.length();
            lineOffset = 0;
        }
        out.append("</div>\n");
        return out;
    }

    /**
     * Render in a streaming-way the solutions of a SPARQL SELECT query to an HTML table, emitting
     * an iterable with of HTML fragments.
     *
     * @param variables
     *            the variables to render in the table, in the order they should be rendered; if
     *            null, variables will be automatically extracted from the solutions and all the
     *            variables in alphanumeric order will be emitted
     * @param solutions
     *            the solutions to render
     */
    public static Iterable<String> renderSolutionTable(final List<String> variables,
            final Iterable<? extends BindingSet> solutions) {

        final List<String> actualVariables;
        if (variables != null) {
            actualVariables = ImmutableList.copyOf(variables);
        } else {
            final Set<String> variableSet = Sets.newHashSet();
            for (final BindingSet solution : solutions) {
                variableSet.addAll(solution.getBindingNames());
            }
            actualVariables = Ordering.natural().sortedCopy(variableSet);
        }

        final int width = 75 / actualVariables.size();
        final StringBuilder builder = new StringBuilder();
        builder.append("<table class=\"sparql table table-condensed tablesorter\"><thead>\n<tr>");
        for (final String variable : actualVariables) {
            builder.append("<th style=\"width: ").append(width).append("%\">")
                    .append(escapeHtml(variable)).append("</th>");
        }
        final Iterable<String> header = ImmutableList.of(builder.toString());
        final Iterable<String> footer = ImmutableList.of("</tbody></table>");
        final Function<BindingSet, String> renderer = new Function<BindingSet, String>() {

            @Override
            public String apply(final BindingSet bindings) {
                if (Thread.interrupted()) {
                    throw new IllegalStateException("Interrupted");
                }
                final StringBuilder builder = new StringBuilder();
                builder.append("<tr>");
                for (final String variable : actualVariables) {
                    builder.append("<td>");
                    try {
                        render(bindings.getValue(variable), builder);
                    } catch (final IOException ex) {
                        throw new Error(ex);
                    }
                    builder.append("</td>");
                }
                builder.append("</tr>\n");
                return builder.toString();
            }

        };
        return Iterables.concat(header, Iterables.transform(solutions, renderer), footer);
    }

    public static <T extends Appendable> T renderMultisetTable(final T out,
            final Multiset<?> multiset, final String elementHeader,
            final String occurrencesHeader, @Nullable final String linkTemplate)
            throws IOException {

        final String tableID = "table" + COUNTER.getAndIncrement();
        out.append("<table id=\"").append(tableID).append("\" class=\"display datatable\">\n");
        out.append("<thead>\n<tr><th>").append(MoreObjects.firstNonNull(elementHeader, "Value"))
                .append("</th><th>")
                .append(MoreObjects.firstNonNull(occurrencesHeader, "Occurrences"))
                .append("</th></tr>\n</thead>\n");
        out.append("<tbody>\n");
        for (final Object element : multiset.elementSet()) {
            final int occurrences = multiset.count(element);
            out.append("<tr><td>");
            RenderUtils.render(element, out);
            out.append("</td><td>");
            if (linkTemplate == null) {
                out.append(Integer.toString(occurrences));
            } else {
                final Escaper esc = UrlEscapers.urlFormParameterEscaper();
                final String e = esc.escape(Data.toString(element, Data.getNamespaceMap()));
                final String u = linkTemplate.replace("${element}", e);
                out.append("<a href=\"").append(u).append("\">")
                        .append(Integer.toString(occurrences)).append("</a>");
            }
            out.append("</td></tr>\n");
        }
        out.append("</tbody>\n</table>\n");
        out.append("<script>$(document).ready(function() { applyDataTable('").append(tableID)
                .append("', false, {}); });</script>");
        return out;
    }

    public static <T extends Appendable> T renderRecordsTable(final T out,
            final Iterable<Record> records, @Nullable List<URI> propertyURIs,
            @Nullable final String extraOptions) throws IOException {

        // Extract the properties to show if not explicitly supplied
        if (propertyURIs == null) {
            final Set<URI> uriSet = Sets.newHashSet();
            for (final Record record : records) {
                uriSet.addAll(record.getProperties());
            }
            propertyURIs = Ordering.from(Data.getTotalComparator()).sortedCopy(uriSet);
        }

        // Emit the table
        final String tableID = "table" + COUNTER.getAndIncrement();
        out.append("<table id=\"").append(tableID).append("\" class=\"display datatable\">\n");
        out.append("<thead>\n<tr><th>URI</th>");
        for (final URI propertyURI : propertyURIs) {
            out.append("<th>").append(RenderUtils.shortenURI(propertyURI)).append("</th>");
        }
        out.append("</tr>\n</thead>\n<tbody>\n");
        for (final Record record : records) {
            out.append("<tr><td>").append(RenderUtils.render(record.getID())).append("</td>");
            for (final URI propertyURI : propertyURIs) {
                out.append("<td>").append(RenderUtils.render(record.get(propertyURI)))
                        .append("</td>");
            }
            out.append("</tr>\n");
        }
        out.append("</tbody>\n</table>\n");
        out.append("<script>$(document).ready(function() { applyDataTable('").append(tableID)
                .append("', true, {").append(Strings.nullToEmpty(extraOptions))
                .append("}); });</script>");
        return out;
    }

    public static <T extends Appendable> T renderRecordsAggregateTable(final T out,
            final Iterable<Record> records, @Nullable final Predicate<URI> propertyFilter,
            @Nullable final String linkTemplate, @Nullable final String extraOptions)
            throws IOException {

        // Aggregate properties and values
        final Map<URI, Multiset<Value>> properties = Maps.newHashMap();
        final Map<Object, URI> examples = Maps.newHashMap();
        for (final Record record : records) {
            for (final URI property : record.getProperties()) {
                if (propertyFilter == null || propertyFilter.apply(property)) {
                    Multiset<Value> values = properties.get(property);
                    if (values == null) {
                        values = HashMultiset.create();
                        properties.put(property, values);
                    }
                    for (final Value value : record.get(property, Value.class)) {
                        values.add(value);
                        examples.put(ImmutableList.of(property, value), record.getID());
                    }
                }
            }
        }

        // Emit the table
        final Ordering<Object> ordering = Ordering.from(Data.getTotalComparator());
        final String tableID = "table" + COUNTER.getAndIncrement();
        out.append("<table id=\"").append(tableID).append("\" class=\"display datatable\">\n");
        out.append("<thead>\n<tr><th>Property</th><th>Value</th>"
                + "<th>Occurrences</th><th>Example</th></tr>\n</thead>\n");
        out.append("<tbody>\n");
        for (final URI property : ordering.sortedCopy(properties.keySet())) {
            final Multiset<Value> values = properties.get(property);
            for (final Value value : ordering.sortedCopy(values.elementSet())) {
                final int occurrences = values.count(value);
                final URI example = examples.get(ImmutableList.of(property, value));
                out.append("<tr><td>");
                render(property, out);
                out.append("</td><td>");
                render(value, out);
                out.append("</td><td>");
                if (linkTemplate == null) {
                    out.append(Integer.toString(occurrences));
                } else {
                    final Escaper e = UrlEscapers.urlFormParameterEscaper();
                    final String p = e.escape(Data.toString(property, Data.getNamespaceMap()));
                    final String v = e.escape(Data.toString(value, Data.getNamespaceMap()));
                    final String u = linkTemplate.replace("${property}", p).replace("${value}", v);
                    out.append("<a href=\"").append(u).append("\">")
                            .append(Integer.toString(occurrences)).append("</a>");
                }
                out.append("</td><td>");
                render(example, out);
                out.append("</td></tr>\n");
            }
        }
        out.append("</tbody>\n</table>\n");
        out.append("<script>$(document).ready(function() { applyDataTable('").append(tableID)
                .append("', false, {").append(Strings.nullToEmpty(extraOptions))
                .append("}); });</script>");
        return out;
    }

    /**
     * Returns a shortened version of the supplied RDF {@code URI}.
     *
     * @param uri
     *            the uri to shorten
     * @return the shortened URI string
     */
    @Nullable
    public static String shortenURI(@Nullable final URI uri) {
        if (uri == null) {
            return null;
        }
        final String prefix = Data.namespaceToPrefix(uri.getNamespace(), Data.getNamespaceMap());
        if (prefix != null) {
            return prefix + ':' + uri.getLocalName();
        }
        final String ns = uri.getNamespace();
        return "&lt;.." + uri.stringValue().substring(ns.length() - 1) + "&gt;";
        // final int index = uri.stringValue().lastIndexOf('/');
        // if (index >= 0) {
        // return "&lt;.." + uri.stringValue().substring(index) + "&gt;";
        // }
        // return "&lt;" + uri.stringValue() + "&gt;";
    }

    /**
     * Transforms the supplied object to an escaped HTML string.
     *
     * @param object
     *            the object
     * @return the escaped HTML string
     */
    @Nullable
    public static String escapeHtml(@Nullable final Object object) {
        return object == null ? null : HtmlEscapers.htmlEscaper().escape(object.toString());
    }

    public static String escapeJavaScriptString(@Nullable final Object object) {
        return object == null ? null : object.toString().replaceAll("'", "\\\\'");
    }

    private RenderUtils() {
    }

}
