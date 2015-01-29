package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.html.HtmlEscapers;

import org.codehaus.enunciate.Facet;
import org.glassfish.jersey.server.mvc.Viewable;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.server.http.UIConfig;
import eu.fbk.knowledgestore.server.http.UIConfig.Example;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.knowledgestore.vocabulary.NIF;
import eu.fbk.knowledgestore.vocabulary.NWR;

@Path("/")
@Facet(name = "internal")
public class Root extends Resource {

    private static final String VERSION = Util.getVersion("eu.fbk.knowledgestore", "ks-core",
            "devel");

    private static final URI NUM_MENTIONS = new URIImpl(KS.NAMESPACE + "numMentions");

    private static final List<String> DESCRIBE_VARS = ImmutableList.of("subject", "predicate",
            "object", "graph");

    private static final int MAX_FETCHED_RESULTS = 10000;

    private static final boolean CHAR_OFFSET_HACK = Boolean.parseBoolean(System.getProperty(
            "ks.charOffsetHack", "false"));

    private static final Pattern NIF_OFFSET_PATTERN = Pattern.compile("char=(\\d+),(\\d+)");

    @GET
    public Response getStatus() {
        String uri = getUriInfo().getRequestUri().toString();
        uri = (uri.endsWith("/") ? uri : uri + "/") + "ui";
        final Response redirect = Response.status(Status.FOUND).location(java.net.URI.create(uri))
                .build();
        throw new WebApplicationException(redirect);
    }

    @GET
    @Path("/static/{name}")
    public Response download(@PathParam("name") final String name) throws Throwable {
        final InputStream stream = Root.class.getResourceAsStream(name);
        if (stream == null) {
            throw new WebApplicationException("No resource named " + name, Status.NOT_FOUND);
        }
        final String type = Data.extensionToMimeType(name);
        init(false, type, null, null);
        final CacheControl control = new CacheControl();
        control.setMaxAge(3600 * 24);
        control.setMustRevalidate(true);
        control.setPrivate(false);
        return newResponseBuilder(Status.OK, stream, null).cacheControl(control).build();
    }

    @GET
    @Path("/ui")
    @Produces("text/html;charset=UTF-8")
    public Viewable ui() throws Throwable {

        final Map<String, Object> model = Maps.newHashMap();
        model.put("maxTriples", getUIConfig().getResultLimit());
        String view = "/status";

        final MultivaluedMap<String, String> parameters = getUriInfo().getQueryParameters();
        final String action = parameters.getFirst("action");

        try {
            if ("lookup".equals(action)) {
                final URI id = Data.convert(parameters.getFirst("id"), URI.class, null);
                final URI selection = Data.convert(parameters.getFirst("selection"), URI.class,
                        null);
                final Integer limit = Data.convert(parameters.getFirst("limit"), Integer.class,
                        null);
                view = "/lookup";
                model.put("tabLookup", Boolean.TRUE);
                uiLookup(model, id, selection, limit);

            } else if ("sparql".equals(action)) {
                final String query = Strings.emptyToNull(parameters.getFirst("query"));
                final String timeoutString = parameters.getFirst("timeout");
                Long timeout = null;
                if (parameters.containsKey("timeout")) {
                    try {
                        timeout = 1000 * Long.valueOf(timeoutString);
                        model.put("timeout", timeoutString);
                    } catch (final Throwable ex) {
                        // ignore
                    }
                }
                view = "/sparql";
                model.put("tabSparql", Boolean.TRUE);
                uiSparql(model, query, timeout);

            } else {
                uiStatus(model);
            }
        } catch (final Throwable ex) {
            if (ex instanceof OperationException) {
                final OperationException oex = (OperationException) ex;
                model.put("error", oex.getOutcome().toString());
                if (oex.getOutcome().getStatus() == Outcome.Status.ERROR_UNEXPECTED) {
                    LOGGER.error("Unexpected error", ex);
                }
            } else {
                model.put("error", ex.getMessage());
                LOGGER.error("Unexpected error", ex);
            }
        }

        return new Viewable(view, model);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Root.class);

    private void uiStatus(final Map<String, Object> model) {

        // Emit uptime and percentage spent in GC
        final StringBuilder builder = new StringBuilder();
        final long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
        final long days = uptime / (24 * 60 * 60 * 1000);
        final long hours = uptime / (60 * 60 * 1000) - days * 24;
        final long minutes = uptime / (60 * 1000) - (days * 24 + hours) * 60;
        long gctime = 0;
        for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gctime += bean.getCollectionTime(); // assume 1 bean or they don't work in parallel
        }
        builder.append(days == 0 ? "" : days + "d").append(hours == 0 ? "" : hours + "h")
                .append(minutes).append("m uptime, ").append(gctime * 100 / uptime).append("% gc");

        // Emit memory usage
        final MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        final MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        final long used = heap.getUsed() + nonHeap.getUsed();
        final long committed = heap.getCommitted() + nonHeap.getCommitted();
        final long mb = 1024 * 1024;
        long max = 0;
        for (final MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
            max += bean.getPeakUsage().getUsed(); // assume maximum at same time in all pools
        }
        builder.append("; ").append(used / mb).append("/").append(max / mb).append("/")
                .append(committed / mb).append(" MB memory used/peak/committed");

        // Emit thread numbers
        final int numThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        final int maxThreads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        final long startedThreads = ManagementFactory.getThreadMXBean()
                .getTotalStartedThreadCount();
        builder.append("; ").append(numThreads).append("/").append(maxThreads).append("/")
                .append(startedThreads).append(" threads active/peak/started");

        model.put("version", VERSION);
        model.put("status", builder.toString());
    }

    private void uiSparql(final Map<String, Object> model, @Nullable final String query,
            @Nullable final Long timeout) throws Throwable {

        // Emit the example queries
        if (!getUIConfig().getSparqlExamples().isEmpty()) {
            final List<String> links = Lists.newArrayList();
            final StringBuilder script = new StringBuilder();
            int index = 0;
            for (final Example example : getUIConfig().getSparqlExamples()) {
                links.add("<a href=\"#\" onclick=\"$('#query').val(sparqlExample(" + index
                        + "))\">" + escapeHtml(example.getLabel()) + "</a>");
                script.append("if (queryNum == ").append(index).append(") {\n");
                script.append("  return \"")
                        .append(example.getValue().replace("\n", "\\n").replace("\"", "\\\""))
                        .append("\";\n");
                script.append("}\n");
                ++index;
            }
            script.append("return \"\";\n");
            model.put("examplesScript", script.toString());
            model.put("examplesLinks", links);
        }

        // Emit the query and evaluate its results, if possible
        if (query != null) {

            // Emit the query string
            model.put("query", query);

            // Emit the query results (only partially materialized).
            final long ts = System.currentTimeMillis();
            final Stream<BindingSet> stream = sendQuery(query, timeout);
            @SuppressWarnings("unchecked")
            final List<String> vars = stream.getProperty("variables", List.class);
            final Iterator<BindingSet> iterator = stream.iterator();
            final List<BindingSet> fetched = ImmutableList.copyOf(Iterators.limit(iterator,
                    MAX_FETCHED_RESULTS));
            model.put("results", render(vars, Iterables.concat(fetched, Stream.create(iterator))));
            final long elapsed = System.currentTimeMillis() - ts;

            // Emit the results message
            final StringBuilder builder = new StringBuilder();
            if (fetched.size() < MAX_FETCHED_RESULTS) {
                builder.append(fetched.size()).append(" results in ");
                builder.append(elapsed).append(" ms");
            } else {
                builder.append("more than ").append(MAX_FETCHED_RESULTS);
            }
            if (timeout != null && elapsed > timeout) {
                builder.append(" (timed out, more results may be available)");
            }
            model.put("resultsMessage", builder.toString());
        }
    }

    private void uiLookup(final Map<String, Object> model, @Nullable final URI id,
            @Nullable final URI selection, @Nullable final Integer limit) throws Throwable {

        if (!getUIConfig().getLookupExamples().isEmpty()) {
            model.put("examplesCount", getUIConfig().getLookupExamples().size());
            model.put("examples", getUIConfig().getLookupExamples());
        }

        final int actualLimit = limit == null ? getUIConfig().getResultLimit() : limit;

        if (id != null) {
            model.put("id", id);
            if (!uiLookupResource(model, id, selection, actualLimit) //
                    && !uiLookupMention(model, id, actualLimit) //
                    && !uiLookupEntity(model, id, actualLimit)) {
                model.put("text", "NO ENTRY FOR ID " + id);
            }
        }
    }

    private boolean uiLookupResource(final Map<String, Object> model, final URI resourceID,
            final URI selection, final int limit) throws Throwable {

        // Retrieve the resource record for the URI specified. Return false if not found
        final Record resource = getRecord(KS.RESOURCE, resourceID);
        if (resource == null) {
            return false;
        }

        // Get mentions, generating links and identifying selected mentions and entities
        final List<Record> mentions = getResourceMentions(resourceID);
        URI selectedEntityID = null;
        Record selectedMention = null;
        final List<String> mentionLinks = Lists.newArrayList();
        final Set<String> entityLinks = Sets.newTreeSet();
        final String linkTemplate = "<a onclick=\"select('%s')\" href=\"#\">%s</a>";
        for (final Record mention : mentions) {
            final URI mentionID = mention.getID();
            mentionLinks.add(String.format(linkTemplate, mentionID, shorten(mentionID)));
            if (mention.getID().equals(selection)) {
                selectedMention = mention;
            }
            for (final URI entityID : mention.get(KS.REFERS_TO, URI.class)) {
                entityLinks.add(String.format(linkTemplate, entityID, shorten(entityID)));
                if (entityID.equals(selection)) {
                    selectedEntityID = selection;
                }
            }
        }

        // Select the resource template
        model.put("resource", Boolean.TRUE);

        // Emit mentions and entities dropdown lists
        if (!mentionLinks.isEmpty()) {
            model.put("resourceMentionsCount", mentionLinks.size());
            model.put("resourceMentions", mentionLinks);
        }
        if (!entityLinks.isEmpty()) {
            model.put("resourceEntitiesCount", entityLinks.size());
            model.put("resourceEntities", entityLinks);
        }

        // Emit resource text box
        final Representation representation = getRepresentation(resourceID);
        if (representation != null) {
            final String text = representation.writeToString();
            final StringBuilder builder = new StringBuilder();
            if (!mentions.isEmpty()) {
                render(builder, text, mentions, selection, true, false);
            } else {
                final Record metadata = representation.getMetadata();
                model.put("resourcePrettyPrint", Boolean.TRUE);
                render(builder, text, metadata.getUnique(NIE.MIME_TYPE, String.class));
            }
            model.put("resourceText", builder.toString());
        }

        // Emit the details box (mention / entity / resource metadata)
        if (selectedEntityID != null) {
            // One entity selected - emit its describe triples
            final List<BindingSet> bindings = getEntityDescribeTriples(selection, limit);
            final int total = bindings.size() < limit ? bindings.size()
                    : countEntityDescribeTriples(selection);
            model.put("resourceDetailsBody", render(new StringBuilder(), DESCRIBE_VARS, bindings));
            model.put("resourceDetailsTitle", String.format("<strong> Entity %s "
                    + "(%d triples out of %d)</strong>", render(new StringBuilder(), selection),
                    bindings.size(), total));

        } else if (selectedMention != null) {
            // One mention selected - emit its details
            final StringBuilder builder = new StringBuilder("<strong>Mention ");
            render(builder, selection);
            builder.append("</strong>");
            final List<URI> entityURIs = selectedMention.get(KS.REFERS_TO, URI.class);
            if (!entityURIs.isEmpty()) {
                builder.append("&nbsp;&nbsp;&#10143;&nbsp;&nbsp;<strong>")
                        .append(entityURIs.size() == 1 ? "Entity" : "Entities")
                        .append("</strong>");
                for (final URI entityURI : entityURIs) {
                    builder.append("&nbsp;&nbsp;<strong>");
                    render(builder, entityURI);
                    builder.append("</strong> <a href=\"#\" onclick=\"select('")
                            .append(escapeHtml(entityURI)).append("')\">(select)</a>");
                }
            }
            model.put("resourceDetailsTitle", builder.toString());
            model.put("resourceDetailsBody", render(selectedMention));

        } else {
            // Nothing selected - emit resource metadata
            model.put("resourceDetailsTitle", "<strong>Resource metadata</strong>");
            model.put("resourceDetailsBody", render(resource));
        }

        // Signal success
        return true;
    }

    private boolean uiLookupMention(final Map<String, Object> model, final URI mentionID,
            final int limit) throws Throwable {

        // Retrieve the mention for the URI specified. Return false if not found
        final Record mention = getRecord(KS.MENTION, mentionID);
        if (mention == null) {
            return false;
        }

        // Select the mention template
        model.put("mention", Boolean.TRUE);

        // Emit the mention description box
        model.put("mentionData", render(new StringBuilder(), mention).toString());

        // Emit the resource box, including the mention snipped
        final URI resourceID = mention.getUnique(KS.MENTION_OF, URI.class, null);
        if (resourceID != null) {
            model.put("mentionResourceLink", render(new StringBuilder(), resourceID, mentionID));
            final Representation representation = getRepresentation(resourceID);
            if (representation == null) {
                model.put("mentionResourceExcerpt", "RESOURCE CONTENT NOT AVAILABLE");
            } else {
                final String text = representation.writeToString();
                model.put("mentionResourceExcerpt", render(new StringBuilder(), text, //
                        ImmutableList.of(mention), null, false, true).toString());
            }
        }

        // Emit the denoted entities box
        final List<URI> entityIDs = mention.get(KS.REFERS_TO, URI.class);
        if (!entityIDs.isEmpty()) {

            // Emit the triples of the first denoted entity, including their total number
            final URI entityID = entityIDs.iterator().next();
            final List<BindingSet> describeTriples = getEntityDescribeTriples(entityID, limit);
            final int total = describeTriples.size() < limit ? describeTriples.size()
                    : countEntityDescribeTriples(entityID);
            model.put("mentionEntityTriplesShown", describeTriples.size());
            model.put("mentionEntityTriplesTotal", total);
            model.put("mentionEntityTriples", render(new StringBuilder(), //
                    ImmutableList.of("subject", "predicate", "object", "graph"), describeTriples));

            // Emit the link(s) to the pages for all the denoted entities
            if (entityIDs.size() == 1) {
                model.put("mentionEntityLink", render(new StringBuilder(), entityID));
            } else {
                final StringBuilder builder = new StringBuilder();
                for (final URI id : entityIDs) {
                    builder.append(builder.length() > 0 ? "&nbsp;&nbsp;" : "");
                    render(builder, id);
                }
                model.put("mentionEntityLinks", builder.toString());
            }
        }

        // Signal success
        return true;
    }

    private boolean uiLookupEntity(final Map<String, Object> model, final URI entityID,
            final int limit) throws Throwable {

        // Lookup (a subset of) describe triples and graph triples for the specified entity
        final List<BindingSet> describeTriples = getEntityDescribeTriples(entityID, limit);
        final List<BindingSet> graphTriples = getEntityGraphTriples(entityID, limit);
        if (describeTriples.isEmpty() && graphTriples.isEmpty()) {
            return false;
        }

        // Select the entity template
        model.put("entity", Boolean.TRUE);

        // Emit the describe box
        if (!describeTriples.isEmpty()) {
            final int total = describeTriples.size() < limit ? describeTriples.size()
                    : countEntityDescribeTriples(entityID);
            model.put("entityTriplesShown", describeTriples.size());
            model.put("entityTriplesTotal", total);
            model.put("entityTriples", render(new StringBuilder(), //
                    ImmutableList.of("subject", "predicate", "object", "graph"), //
                    describeTriples).toString());
        }

        // Emit the graph box
        if (!graphTriples.isEmpty()) {
            final int total = graphTriples.size() < limit ? graphTriples.size()
                    : countEntityGraphTriples(entityID);
            model.put("entityGraphShown", graphTriples.size());
            model.put("entityGraphTotal", total);
            model.put("entityGraph", render(new StringBuilder(), //
                    ImmutableList.of("subject", "predicate", "object"), graphTriples).toString());
        }

        // Emit the resources box
        final List<Record> resources = getEntityResources(entityID, getUIConfig().getResultLimit());
        if (!resources.isEmpty()) {

            // Emit resource and mention counts
            final int[] counts = countEntityResourcesAndMentions(entityID);
            model.put("entityResourcesShown", resources.size());
            model.put("entityResourcesCount", counts[0]);
            model.put("entityMentionsCount", counts[1]);

            // Emit the resources table
            final StringBuilder builder = new StringBuilder();
            final List<URI> overviewProperties = getUIConfig().getResourceOverviewProperties();
            final int width = 75 / (overviewProperties.size() + 2);
            final String th = "<th style=\"width: " + width + "%\">";
            builder.append("<table class=\"sparql table table-condensed tablesorter\"><thead>\n");
            builder.append("<tr>").append(th).append("resource ID</th>");
            for (final URI property : overviewProperties) {
                builder.append(th).append(escapeHtml(format(property))).append("</th>");
            }
            builder.append(th);
            if (resources.size() < getUIConfig().getResultLimit()) {
                builder.append("# mentions");
            } else {
                builder.append("<span title=\"Number of mentions per resource may be lower than "
                        + "the exact value as only a subset of all the entity mentions has been "
                        + "considered for building this page\"># mentions (truncated)</title>");
            }
            builder.append("</th></tr>\n</thead><tbody>\n");
            for (final Record resource : resources) {
                builder.append("<tr><td>");
                render(builder, resource.getID(), entityID);
                for (final URI property : overviewProperties) {
                    builder.append("</td><td>");
                    render(builder, resource.get(property));
                }
                builder.append("</td><td>");
                render(builder, resource.getUnique(NUM_MENTIONS, Integer.class, null));
                builder.append("</td></tr>\n");
            }
            builder.append("</tbody></table>");
            model.put("entityResources", builder.toString());
        }

        // Signal success
        return true;
    }

    private String render(final Object object) {
        final StringBuilder builder = new StringBuilder();
        render(builder, object);
        return builder.toString();
    }

    private StringBuilder render(final StringBuilder builder, final Object object) {
        if (object instanceof URI) {
            render(builder, (URI) object, (URI) null);
        } else if (object instanceof Literal) {
            render(builder, (Literal) object);
        } else if (object instanceof BNode) {
            render(builder, (BNode) object);
        } else if (object instanceof Record) {
            render(builder, (Record) object);
        } else if (object instanceof Iterable<?>) {
            String separator = "";
            for (final Object element : (Iterable<?>) object) {
                builder.append(separator);
                render(builder, element);
                separator = "<br/>";
            }
        } else if (object != null) {
            builder.append(object);
        }
        return builder;
    }

    private StringBuilder render(final StringBuilder builder, final URI uri,
            @Nullable final URI selection) {
        // builder.append("<a title=\"").append(uri.stringValue()).append("\" href=\"")
        // .append(escapeHtml(uri.stringValue())).append("\" onclick=\"_lookup(this");
        // if (selection != null) {
        // builder.append(",'").append(escapeParam(selection)).append("'");
        // }
        // return builder.append(")\">").append(shorten(uri)).append("</a>");
        builder.append("<a href=\"").append(escapeHtml(uri.stringValue())).append("\"");
        if (selection != null) {
            builder.append(" data-sel=\"").append(escapeHtml(selection)).append("\"");
        }
        return builder.append(" class=\"uri\">").append(shorten(uri)).append("</a>");
    }

    private StringBuilder render(final StringBuilder builder, final Literal literal) {
        builder.append("<span");
        if (literal.getLanguage() != null) {
            builder.append(" title=\"@").append(literal.getLanguage()).append("\"");
        } else if (literal.getDatatype() != null) {
            builder.append(" title=\"&lt;").append(literal.getDatatype().stringValue())
                    .append("&gt;\"");
        }
        return builder.append(">").append(literal.stringValue()).append("</span>");
    }

    private StringBuilder render(final StringBuilder builder, final BNode bnode) {
        return builder.append("_:").append(bnode.getID());
    }

    private StringBuilder render(final StringBuilder builder, final Record record) {
        builder.append("<table class=\"record table table-condensed\"><tbody>\n<tr><td>ID</td><td>");
        render(builder, record.getID());
        builder.append("</td></tr>\n");
        for (final URI property : Ordering.from(Data.getTotalComparator()).sortedCopy(
                record.getProperties())) {
            builder.append("<tr><td>");
            render(builder, property);
            builder.append("</td><td>");
            final List<Object> values = record.get(property);
            if (values.size() == 1) {
                render(builder, values.get(0));
            } else {
                builder.append("<div class=\"scroll\">");
                String separator = "";
                for (final Object value : Ordering.from(Data.getTotalComparator()).sortedCopy(
                        record.get(property))) {
                    builder.append(separator);
                    render(builder, value);
                    separator = "<br/>";
                }
                builder.append("</div>");
            }
            builder.append("</td></tr>\n");
        }
        return builder.append("</tbody></table>");
    }

    private StringBuilder render(final StringBuilder builder, final List<String> variables,
            final Iterable<? extends BindingSet> solutions) {
        for (final String string : render(variables, solutions)) {
            builder.append(string);
        }
        return builder;
    }

    private Iterable<String> render(final List<String> variables,
            final Iterable<? extends BindingSet> solutions) {
        final int width = 75 / variables.size();
        final StringBuilder builder = new StringBuilder();
        builder.append("<table class=\"sparql table table-condensed tablesorter\"><thead>\n<tr>");
        for (final String variable : variables) {
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
                for (final String variable : variables) {
                    builder.append("<td>");
                    render(builder, bindings.getValue(variable));
                    builder.append("</td>");
                }
                builder.append("</tr>\n");
                return builder.toString();
            }

        };
        return Iterables.concat(header, Iterables.transform(solutions, renderer), footer);
    }

    private StringBuilder render(final StringBuilder builder, final String text,
            final String contentType) {
        if (contentType.equals("text/plain")) {
            builder.append("<div class=\"text\">\n").append(escapeHtml(text)).append("\n</div>\n");
        } else {
            // TODO: only XML enabled by default - should be generalized / made more robust
            builder.append("<pre class=\"text-pre pre-scrollable prettyprint linenums lang-xml\">")
                    .append(escapeHtml(text)).append("</pre>");
        }
        return builder;
    }

    private StringBuilder render(final StringBuilder builder, final String text,
            final List<Record> mentions, @Nullable final URI selection, final boolean canSelect,
            final boolean onlyMention) {

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

        builder.append("<div class=\"text\">\n");
        for (final String l : lines) {
            final String line = CHAR_OFFSET_HACK ? l.trim() : l;
            lineStart += CHAR_OFFSET_HACK ? 0 : 1;
            boolean mentionFound = false;
            while (mentionIndex < mentions.size()) {
                final Record mention = mentions.get(mentionIndex);
                final Integer begin = mention.getUnique(NIF.BEGIN_INDEX, Integer.class);
                final Integer end = mention.getUnique(NIF.END_INDEX, Integer.class);
                String cssStyle = null;
                for (final UIConfig.Category category : getUIConfig().getMentionCategories()) {
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
                    builder.append("<p>");
                }
                builder.append(escapeHtml(line.substring(lineOffset, begin - lineStart)));
                builder.append("<a href=\"#\"");
                if (selected && !anchorAdded) {
                    builder.append(" id=\"selection\"");
                    anchorAdded = true;
                }
                if (canSelect) {
                    builder.append(" onclick=\"select('").append(mention.getID().toString())
                            .append("')\"");
                }
                builder.append(" class=\"mention").append(selected ? " selected" : "")
                        .append("\" style=\"").append(cssStyle).append("\" title=\"");
                String separator = "";
                for (final URI property : getUIConfig().getMentionOverviewProperties()) {
                    final List<Value> values = mention.get(property, Value.class);
                    if (!values.isEmpty()) {
                        builder.append(separator).append(format(property)).append(" = ");
                        for (final Value value : values) {
                            if (!KS.MENTION.equals(value)
                                    && !NWR.TIME_OR_EVENT_MENTION.equals(value)
                                    && !NWR.ENTITY_MENTION.equals(value)) {
                                builder.append(" ").append(format(value));
                            }
                        }
                        separator = "\n";
                    }
                }
                builder.append("\">");
                builder.append(escapeHtml(line.substring(begin - lineStart, end - lineStart)));
                builder.append("</a>");
                lineOffset = end - lineStart;
                ++mentionIndex;
                mentionFound = true;
            }
            if (mentionFound || !onlyMention) {
                if (!mentionFound) {
                    builder.append("<p>\n");
                }
                builder.append(escapeHtml(line.substring(lineOffset, line.length())));
                builder.append("</p>\n");
            }
            lineStart += line.length();
            lineOffset = 0;
        }
        return builder.append("</div>\n");
    }

    private String shorten(final URI uri) {
        final String prefix = Data.namespaceToPrefix(uri.getNamespace(), Data.getNamespaceMap());
        if (prefix != null) {
            return prefix + ':' + uri.getLocalName();
        }
        final int index = uri.stringValue().lastIndexOf('/');
        if (index >= 0) {
            return "&lt;.." + uri.stringValue().substring(index) + "&gt;";
        }
        return "&lt;" + uri.stringValue() + "&gt;";
    }

    private String format(final Value value) {
        return Data.toString(value, Data.getNamespaceMap());
    }

    private String escapeHtml(final Object object) {
        return object == null ? null : HtmlEscapers.htmlEscaper().escape(object.toString());
    }

    // DATA ACCESS METHODS

    @Nullable
    private Record getRecord(final URI layer, @Nullable final URI id) throws Throwable {
        final Record record = id == null ? null : getSession().retrieve(layer).ids(id).exec()
                .getUnique();
        if (record != null && layer.equals(KS.MENTION)) {
            for (final URI entityID : getSession()
                    .sparql("SELECT ?e WHERE { ?e $$ $$ }", getUIConfig().getDenotedByProperty(),
                            id).execTuples().transform(URI.class, true, "e")) {
                record.add(KS.REFERS_TO, entityID);
            }
        }
        return record;
    }

    @Nullable
    private Representation getRepresentation(@Nullable final URI resourceID) throws Throwable {
        final Representation representation = resourceID == null ? null : getSession().download(
                resourceID).exec();
        if (representation != null) {
            closeOnCompletion(representation);
        }
        return representation;
    }

    private List<Record> getResourceMentions(final URI resourceID) throws Throwable {

        final Record resource;
        resource = getSession().retrieve(KS.RESOURCE).ids(resourceID).exec().getUnique();
        if (resource == null) {
            return Collections.emptyList();
        }

        final Map<URI, Record> mentions = Maps.newHashMap();
        final List<URI> mentionIDs = resource.get(KS.HAS_MENTION, URI.class);
        if (mentionIDs.isEmpty()) {
            return Collections.emptyList();
        }

        for (final Record mention : getSession().retrieve(KS.MENTION).ids(mentionIDs).exec()) {
            mentions.put(mention.getID(), mention);
        }

        final Set<URI> entityIDs = Sets.newHashSet();
        for (final Record mention : mentions.values()) {
            for (final URI entityID : mention.get(KS.REFERS_TO, URI.class)) {
                entityIDs.add(entityID);
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ?m ?e WHERE { ?e ");
        builder.append(Data.toString(getUIConfig().getDenotedByProperty(), null));
        builder.append(" ?m VALUES ?m {");
        for (final URI mentionID : mentionIDs) {
            builder.append(' ').append(Data.toString(mentionID, null));
        }
        builder.append(" } }");
        for (final BindingSet bindings : getSession().sparql(builder.toString()).execTuples()) {
            final URI mentionID = (URI) bindings.getValue("m");
            final URI entityID = (URI) bindings.getValue("e");
            mentions.get(mentionID).add(KS.REFERS_TO, entityID);
            entityIDs.add(entityID);
        }

        builder = new StringBuilder();
        builder.append("SELECT ?m ?e WHERE { VALUES ?p { sem:hasActor sem:hasTime sem:hasPlace } VALUES ?e0 {");
        for (final URI entityID : entityIDs) {
            builder.append(' ').append(Data.toString(entityID, null));
        }
        builder.append(" } ?e0 ?p ?e . ?e $$ ?m FILTER(STRSTARTS(STR(?m), $$)) }");
        for (final BindingSet bindings : getSession().sparql(builder.toString(),
                getUIConfig().getDenotedByProperty(), resourceID.stringValue()).execTuples()) {
            final URI mentionID = (URI) bindings.getValue("m");
            final URI entityID = (URI) bindings.getValue("e");
            Record mention = mentions.get(mentionID);
            if (mention == null) {
                mention = Record.create(mentionID, KS.MENTION);
                final Matcher matcher = NIF_OFFSET_PATTERN.matcher(mentionID.stringValue());
                if (matcher.find()) {
                    mention.set(NIF.BEGIN_INDEX, Integer.parseInt(matcher.group(1)));
                    mention.set(NIF.END_INDEX, Integer.parseInt(matcher.group(2)));
                }
                mentions.put(mentionID, mention);
            }
            mention.add(KS.REFERS_TO, entityID);
        }

        final List<Record> sortedMentions = Lists.newArrayList(mentions.values());
        sortedMentions.sort(new Comparator<Record>() {

            @Override
            public int compare(final Record r1, final Record r2) {
                final int begin1 = r1.getUnique(NIF.BEGIN_INDEX, Integer.class, 0);
                final int begin2 = r2.getUnique(NIF.BEGIN_INDEX, Integer.class, 0);
                int result = Integer.compare(begin1, begin2);
                if (result == 0) {
                    final int end1 = r1.getUnique(NIF.END_INDEX, Integer.class, Integer.MAX_VALUE);
                    final int end2 = r2.getUnique(NIF.END_INDEX, Integer.class, Integer.MAX_VALUE);
                    result = Integer.compare(end1, end2); // longest mention last
                }
                return result;
            }

        });
        return sortedMentions;
    }

    private List<Record> getEntityResources(final URI entityID, final int maxResults)
            throws Throwable {

        final Stream<BindingSet> tupleStream = getSession().sparql(
                "SELECT ?r (COUNT(*) AS ?n) "
                        + "WHERE { $$ $$ ?m . BIND(IRI(STRBEFORE(STR(?m),'#')) AS ?r) } "
                        + "GROUP BY ?r ORDER BY DESC(?r) LIMIT $$", entityID,
                getUIConfig().getDenotedByProperty(), maxResults).execTuples();

        final Multiset<URI> resourceIDs = HashMultiset.create();
        try {
            for (final BindingSet tuple : tupleStream) {
                final URI resourceID = (URI) tuple.getValue("r");
                final int mentionCount = ((Literal) tuple.getValue("n")).intValue();
                resourceIDs.add(resourceID, mentionCount);
            }
        } finally {
            tupleStream.close();
        }

        final List<Record> resources;
        resources = getSession().retrieve(KS.RESOURCE).ids(resourceIDs).exec().toList();
        for (final Record resource : resources) {
            resource.set(NUM_MENTIONS, resourceIDs.count(resource.getID()));
        }
        return resources;
    }

    private List<BindingSet> getEntityDescribeTriples(final URI entityID, final int limit)
            throws Throwable {
        return getSession()
                .sparql("SELECT (COALESCE(?s, $$) AS ?subject) ?predicate "
                        + "(COALESCE(?o, $$) AS ?object) ?graph "
                        + "WHERE { { GRAPH ?graph { $$ ?predicate ?o } } UNION "
                        + "{ GRAPH ?graph { ?s ?predicate $$ } } } LIMIT $$", entityID, entityID,
                        entityID, entityID, limit).execTuples().toList();
    }

    private List<BindingSet> getEntityGraphTriples(final URI entityID, final int limit)
            throws Throwable {
        return getSession()
                .sparql("SELECT ?subject ?predicate ?object "
                        + "WHERE { GRAPH $$ { ?subject ?predicate ?object } } LIMIT $$", entityID,
                        limit).execTuples().toList();
    }

    private int countEntityDescribeTriples(final URI entityID) throws Throwable {
        return getSession()
                .sparql("SELECT (COUNT(*) AS ?n) "
                        + "WHERE { { GRAPH ?g { $$ ?p ?o } } UNION { GRAPH ?g { ?s ?p $$ } } }",
                        entityID, entityID).execTuples().transform(Integer.class, true, "n")
                .getUnique();
    }

    private int countEntityGraphTriples(final URI entityID) throws Throwable {
        return getSession()
                .sparql("SELECT (COUNT(*) AS ?n) WHERE { GRAPH $$ { ?s ?p ?o } }", entityID)
                .execTuples().transform(Integer.class, true, "n").getUnique();
    }

    private int[] countEntityResourcesAndMentions(final URI entityID) throws Throwable {
        final BindingSet tuple = getSession()
                .sparql("SELECT (COUNT(DISTINCT ?r) AS ?nr) (COUNT(*) AS ?nm) "
                        + "WHERE { $$ $$ ?m . BIND(IRI(STRBEFORE(STR(?m), \"#\")) AS ?r) }",
                        entityID, getUIConfig().getDenotedByProperty()).execTuples().getUnique();
        return new int[] { ((Literal) tuple.getValue("nr")).intValue(),
                ((Literal) tuple.getValue("nm")).intValue() };
    }

    private Stream<BindingSet> sendQuery(final String query, final Long timeout) throws Throwable {

        final String form = RDFUtil.detectSparqlForm(query);
        if (form.equalsIgnoreCase("select")) {
            return closeOnCompletion(getSession().sparql(query).timeout(timeout).execTuples());

        } else if (form.equalsIgnoreCase("construct") || form.equals("describe")) {
            final List<String> variables = ImmutableList.of("subject", "predicate", "object");
            final Function<Statement, BindingSet> transformer = new Function<Statement, BindingSet>() {

                @Override
                public BindingSet apply(final Statement statement) {
                    return new ListBindingSet(variables, statement.getSubject(),
                            statement.getPredicate(), statement.getObject());
                }

            };
            final Stream<BindingSet> stream = getSession().sparql(query).timeout(timeout)
                    .execTriples().transform(transformer, 1);
            stream.setProperty("variables", variables);
            return closeOnCompletion(stream);

        } else {
            final boolean result = getSession().sparql(query).timeout(timeout).execBoolean();
            final List<String> variables = ImmutableList.of("result");
            final BindingSet bindings = new ListBindingSet(variables,
                    BooleanLiteralImpl.valueOf(result));
            return closeOnCompletion(Stream.create(new BindingSet[] { bindings }).setProperty(
                    "variables", variables));
        }
    }

}
