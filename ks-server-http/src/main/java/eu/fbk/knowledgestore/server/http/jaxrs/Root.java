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

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

import org.codehaus.enunciate.Facet;
import org.glassfish.jersey.server.mvc.Viewable;
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
import eu.fbk.knowledgestore.server.http.UIConfig.Example;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.knowledgestore.vocabulary.NIF;

@Path("/")
@Facet(name = "internal")
public class Root extends Resource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Root.class);

    private static final String VERSION = Util.getVersion("eu.fbk.knowledgestore", "ks-core",
            "devel");

    private static final URI NUM_MENTIONS = new URIImpl(KS.NAMESPACE + "numMentions");

    private static final List<String> DESCRIBE_VARS = ImmutableList.of("subject", "predicate",
            "object", "graph");

    private static final int MAX_FETCHED_RESULTS = 10000;

    // private static final Pattern NIF_OFFSET_PATTERN = Pattern.compile("char=(\\d+),(\\d+)");

    @GET
    public Response getStatus() {
        String uri = getUriInfo().getRequestUri().toString();
        uri = (uri.endsWith("/") ? uri : uri + "/") + "ui";
        final Response redirect = Response.status(Status.FOUND).location(java.net.URI.create(uri))
                .build();
        throw new WebApplicationException(redirect);
    }

    @GET
    @Path("/static/{name:.*}")
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

        final String action = getParameter("action", String.class, null, model);
        final Long timeoutSec = getParameter("timeout", Long.class, null, model);
        final Long timeout = timeoutSec == null ? null : timeoutSec * 1000;
        final int limit = getParameter("limit", Integer.class, getUIConfig().getResultLimit(),
                model);

        try {
            if ("lookup".equals(action)) {
                final URI id = getParameter("id", URI.class, null, model);
                final URI selection = getParameter("selection", URI.class, null, model);
                view = "/lookup";
                model.put("tabLookup", Boolean.TRUE);
                uiLookup(model, id, selection, limit);

            } else if ("sparql".equals(action)) {
                final String query = getParameter("query", String.class, null, model);
                view = "/sparql";
                model.put("tabSparql", Boolean.TRUE);
                uiSparql(model, query, timeout);

            } else if ("entity-mentions".equals(action)) {
                final URI entityID = getParameter("entity", URI.class, null, model);
                final URI property = getParameter("property", URI.class, null, model);
                final Value value = getParameter("value", Value.class, null, model);
                view = "/entity-mentions";
                model.put("tabReports", Boolean.TRUE);
                model.put("subtabEntityMentions", Boolean.TRUE);
                uiReportEntityMentions(model, entityID, property, value, limit);

            } else if ("entity-mentions-aggregate".equals(action)) {
                final URI entityID = getParameter("entity", URI.class, null, model);
                view = "/entity-mentions-aggregate";
                model.put("tabReports", Boolean.TRUE);
                model.put("subtabEntityMentionsAggregate", Boolean.TRUE);
                uiReportEntityMentionsAggregate(model, entityID);

            } else if ("mention-value-occurrences".equals(action)) {
                final URI entityID = getParameter("entity", URI.class, null, model);
                final URI property = getParameter("property", URI.class, null, model);
                view = "/mention-value-occurrences";
                model.put("tabReports", Boolean.TRUE);
                model.put("subtabMentionValueOccurrences", Boolean.TRUE);
                uiReportMentionValueOccurrences(model, entityID, property);

            } else if ("mention-property-occurrences".equals(action)) {
                final URI entityID = getParameter("entity", URI.class, null, model);
                view = "/mention-property-occurrences";
                model.put("tabReports", Boolean.TRUE);
                model.put("subtabMentionPropertyOccurrences", Boolean.TRUE);
                uiReportMentionPropertyOccurrences(model, entityID);

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

    @SuppressWarnings("unchecked")
    private <T> T getParameter(final String name, final Class<T> clazz,
            @Nullable final T defaultValue, @Nullable final Map<String, Object> model) {
        T result = defaultValue;
        final String stringValue = getUriInfo().getQueryParameters().getFirst(name);
        if (stringValue != null && !"".equals(stringValue)) {
            if (Value.class.isAssignableFrom(clazz)) {
                final char c = stringValue.charAt(0);
                if (c == '\'' || c == '"' || c == '<' || //
                        stringValue.indexOf(':') >= 0 && stringValue.indexOf('/') < 0) {
                    try {
                        final Value value = Data.parseValue(stringValue, Data.getNamespaceMap());
                        if (clazz.isInstance(value)) {
                            result = clazz.cast(value);
                        }
                    } catch (final Throwable ex) {
                        // ignore
                    }
                }
                if (result == defaultValue) {
                    if (URI.class.equals(clazz)) {
                        result = (T) Data.getValueFactory().createURI(Data.cleanIRI(stringValue));
                    } else if (clazz.isAssignableFrom(Literal.class)) {
                        result = (T) Data.getValueFactory().createLiteral(stringValue);
                    }
                }
            } else {
                result = Data.convert(stringValue, clazz, defaultValue);
            }
        }
        if (result != null) {
            model.put(name, result);
        }
        return result;
    }

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
                        + "))\">" + RenderUtils.escapeJavaScriptString(example.getLabel()) + "</a>");
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

            // Emit the query results (only partially materialized).
            final long ts = System.currentTimeMillis();
            final Stream<BindingSet> stream = sendQuery(query, timeout);
            @SuppressWarnings("unchecked")
            final List<String> vars = stream.getProperty("variables", List.class);
            final Iterator<BindingSet> iterator = stream.iterator();
            final List<BindingSet> fetched = ImmutableList.copyOf(Iterators.limit(iterator,
                    MAX_FETCHED_RESULTS));
            model.put(
                    "results",
                    RenderUtils.renderSolutionTable(vars,
                            Iterables.concat(fetched, Stream.create(iterator))));
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
            @Nullable final URI selection, final int limit) throws Throwable {

        if (!getUIConfig().getLookupExamples().isEmpty()) {
            model.put("examplesCount", getUIConfig().getLookupExamples().size());
            model.put("examples", getUIConfig().getLookupExamples());
        }

        if (id != null) {
            if (!uiLookupResource(model, id, selection, limit) //
                    && !uiLookupMention(model, id, limit) //
                    && !uiLookupEntity(model, id, limit)) {
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
            mentionLinks.add(String.format(linkTemplate, RenderUtils.escapeJavaScriptString(mentionID),
                    RenderUtils.shortenURI(mentionID)));
            if (mention.getID().equals(selection)) {
                selectedMention = mention;
            }
            for (final URI entityID : mention.get(KS.REFERS_TO, URI.class)) {
                entityLinks.add(String.format(linkTemplate, RenderUtils.escapeJavaScriptString(entityID),
                        RenderUtils.shortenURI(entityID)));
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
                RenderUtils.renderText(text, mentions, selection, true, false, getUIConfig(),
                        builder);
            } else {
                final Record metadata = representation.getMetadata();
                model.put("resourcePrettyPrint", Boolean.TRUE);
                RenderUtils.renderText(text, metadata.getUnique(NIE.MIME_TYPE, String.class),
                        builder);
            }
            model.put("resourceText", builder.toString());
        }

        // Emit the details box (mention / entity / resource metadata)
        if (selectedEntityID != null) {
            // One entity selected - emit its describe triples
            final List<BindingSet> bindings = getEntityDescribeTriples(selection, limit);
            final int total = bindings.size() < limit ? bindings.size()
                    : countEntityDescribeTriples(selection);
            model.put("resourceDetailsBody",
                    String.join("", RenderUtils.renderSolutionTable(DESCRIBE_VARS, bindings)));
            model.put("resourceDetailsTitle", String.format("<strong> Entity %s "
                    + "(%d triples out of %d)</strong>", RenderUtils.render(selection),
                    bindings.size(), total));

        } else if (selectedMention != null) {
            // One mention selected - emit its details
            final StringBuilder builder = new StringBuilder("<strong>Mention ");
            RenderUtils.render(selection, builder);
            builder.append("</strong>");
            final List<URI> entityURIs = selectedMention.get(KS.REFERS_TO, URI.class);
            if (!entityURIs.isEmpty()) {
                builder.append("&nbsp;&nbsp;&#10143;&nbsp;&nbsp;<strong>")
                        .append(entityURIs.size() == 1 ? "Entity" : "Entities")
                        .append("</strong>");
                for (final URI entityURI : entityURIs) {
                    builder.append("&nbsp;&nbsp;<strong>");
                    RenderUtils.render(entityURI, builder);
                    builder.append("</strong> <a href=\"#\" onclick=\"select('")
                            .append(RenderUtils.escapeJavaScriptString(entityURI)).append("')\">(select)</a>");
                }
            }
            model.put("resourceDetailsTitle", builder.toString());
            model.put("resourceDetailsBody", RenderUtils.render(selectedMention));

        } else {
            // Nothing selected - emit resource metadata
            model.put("resourceDetailsTitle", "<strong>Resource metadata</strong>");
            model.put("resourceDetailsBody", RenderUtils.render(resource));
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
        model.put("mentionData", RenderUtils.render(mention));

        // Emit the resource box, including the mention snipped
        final URI resourceID = mention.getUnique(KS.MENTION_OF, URI.class, null);
        if (resourceID != null) {
            model.put("mentionResourceLink",
                    RenderUtils.render(resourceID, mentionID, new StringBuilder()).toString());
            final Representation representation = getRepresentation(resourceID);
            if (representation == null) {
                model.put("mentionResourceExcerpt", "RESOURCE CONTENT NOT AVAILABLE");
            } else {
                final String text = representation.writeToString();
                model.put("mentionResourceExcerpt", RenderUtils.renderText(text,
                        ImmutableList.of(mention), null, false, true, getUIConfig(),
                        new StringBuilder()));
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
            model.put("mentionEntityTriples", String.join("", RenderUtils.renderSolutionTable( //
                    ImmutableList.of("subject", "predicate", "object", "graph"), describeTriples)));

            // Emit the link(s) to the pages for all the denoted entities
            if (entityIDs.size() == 1) {
                model.put("mentionEntityLink", RenderUtils.render(entityID));
            } else {
                final StringBuilder builder = new StringBuilder();
                for (final URI id : entityIDs) {
                    builder.append(builder.length() > 0 ? "&nbsp;&nbsp;" : "");
                    RenderUtils.render(id, builder);
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
            model.put("entityTriples", String.join("", RenderUtils.renderSolutionTable( //
                    ImmutableList.of("subject", "predicate", "object", "graph"), describeTriples)));
        }

        // Emit the graph box
        if (!graphTriples.isEmpty()) {
            final int total = graphTriples.size() < limit ? graphTriples.size()
                    : countEntityGraphTriples(entityID);
            model.put("entityGraphShown", graphTriples.size());
            model.put("entityGraphTotal", total);
            model.put("entityGraph", String.join("", RenderUtils.renderSolutionTable( //
                    ImmutableList.of("subject", "predicate", "object"), graphTriples)));
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
                builder.append(th)
                        .append(RenderUtils.escapeHtml(Data.toString(property,
                                Data.getNamespaceMap()))).append("</th>");
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
                RenderUtils.render(resource.getID(), entityID, builder);
                for (final URI property : overviewProperties) {
                    builder.append("</td><td>");
                    RenderUtils.render(resource.get(property), builder);
                }
                builder.append("</td><td>");
                RenderUtils.render(resource.getUnique(NUM_MENTIONS, Integer.class, null), builder);
                builder.append("</td></tr>\n");
            }
            builder.append("</tbody></table>");
            model.put("entityResources", builder.toString());
        }

        // Signal success
        return true;
    }

    private void uiReportEntityMentions(final Map<String, Object> model,
            @Nullable final URI entityID, @Nullable final URI property,
            @Nullable final Value value, final int limit) throws Throwable {

        // Do nothing in case the entity ID is missing
        if (entityID == null) {
            return;
        }

        // Retrieve all the mentions satisfying the property[=value] optional filter
        int numMentions = 0;
        final List<Record> mentions = Lists.newArrayList();
        for (final Record mention : getEntityMentions(entityID, Integer.MAX_VALUE, null)) {
            if (property == null || !mention.isNull(property)
                    && (value == null || mention.get(property).contains(value))) {
                ++numMentions;
                if (mentions.size() < limit) {
                    mentions.add(mention);
                }
            }
        }

        // Render the mention table, including column toggling functionality
        model.put("message", mentions.size() + " mentions shown out of " + numMentions);
        model.put("mentionTable",
                RenderUtils.renderRecordsTable(new StringBuilder(), mentions, null, null));
    }

    private void uiReportEntityMentionsAggregate(final Map<String, Object> model,
            final URI entityID) throws Throwable {

        // Do nothing in case the entity ID is missing
        if (entityID == null) {
            return;
        }

        // Render the table
        final Stream<Record> mentions = getEntityMentions(entityID, Integer.MAX_VALUE, null);
        final Predicate<URI> filter = Predicates.not(Predicates.in(ImmutableSet.<URI>of(
                NIF.BEGIN_INDEX, NIF.END_INDEX, KS.MENTION_OF)));
        final String linkTemplate = "ui?action=entity-mentions&entity="
                + UrlEscapers.urlFormParameterEscaper().escape(entityID.stringValue())
                + "&property=${property}&value=${value}";
        model.put("propertyValuesTable", RenderUtils.renderRecordsAggregateTable(
                new StringBuilder(), mentions, filter, linkTemplate, null));
    }

    private void uiReportMentionValueOccurrences(final Map<String, Object> model,
            final URI entityID, @Nullable final URI property) throws Throwable {

        // Do nothing in case the entity ID is missing
        if (entityID == null || property == null) {
            return;
        }

        // Compute the # of occurrences of all the values of the given property in entity mentions
        final Multiset<Value> propertyValues = HashMultiset.create();
        for (final Record mention : getEntityMentions(entityID, Integer.MAX_VALUE, null)) {
            propertyValues.addAll(mention.get(property, Value.class));
        }

        // Render the table
        final Escaper esc = UrlEscapers.urlFormParameterEscaper();
        final String linkTemplate = "ui?action=entity-mentions&entity="
                + esc.escape(entityID.stringValue()) + "&property="
                + esc.escape(Data.toString(property, Data.getNamespaceMap()))
                + "&value=${element}";
        model.put("valueOccurrencesTable", RenderUtils.renderMultisetTable(new StringBuilder(),
                propertyValues, "Property value", "# Mentions", linkTemplate));
    }

    private void uiReportMentionPropertyOccurrences(final Map<String, Object> model,
            final URI entityID) throws Throwable {

        // Do nothing in case the entity ID is missing
        if (entityID == null) {
            return;
        }

        // Compute the # of occurrences of each property URI in entity mentions
        final Multiset<URI> propertyURIs = HashMultiset.create();
        for (final Record mention : getEntityMentions(entityID, Integer.MAX_VALUE, null)) {
            propertyURIs.addAll(mention.getProperties());
        }

        // Render the table
        final Escaper esc = UrlEscapers.urlFormParameterEscaper();
        final String linkTemplate = "ui?action=entity-mentions&entity="
                + esc.escape(entityID.stringValue()) + "&property=${element}";
        model.put("propertyOccurrencesTable", RenderUtils.renderMultisetTable(new StringBuilder(),
                propertyURIs, "Property", "# Mentions", linkTemplate));
    }

    // DATA ACCESS METHODS

    @Nullable
    private Record getRecord(final URI layer, @Nullable final URI id) throws Throwable {
        final Record record = id == null ? null : getSession().retrieve(layer).ids(id).exec()
                .getUnique();
        if (record != null && layer.equals(KS.MENTION)) {
            final String template = "SELECT ?e WHERE { ?e $$ $$ "
                    + (getUIConfig().isDenotedByAllowsGraphs() ? ""
                            : "FILTER NOT EXISTS { GRAPH ?e { ?s ?p ?o } } ") + "}";
            for (final URI entityID : getSession()
                    .sparql(template, getUIConfig().getDenotedByProperty(), id).execTuples()
                    .transform(URI.class, true, "e")) {
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

        for (final List<URI> ids : Stream.create(mentionIDs).chunk(128)) {
            final StringBuilder builder = new StringBuilder();
            builder.append("SELECT ?m ?e WHERE { ?e ");
            builder.append(Data.toString(getUIConfig().getDenotedByProperty(), null));
            builder.append(" ?m VALUES ?m {");
            for (final URI mentionID : ids) {
                builder.append(' ').append(Data.toString(mentionID, null));
            }
            builder.append(" } ");
            if (!getUIConfig().isDenotedByAllowsGraphs()) {
                builder.append("FILTER NOT EXISTS { GRAPH ?e { ?s ?p ?o } } ");
            }
            builder.append("}");
            for (final BindingSet bindings : getSession().sparql(builder.toString()).execTuples()) {
                final URI mentionID = (URI) bindings.getValue("m");
                final URI entityID = (URI) bindings.getValue("e");
                mentions.get(mentionID).add(KS.REFERS_TO, entityID);
                entityIDs.add(entityID);
            }
        }

        // FOLLOWING CODE CAN INCREASE THE NUMBER OF MENTIONS RETRIEVED, BUT THE QUERY USED MAY
        // TAKE UP TO SOME HUNDRED OF SECONDS (AND USING A TIMEOUT PRODUCES A NON-DETERMINISTIC
        // OUTPUT
        // if (!entityIDs.isEmpty()) {
        // builder = new StringBuilder();
        // builder.append("SELECT ?m ?e WHERE { "
        // + "VALUES ?p { sem:hasActor sem:hasTime sem:hasPlace } VALUES ?e0 {");
        // for (final URI entityID : entityIDs) {
        // builder.append(' ').append(Data.toString(entityID, null));
        // }
        // builder.append(" } ?e0 ?p ?e . ?e $$ ?m FILTER(STRSTARTS(STR(?m), $$)) }");
        // for (final BindingSet bindings : getSession()
        // .sparql(builder.toString(), getUIConfig().getDenotedByProperty(),
        // resourceID.stringValue()).timeout(1000L).execTuples()) {
        // final URI mentionID = (URI) bindings.getValue("m");
        // final URI entityID = (URI) bindings.getValue("e");
        // Record mention = mentions.get(mentionID);
        // if (mention == null) {
        // mention = Record.create(mentionID, KS.MENTION);
        // final Matcher matcher = NIF_OFFSET_PATTERN.matcher(mentionID.stringValue());
        // if (matcher.find()) {
        // mention.set(NIF.BEGIN_INDEX, Integer.parseInt(matcher.group(1)));
        // mention.set(NIF.END_INDEX, Integer.parseInt(matcher.group(2)));
        // }
        // mentions.put(mentionID, mention);
        // }
        // mention.add(KS.REFERS_TO, entityID);
        // }
        // }

        final List<Record> sortedMentions = Lists.newArrayList(mentions.values());
        Collections.sort(sortedMentions, new Comparator<Record>() {

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

    private Stream<Record> getEntityMentions(final URI entityID, final int maxResults,
            @Nullable final int[] numMentions) throws Throwable {

        // First retrieve all the URIs of the mentions denoting the entity, via SPARQL query
        final List<URI> mentionURIs = getSession()
                .sparql("SELECT ?m WHERE { $$ $$ ?m}", entityID,
                        getUIConfig().getDenotedByProperty()).execTuples()
                .transform(URI.class, true, "m").toList();

        // Return the total number of mentions, if an holder variable has been supplied
        if (numMentions != null) {
            numMentions[0] = mentionURIs.size();
        }

        // Then return a stream that returns the mention records as they are fetched from the KS
        return getSession().retrieve(KS.MENTION).limit((long) maxResults).ids(mentionURIs).exec();
    }

    private List<Record> getEntityResources(final URI entityID, final int maxResults)
            throws Throwable {

        // Retrieve up to maxResults IDs of resources mentioning the entity
        final Multiset<URI> resourceIDs = HashMultiset.create();
        try (Stream<URI> stream = getSession()
                .sparql("SELECT ?m WHERE { $$ $$ ?m }", entityID,
                        getUIConfig().getDenotedByProperty()).execTuples()
                .transform(URI.class, true, "m")) {
            for (final URI mentionID : stream) {
                final String string = mentionID.stringValue();
                final int index = string.indexOf("#");
                if (index > 0) {
                    final URI resourceID = Data.getValueFactory().createURI(
                            string.substring(0, index));
                    if (resourceIDs.elementSet().size() == maxResults
                            && !resourceIDs.contains(resourceID)) {
                        break;
                    }
                    resourceIDs.add(resourceID);
                }
            }
        }

        // Lookup the resources in the KS
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
