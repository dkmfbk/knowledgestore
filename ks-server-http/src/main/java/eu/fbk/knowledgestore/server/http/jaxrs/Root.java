package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collections;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

    private static final String DESCRIBE_QUERY = "" //
            + "SELECT\n" //
            + "  (COALESCE(?s, $$) AS ?subject)\n" //
            + "  ?predicate\n" //
            + "  (COALESCE(?o, $$) AS ?object)\n" //
            + "  ?graph\n" //
            + "WHERE {\n" //
            + "  { GRAPH ?graph { $$ ?predicate ?o }  } UNION\n" //
            + "  { GRAPH ?graph { ?s ?predicate $$ }  }\n" //
            // + "  FILTER (?predicate != <http://groundedannotationframework.org/denotedBy>)\n"
            + "}\n" //
            + "LIMIT $$";

    private static final String GRAPH_QUERY = "" //
            + "SELECT ?subject ?predicate ?object\n" //
            + "WHERE {\n" //
            + "  GRAPH $$ { ?subject ?predicate ?object }\n" //
            + "}\n" //
            + "LIMIT $$";

    private static final int MAX_FETCHED_RESULTS = 10000;

    private static final boolean CHAR_OFFSET_HACK = Boolean.parseBoolean(System.getProperty(
            "ks.charOffsetHack", "false"));

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
                view = "/lookup";
                model.put("tabLookup", Boolean.TRUE);
                uiLookup(model, id, selection);

            } else if ("sparql".equals(action)) {
                final String query = Strings.emptyToNull(parameters.getFirst("query"));
                Long timeout = null;
                try {
                    timeout = 1000 * Long.valueOf(parameters.getFirst("timeout"));
                } catch (final Throwable ex) {
                    // ignore
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

        final long ts = System.currentTimeMillis();
        final StringBuilder resultsMessage = new StringBuilder();

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

        if (query == null) {
            return;
        }

        model.put("query", query);

        final String form = RDFUtil.detectSparqlForm(query);
        if (form.equalsIgnoreCase("select")) {
            final Stream<BindingSet> stream = getSession().sparql(query).timeout(timeout)
                    .execTuples();
            this.closeOnCompletion(stream);
            @SuppressWarnings("unchecked")
            final List<String> variables = stream.getProperty("variables", List.class);
            final Iterator<BindingSet> iterator = stream.iterator();
            final List<BindingSet> solutions = ImmutableList.copyOf(Iterators.limit(iterator,
                    MAX_FETCHED_RESULTS + 1));
            if (solutions.size() <= MAX_FETCHED_RESULTS) {
                resultsMessage.append(solutions.size());
                model.put("results", render(variables, solutions));
            } else {
                resultsMessage.append("more than ").append(MAX_FETCHED_RESULTS);
                model.put("results",
                        render(variables, Iterables.concat(solutions, Stream.create(iterator))));
            }
        } else if (form.equalsIgnoreCase("construct") || form.equals("describe")) {
            final Stream<Statement> stream = getSession().sparql(query).timeout(timeout)
                    .execTriples();
            this.closeOnCompletion(stream);
            final Iterator<Statement> iterator = stream.iterator();
            final List<Statement> statements = ImmutableList.copyOf(Iterators.limit(iterator,
                    MAX_FETCHED_RESULTS + 1));
            if (statements.size() < MAX_FETCHED_RESULTS) {
                resultsMessage.append(statements.size());
                model.put("results", render(statements));
            } else {
                resultsMessage.append("more than ").append(MAX_FETCHED_RESULTS);
                model.put("results", render(Iterables.concat(statements, Stream.create(iterator))));
            }
        } else {
            final boolean result = getSession().sparql(query).timeout(timeout).execBoolean();
            final List<String> variables = ImmutableList.of("result");
            final List<BindingSet> solutions = ImmutableList.<BindingSet>of(new ListBindingSet(
                    variables, result ? BooleanLiteralImpl.TRUE : BooleanLiteralImpl.FALSE));
            resultsMessage.append(1);
            model.put("results", render(variables, solutions));
        }

        final long elapsed = System.currentTimeMillis() - ts;
        resultsMessage.append(" results in ").append(elapsed).append(" ms");
        if (timeout != null && elapsed > timeout) {
            resultsMessage.append(" (timed out, more results may be available)");
        }

        model.put("resultsMessage", resultsMessage.toString());
        model.put("hasResults", true);
    }

    private void uiLookup(final Map<String, Object> model, @Nullable final URI id,
            @Nullable final URI selection) throws Throwable {

        if (!getUIConfig().getLookupExamples().isEmpty()) {
            model.put("examplesCount", getUIConfig().getLookupExamples().size());
            model.put("examples", getUIConfig().getLookupExamples());
        }

        if (id == null) {
            return;
        }

        model.put("id", id);

        final Record resource = getSession().retrieve(KS.RESOURCE).ids(id).exec().getUnique();
        if (resource != null) {
            uiLookupResource(model, resource, selection);
        } else {
            final Record mention = getSession().retrieve(KS.MENTION).ids(id).exec().getUnique();
            if (mention != null) {
                uiLookupMention(model, mention);
            } else {
                final List<BindingSet> descriptionTriples = getSession()
                        .sparql(DESCRIBE_QUERY, id, id, id, id, getUIConfig().getResultLimit())
                        .execTuples().toList();
                final List<BindingSet> graphTriples = getSession()
                        .sparql(GRAPH_QUERY, id, getUIConfig().getResultLimit()).execTuples()
                        .toList();
                if (!descriptionTriples.isEmpty()) {
                    uiLookupEntity(model, id, descriptionTriples, graphTriples);
                } else {
                    model.put("text", "NO ENTRY FOR ID " + id);
                }
            }
        }
    }

    private void uiLookupResource(final Map<String, Object> model, final Record resource,
            final URI selection) throws Throwable {

        // Set object type
        model.put("resource", Boolean.TRUE);

        // Retrieve mentions and entities
        final List<Record> mentions = getSession().retrieve(KS.MENTION)
                .ids(resource.get(KS.HAS_MENTION, URI.class)).exec()
                .toSortedList(Integer.class, false, NIF.BEGIN_INDEX);
        final Set<URI> entityIDs = mentionsToEntities(mentions);

        // Generate mentions and entities dropdown lists
        final List<String> mentionLinks = Lists.newArrayList();
        final List<String> entityLinks = Lists.newArrayList();
        for (final Record mention : mentions) {
            mentionLinks.add("<a onclick=\"select('" + mention.getID() + "')\" href=\"#\">"
                    + shorten(mention.getID()) + "</a>");
        }
        for (final URI entityID : entityIDs) {
            entityLinks.add("<a onclick=\"select('" + entityID + "')\" href=\"#\">"
                    + shorten(entityID) + "</a>");
        }
        Collections.sort(entityLinks);

        // Generate mentions dropdown list, if there is at least a mention
        if (!mentionLinks.isEmpty()) {
            model.put("resourceMentionsCount", mentionLinks.size());
            model.put("resourceMentions", mentionLinks);
        }

        // Generate entities dropdown list, if there is at least a mention
        if (!entityLinks.isEmpty()) {
            model.put("resourceEntitiesCount", entityLinks.size());
            model.put("resourceEntities", entityLinks);
        }

        // Setup details panel and build set of selected mention IDs
        final Set<URI> selectedMentionIDs = Sets.newHashSet();
        final String titleKey = "resourceDetailsTitle";
        final String bodyKey = "resourceDetailsBody";
        if (selection != null) {
            final Record mention = getSession().retrieve(KS.MENTION).ids(selection).exec()
                    .getUnique();
            if (mention != null) {
                selectedMentionIDs.add(mention.getID());
                final Set<URI> entityURIs = mentionToEntities(mention);
                model.put(bodyKey, render(mention));
                final StringBuilder builder = new StringBuilder("<strong>Mention ");
                render(builder, selection);
                builder.append("</strong>");
                if (!entityURIs.isEmpty()) {
                    builder.append("&nbsp;&nbsp;&#10143;&nbsp;&nbsp;");
                    builder.append(entityURIs.size() == 1 ? "<strong>Entity</strong>"
                            : "<strong>Entities</strong>");
                    for (final URI entityURI : entityURIs) {
                        builder.append("&nbsp;&nbsp;<strong>");
                        render(builder, entityURI);
                        builder.append("</strong>");
                        boolean mentionedInText = false;
                        for (final Record m : mentions) {
                            if (m.get(KS.REFERS_TO, URI.class).contains(entityURI)) {
                                mentionedInText = true;
                                break;
                            }
                        }
                        if (mentionedInText) {
                            builder.append(" <a href=\"#\" onclick=\"select('")
                                    .append(escapeHtml(entityURI)).append("')\">(select)</a>");
                        }
                    }
                }
                model.put(titleKey, builder.toString());
            } else {
                final List<BindingSet> bindings = getSession()
                        .sparql(DESCRIBE_QUERY, selection, selection, selection, selection,
                                getUIConfig().getResultLimit()).execTuples().toList();
                if (!bindings.isEmpty()) {
                    for (final Record entityMention : entityToMentions(selection)) {
                        selectedMentionIDs.add(entityMention.getID());
                    }
                    model.put(bodyKey, render(new StringBuilder(), DESCRIBE_VARS, bindings));
                    model.put(titleKey, "<strong>Entity " + render(new StringBuilder(), selection)
                            + "</strong> (max " + getUIConfig().getResultLimit() + " triples)");
                }
            }
        }
        if (!model.containsKey(titleKey)) {
            model.put(titleKey, "<strong>Resource metadata</strong>");
            model.put(bodyKey, render(resource));
        }

        // Setup resource text panel
        final Representation representation = getSession().download(resource.getID()).exec();
        if (representation != null) {
            final String text = representation.writeToString();
            final StringBuilder builder = new StringBuilder();
            if (!mentions.isEmpty()) {
                render(builder, text, mentions, selectedMentionIDs, true, false);
            } else {
                final Record metadata = representation.getMetadata();
                model.put("resourcePrettyPrint", Boolean.TRUE);
                render(builder, text, metadata.getUnique(NIE.MIME_TYPE, String.class));
            }
            model.put("resourceText", builder.toString());
        }
    }

    private void uiLookupMention(final Map<String, Object> model, final Record mention)
            throws Throwable {

        final Set<URI> entityIDs = mentionToEntities(mention);
        final URI resourceID = mention.getUnique(KS.MENTION_OF, URI.class, null);

        model.put("mention", Boolean.TRUE);
        model.put("mentionData", render(new StringBuilder(), mention).toString());

        if (resourceID != null) {
            model.put("mentionResourceLink",
                    render(new StringBuilder(), resourceID, mention.getID()));
            final Representation representation = getSession().download(resourceID).exec();
            if (representation == null) {
                model.put("mentionResourceExcerpt", "RESOURCE CONTENT NOT AVAILABLE");
            } else {
                final String text = representation.writeToString();
                model.put("mentionResourceExcerpt", render(new StringBuilder(), text, //
                        ImmutableList.of(mention), ImmutableSet.<URI>of(), false, true).toString());
            }
        }

        if (!entityIDs.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            for (final URI entityID : entityIDs) {
                builder.append(builder.length() > 0 ? "&nbsp;&nbsp;" : "");
                render(builder, entityID);
            }
            model.put("mentionEntityLinks", builder.toString());
            final URI entityID = entityIDs.iterator().next();
            final List<BindingSet> bindings = getSession()
                    .sparql(DESCRIBE_QUERY, entityID, entityID, entityID, entityID,
                            getUIConfig().getResultLimit()).execTuples().toList();
            if (entityIDs.size() == 1) {
                model.put("mentionEntityLink", render(new StringBuilder(), entityID));
            }
            model.put("mentionEntityTriplesCount", bindings.size());
            model.put("mentionEntityTriples", render(new StringBuilder(), DESCRIBE_VARS, bindings));
        }
    }

    private void uiLookupEntity(final Map<String, Object> model, final URI id,
            final List<BindingSet> descriptionTriples, final List<BindingSet> graphTriples)
            throws Throwable {

        model.put("entity", Boolean.TRUE);

        if (!descriptionTriples.isEmpty()) {
            model.put("entityTriplesCount", descriptionTriples.size());
            model.put("entityTriples", render(new StringBuilder(), //
                    ImmutableList.of("subject", "predicate", "object", "graph"), //
                    descriptionTriples).toString());
        }

        if (!graphTriples.isEmpty()) {
            model.put("entityGraphCount", graphTriples.size());
            model.put("entityGraph", render(new StringBuilder(), //
                    ImmutableList.of("subject", "predicate", "object"), graphTriples).toString());
        }

        final List<Record> resources = entityToResources(id);
        if (resources.isEmpty()) {
            return;
        }

        final List<URI> overviewProperties = getUIConfig().getResourceOverviewProperties();
        final StringBuilder builder = new StringBuilder();
        final int width = 75 / (overviewProperties.size() + 2);
        final String th = "<th style=\"width: " + width + "%\">";
        builder.append("<table class=\"sparql table table-condensed tablesorter\"><thead>\n");
        builder.append("<tr>").append(th).append("resource ID</th>");
        for (final URI property : overviewProperties) {
            builder.append(th).append(escapeHtml(format(property))).append("</th>");
        }
        builder.append(th).append("# mentions</th>");
        builder.append("</tr>\n</thead><tbody>\n");
        for (final Record resource : resources) {
            builder.append("<tr><td>");
            render(builder, resource.getID(), id);
            for (final URI property : overviewProperties) {
                builder.append("</td><td>");
                render(builder, resource.get(property));
            }
            builder.append("</td><td>");
            render(builder, resource.getUnique(NUM_MENTIONS, Integer.class, null));
            builder.append("</td></tr>\n");
        }
        builder.append("</tbody></table>");

        model.put("entityResourcesCount", resources.size());
        model.put("entityResources", builder.toString());
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

    private Iterable<String> render(final Iterable<? extends Statement> statements) {
        final Iterable<String> header = ImmutableList.of(""
                + "<table class=\"rdf table table-condensed tablesorter\"><thead>\n"
                + "<tr><th style=\"width: 30%\">subject</th>"
                + "<th style=\"width: 30%\">predicate</th>"
                + "<th style=\"width: 30%\">object</th></tr>\n</thead><tbody>\n");
        final Iterable<String> footer = ImmutableList.of("</tbody></table>");
        final Function<Statement, String> renderer = new Function<Statement, String>() {

            @Override
            public String apply(final Statement statement) {
                if (Thread.interrupted()) {
                    throw new IllegalStateException("Interrupted");
                }
                final StringBuilder builder = new StringBuilder();
                builder.append("<tr><td>");
                render(builder, statement.getSubject());
                builder.append("</td><td>");
                render(builder, statement.getPredicate());
                builder.append("</td><td>");
                render(builder, statement.getObject());
                builder.append("</td></tr>\n");
                return builder.toString();
            }

        };
        return Iterables.concat(header, Iterables.transform(statements, renderer), footer);
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
            final List<Record> mentions, final Set<URI> selection, final boolean canSelect,
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
                final Object refersTo = mention.getUnique(KS.REFERS_TO);
                final boolean selected = selection.contains(mention.getID()) || refersTo != null
                        && selection.contains(refersTo);
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

    // private String escapeParam(final Object object) {
    // return object == null ? null : UrlEscapers.urlFormParameterEscaper().escape(
    // object.toString());
    // }

    private String escapeHtml(final Object object) {
        return object == null ? null : HtmlEscapers.htmlEscaper().escape(object.toString());
    }

    private Set<URI> mentionToEntities(final Record mention) throws Throwable {
        Set<URI> entityIDs = ImmutableSet.copyOf(mention.get(KS.REFERS_TO, URI.class));
        if (entityIDs.isEmpty()) {
            entityIDs = getSession()
                    .sparql("SELECT ?e WHERE { ?e $$ $$ }", getUIConfig().getDenotedByProperty(),
                            mention.getID()).execTuples().transform(URI.class, true, "e").toSet();
        }
        if (getUIConfig().isRefersToFunctional() && entityIDs.size() > 1) {
            throw new IllegalArgumentException("Multiple entities associated to mention "
                    + mention.getID() + ": " + Joiner.on(", ").join(entityIDs));
        }
        return entityIDs;
    }

    private Set<URI> mentionsToEntities(final Iterable<Record> mentions) throws Throwable {
        final Set<URI> entityIDs = Sets.newHashSet();
        final List<URI> unboundMentionIDs = Lists.newArrayList();
        for (final Record mention : mentions) {
            final URI entityID = mention.getUnique(KS.REFERS_TO, URI.class, null);
            if (entityID != null) {
                entityIDs.add(entityID);
            } else {
                unboundMentionIDs.add(mention.getID());
            }
        }
        if (!unboundMentionIDs.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            builder.append("SELECT ?e WHERE { ?e ");
            builder.append(Data.toString(getUIConfig().getDenotedByProperty(), null));
            builder.append(" ?m VALUES ?m {");
            for (final URI mentionID : unboundMentionIDs) {
                builder.append(' ').append(Data.toString(mentionID, null));
            }
            builder.append(" } }");
            getSession().sparql(builder.toString()).execTuples().transform(URI.class, true, "e")
                    .toCollection(entityIDs);
        }
        return entityIDs;
    }

    private List<Record> entityToMentions(final URI entityID) throws Throwable {
        final List<URI> mentionIDs = getSession()
                .sparql("SELECT ?m WHERE { $$ $$ ?m } LIMIT $$", entityID,
                        getUIConfig().getDenotedByProperty(), getUIConfig().getResultLimit())
                .execTuples().transform(URI.class, true, "m").toList();
        return getSession().retrieve(KS.MENTION).ids(mentionIDs).exec().toList();
    }

    private List<Record> entityToResources(final URI entityID) throws Throwable {
        final List<Record> mentions = entityToMentions(entityID);

        final Multiset<URI> resourceIDs = HashMultiset.create();
        for (final Record mention : mentions) {
            final URI resourceID = mention.getUnique(KS.MENTION_OF, URI.class, null);
            if (resourceID != null) {
                resourceIDs.add(resourceID);
            }
        }

        final List<Record> resources = getSession().retrieve(KS.RESOURCE).ids(resourceIDs).exec()
                .toList();
        for (final Record resource : resources) {
            resource.set(NUM_MENTIONS, resourceIDs.count(resource.getID()));
        }
        return resources;
    }

}
