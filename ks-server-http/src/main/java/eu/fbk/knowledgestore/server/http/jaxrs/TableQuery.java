package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.vocabulary.KS;

public class TableQuery {

    private static final int MAX_MENTIONS = 10000;

    private final Session session;

    public TableQuery(final Session session) {
        this.session = session;
    }

    public void renderPropertyOccurrencesTable(final Appendable out, final URI entityURI)
            throws Throwable {

        // Compute the # of occurrences of each property URI in entity mentions
        final Multiset<URI> propertyURIs = HashMultiset.create();
        for (final Record mention : getEntityMentions(entityURI, MAX_MENTIONS)) {
            propertyURIs.addAll(mention.getProperties());
        }

        // Render the table
        renderMultisetTable(out, propertyURIs);
    }

    public void renderValueOccurrencesTable(final Appendable out, final URI entityURI,
            final URI propertyURI) throws Throwable {

        // Compute the # of occurrences of all the values of the given property in entity mentions
        final Multiset<Value> propertyValues = HashMultiset.create();
        for (final Record mention : getEntityMentions(entityURI, MAX_MENTIONS)) {
            propertyValues.addAll(mention.get(propertyURI, Value.class));
        }

        // Render the table
        renderMultisetTable(out, propertyValues);
    }

    public void renderMentionTable(final URI entity, final URI property, final String value,
            final Appendable out) throws Throwable {

        // Retrieve all the mentions satisfying the property[=value] optional filter
        final List<Record> mentions = Lists.newArrayList();
        for (final Record mention : getEntityMentions(entity, MAX_MENTIONS)) {
            if (property == null || !mention.isNull(property)
                    && (value == null || mention.get(property).contains(value))) {
                mentions.add(mention);
            }
        }

        // Render the mention table, including column toggling functionality
        renderRecordsTable(out, mentions, null, true);
    }

    public Stream<Record> getEntityMentions(final URI entityURI, final int maxResults)
            throws Throwable {

        // First retrieve all the URIs of the mentions denoting the entity, via SPARQL query
        final List<URI> mentionURIs = this.session
                .sparql("SELECT ?mention WHERE { $$ gaf:denotedBy ?mention}", entityURI)
                .execTuples().transform(URI.class, true, "mention").toList();

        // Then return a stream that returns the mention records as they are fetched from the KS
        return this.session.retrieve(KS.MENTION).limit((long) maxResults).ids(mentionURIs).exec();
    }

    public static <T extends Appendable> T renderMultisetTable(final T out,
            final Multiset<?> multiset) throws IOException {

        out.append("<table class=\"display datatable\">\n");
        out.append("<thead>\n<tr><th>Values</th><th>Occurrences</th></tr>\n</thead>\n");
        out.append("<tbody>\n");
        for (final Object element : multiset.elementSet()) {
            final int occurrences = multiset.count(element);
            out.append("<tr><td>").append(RenderUtils.render(element)).append("</td><td>")
                    .append(Integer.toString(occurrences)).append("</td></tr>\n");
        }
        out.append("</tbody>\n</table>\n");
        return out;
    }

    public static <T extends Appendable> T renderRecordsTable(final T out,
            final Iterable<Record> records, @Nullable List<URI> propertyURIs,
            final boolean toggleColumns) throws IOException {

        // Extract the properties to show if not explicitly supplied
        if (propertyURIs == null) {
            final Set<URI> uriSet = Sets.newHashSet();
            for (final Record record : records) {
                uriSet.addAll(record.getProperties());
            }
            propertyURIs = Ordering.from(Data.getTotalComparator()).sortedCopy(propertyURIs);
        }

        // Emit the panel for toggling displayed columns
        out.append("<div>Toggle column: | <a class=\"toggle-vis\" data-column=\"0\">mentionURI</a> |");
        for (int i = 0; i < propertyURIs.size(); ++i) {
            final String qname = RenderUtils.shortenURI(propertyURIs.get(i));
            out.append(" <a class=\"toggle-vis\" data-column=\"").append(Integer.toString(i + 1))
                    .append("\">").append(qname).append("</a> | ");
        }
        out.append("</div>");

        // Emit the table
        out.append("<table class=\"display datatable\">\n<thead>\n<tr><th>URI</th>");
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
        return out;
    }

}
