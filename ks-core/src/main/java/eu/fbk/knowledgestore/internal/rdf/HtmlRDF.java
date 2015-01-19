package eu.fbk.knowledgestore.internal.rdf;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.helpers.RDFWriterBase;

public class HtmlRDF implements RDFWriterFactory {

    /** RDFFormat constant for the Turtle Quads (TQL) format). */
    public static final RDFFormat FORMAT = new RDFFormat("RDFHTML", "text/html", Charsets.UTF_8,
            "html", true, false);

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
    public RDFWriter getWriter(final OutputStream out) {
        return getWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    }

    @Override
    public RDFWriter getWriter(final Writer writer) {
        Preconditions.checkNotNull(writer);
        return new HTMLWriter(writer);
    }

    private static class HTMLWriter extends RDFWriterBase {

        private final Writer writer;

        private final Map<String, String> prefixes;

        HTMLWriter(final Writer writer) {
            this.writer = writer;
            this.prefixes = Maps.newLinkedHashMap();
        }

        @Override
        public RDFFormat getRDFFormat() {
            return FORMAT;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            try {
                this.writer.write("<html>\n<head>\n" //
                        + "<meta http-equiv=\"Content-type\" " //
                        + "content=\"text/html;charset=UTF-8\"/>\n" //
                        + "</head>\n<body>\n" //
                        + "<table class=\"rdf\">\n<thead>\n" //
                        + "<tr><th>Subject</th><th>Predicate</th><th>Object</th></tr>\n" //
                        + "</thead>\n<tbody>\n");
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // ignore
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.prefixes.put(uri, prefix);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            try {
                this.writer.write("<tr><td>");
                RDFUtil.toHtml(statement.getSubject(), this.prefixes, this.writer);
                this.writer.write("</td><td>");
                RDFUtil.toHtml(statement.getPredicate(), this.prefixes, this.writer);
                this.writer.write("</td><td>");
                RDFUtil.toHtml(statement.getObject(), this.prefixes, this.writer);
                this.writer.write("</td></tr>\n");
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.writer.write("</tbody>\n</table>\n</html>\n");
                this.writer.flush();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

    }

}
