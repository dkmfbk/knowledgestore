package eu.fbk.knowledgestore.internal.rdf;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.BooleanQueryResultWriterFactory;
import org.openrdf.query.resultio.QueryResultFormat;
import org.openrdf.query.resultio.QueryResultWriterBase;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;

public class HtmlSparql implements BooleanQueryResultWriterFactory, TupleQueryResultWriterFactory {

    public static final TupleQueryResultFormat TUPLE_FORMAT = new TupleQueryResultFormat(
            "HTML/TUPLE", "text/html", Charsets.UTF_8, "html");

    public static final BooleanQueryResultFormat BOOLEAN_FORMAT = new BooleanQueryResultFormat(
            "HTML/BOOLEAN", "text/html", Charsets.UTF_8, "html");

    static {
        TupleQueryResultFormat.register(TUPLE_FORMAT);
        BooleanQueryResultFormat.register(BOOLEAN_FORMAT);
    }

    public static void register() {
        // calling this method will cause the static initializer to run once
    }

    @Override
    public TupleQueryResultFormat getTupleQueryResultFormat() {
        return TUPLE_FORMAT;
    }

    @Override
    public BooleanQueryResultFormat getBooleanQueryResultFormat() {
        return BOOLEAN_FORMAT;
    }

    @Override
    public HtmlWriter getWriter(final OutputStream out) {
        return new HtmlWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    }

    private static final class HtmlWriter extends QueryResultWriterBase implements
            BooleanQueryResultWriter, TupleQueryResultWriter {

        private final Writer writer;

        private final Map<String, String> prefixes;

        private List<String> variables;

        HtmlWriter(final Writer writer) {
            this.writer = writer;
            this.prefixes = Maps.newHashMap();
            this.variables = null;
        }

        @Override
        public QueryResultFormat getQueryResultFormat() {
            return TUPLE_FORMAT;
        }

        @Override
        public TupleQueryResultFormat getTupleQueryResultFormat() {
            return TUPLE_FORMAT;
        }

        @Override
        public BooleanQueryResultFormat getBooleanQueryResultFormat() {
            return BOOLEAN_FORMAT;
        }

        @Override
        public void handleNamespace(final String prefix, final String uri) {
            this.prefixes.put(uri, prefix);
        }

        @Override
        public void startDocument() throws QueryResultHandlerException {
            try {
                this.writer.append("<html>\n<head>\n<meta http-equiv=\"Content-type\" "
                        + "content=\"text/html;charset=UTF-8\"/>\n");
            } catch (final IOException ex) {
                throw new QueryResultHandlerException(ex);
            }
        }

        @Override
        public void handleStylesheet(final String stylesheetUrl) {
        }

        @Override
        public void startHeader() {
        }

        @Override
        public void handleLinks(final List<String> linkURLs) throws QueryResultHandlerException {
            try {
                for (final String linkURL : linkURLs) {
                    this.writer.append("<link rel=\"nofollow\" href=\"" + linkURL + "\">\n");
                }
            } catch (final IOException ex) {
                throw new QueryResultHandlerException(ex);
            }
        }

        @Override
        public void endHeader() {
        }

        @Override
        public void write(final boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleBoolean(final boolean value) throws QueryResultHandlerException {
            try {
                this.writer.append("</head>\n<body>\n"//
                        + "<table class=\"sparql\">\n<thead>\n" //
                        + "<tr><th>boolean</th></tr>\n" //
                        + "</thead>\n<tbody>\n" //
                        + "<tr><td>");
                RDFUtil.toHtml(value ? BooleanLiteralImpl.TRUE : BooleanLiteralImpl.FALSE,
                        this.prefixes, this.writer);
                this.writer.append("</td></tr>\n" //
                        + "</tbody>\n</table>\n" //
                        + "</body>\n</html>\n");
                this.writer.flush();
            } catch (final IOException ex) {
                throw new TupleQueryResultHandlerException(ex);
            }
        }

        @Override
        public void startQueryResult(final List<String> variables)
                throws TupleQueryResultHandlerException {
            try {
                this.variables = ImmutableList.copyOf(variables);
                this.writer.append("</head>\n<body>\n" //
                        + "<table class=\"sparql\">\n<thead>\n<tr>");
                for (final String variable : variables) {
                    this.writer.append("<th>").append(variable).append("</th>");
                }
                this.writer.append("</tr>\n</thead>\n<tbody>\n");
            } catch (final IOException ex) {
                throw new TupleQueryResultHandlerException(ex);
            }
        }

        @Override
        public void handleSolution(final BindingSet bindings)
                throws TupleQueryResultHandlerException {
            try {
                this.writer.append("<tr>");
                for (final String variable : this.variables) {
                    this.writer.append("<td>");
                    RDFUtil.toHtml(bindings.getValue(variable), this.prefixes, this.writer);
                    this.writer.append("</td>");
                }
                this.writer.append("</tr>\n");
            } catch (final IOException ex) {
                throw new TupleQueryResultHandlerException(ex);
            }
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            try {
                this.writer.append("</tbody>\n</table>\n" //
                        + "</body>\n</html>\n");
                this.writer.flush();
            } catch (final IOException ex) {
                throw new TupleQueryResultHandlerException(ex);
            }
        }

    }

}
