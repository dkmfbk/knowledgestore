package eu.fbk.knowledgestore.triplestore;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.sail.NotifyingSail;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionWrapper;
import org.openrdf.sail.helpers.NotifyingSailWrapper;

import info.aduna.iteration.CloseableIteration;

public class SPARQLRendererSail extends NotifyingSailWrapper {

    public SPARQLRendererSail(final NotifyingSail sail) {
        super(sail);
    }

    @Override
    public NotifyingSailConnection getConnection() throws SailException {
        return new SparqlRendererConnection(super.getConnection());
    }

    private static class SparqlRendererConnection extends NotifyingSailConnectionWrapper {

        public SparqlRendererConnection(final NotifyingSailConnection connection) {
            super(connection);
        }

        @Override
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings,
                final boolean includeInferred) throws SailException {
            try {
                System.out.println(tupleExpr);
                System.out.println(dataset);

                final SPARQLRenderer renderer = new SPARQLRenderer(null, false);
                final String string = renderer.render(tupleExpr, dataset);

                // ParsedQuery q;
                // if (string1.contains("SELECT")) {
                // q = new ParsedTupleQuery(tupleExpr);
                // } else if (string1.contains("ASK")) {
                // q = new ParsedBooleanQuery(tupleExpr);
                // } else {
                // q = new ParsedGraphQuery(tupleExpr);
                // }
                // q.setDataset(dataset);
                // final String string = new SPARQLQueryRenderer().render(q);

                System.out.println(string);
                final ParsedQuery parsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL,
                        string, null);
                final TupleExpr newTupleExpr = parsedQuery.getTupleExpr();
                final Dataset newDataset = parsedQuery.getDataset();
                return super.evaluate(newTupleExpr, newDataset, bindings, includeInferred);

            } catch (final Throwable ex) {
                throw new SailException(ex);
            }
        }

    }

}
