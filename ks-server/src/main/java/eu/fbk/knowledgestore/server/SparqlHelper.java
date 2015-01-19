package eu.fbk.knowledgestore.server;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.IncompatibleOperationException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.DescribeOperator;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ZeroLengthPath;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.iterator.DescribeIteration;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.BlankNodeVarProcessor;
import org.openrdf.query.parser.sparql.DatasetDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.TupleExprBuilder;
import org.openrdf.query.parser.sparql.WildcardProjectionProcessor;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTOperationContainer;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTServiceGraphPattern;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.triplestore.SelectQuery;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;

final class SparqlHelper {

    static ParsedQuery parse(final String queryStr, final String baseURI)
            throws MalformedQueryException {
        try {
            final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryStr);
            StringEscapesProcessor.process(qc);
            BaseDeclProcessor.process(qc, baseURI);
            final Map<String, String> prefixes = parseHelper(qc); // [FC] changed
            WildcardProjectionProcessor.process(qc);
            BlankNodeVarProcessor.process(qc);
            if (qc.containsQuery()) {
                TupleExpr tupleExpr;
                final TupleExprBuilder tupleExprBuilder = new TupleExprBuilder(
                        Data.getValueFactory()); // [FC] changed
                try {
                    tupleExpr = (TupleExpr) qc.jjtAccept(tupleExprBuilder, null);
                } catch (final VisitorException e) {
                    throw new MalformedQueryException(e.getMessage(), e);
                }
                ParsedQuery query;
                final ASTQuery queryNode = qc.getQuery();
                if (queryNode instanceof ASTSelectQuery) {
                    query = new ParsedTupleQuery(queryStr, tupleExpr);
                } else if (queryNode instanceof ASTConstructQuery) {
                    query = new ParsedGraphQuery(queryStr, tupleExpr, prefixes);
                } else if (queryNode instanceof ASTAskQuery) {
                    query = new ParsedBooleanQuery(queryStr, tupleExpr);
                } else if (queryNode instanceof ASTDescribeQuery) {
                    query = new ParsedGraphQuery(queryStr, tupleExpr, prefixes);
                } else {
                    throw new RuntimeException("Unexpected query type: " + queryNode.getClass());
                }
                final Dataset dataset = DatasetDeclProcessor.process(qc);
                if (dataset != null) {
                    query.setDataset(dataset);
                }
                return query;
            } else {
                throw new IncompatibleOperationException(
                        "supplied string is not a query operation");
            }
        } catch (final ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (final TokenMgrError e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
    }

    private static Map<String, String> parseHelper(final ASTOperationContainer qc)
            throws MalformedQueryException {
        final List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();
        final Map<String, String> prefixMap = new LinkedHashMap<String, String>();
        for (final ASTPrefixDecl prefixDecl : prefixDeclList) {
            final String prefix = prefixDecl.getPrefix();
            final String iri = prefixDecl.getIRI().getValue();
            if (prefixMap.containsKey(prefix)) {
                throw new MalformedQueryException("Multiple prefix declarations for prefix '"
                        + prefix + "'");
            }
            prefixMap.put(prefix, iri);
        }
        final QNameProcessor visitor = new QNameProcessor(prefixMap);
        try {
            qc.jjtAccept(visitor, null);
        } catch (final VisitorException e) {
            throw new MalformedQueryException(e);
        }
        return prefixMap;
    }

    static CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
            final TripleTransaction transaction, final TupleExpr expr,
            @Nullable final Dataset dataset, @Nullable final BindingSet bindings,
            @Nullable final Long timeout) throws QueryEvaluationException {

        Preconditions.checkNotNull(transaction);

        final AtomicBoolean delegate = new AtomicBoolean(false);
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final StatementPattern node) throws RuntimeException {
                delegate.set(true);
            }

        });

        final EvaluationStrategy strategy = delegate.get() ? new DelegatingEvaluationStrategy(
                transaction, dataset, timeout, false) : new LocalEvaluationStrategy(transaction,
                dataset, timeout);

        return strategy
                .evaluate(expr, bindings != null ? bindings : EmptyBindingSet.getInstance());
    }

    private static Value skolemize(final Value value) {
        return value instanceof BNode ? Data.getValueFactory().createURI("bnode:",
                ((BNode) value).getID()) : value;
    }

    private static BindingSet skolemize(final BindingSet bindings) {
        final QueryBindingSet result = new QueryBindingSet();
        for (final String name : bindings.getBindingNames()) {
            result.setBinding(name, skolemize(bindings.getValue(name)));
        }
        return result;
    }

    private static CloseableIteration<BindingSet, QueryEvaluationException> skolemize(
            final CloseableIteration<BindingSet, QueryEvaluationException> iter) {
        return new ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException>(iter) {

            @Override
            protected BindingSet convert(final BindingSet bindings)
                    throws QueryEvaluationException {
                return skolemize(bindings);
            }

        };
    }

    private static Value deskolemize(final Value value) {
        if (value instanceof URI) {
            final URI uri = (URI) value;
            if (uri.getNamespace().equals("bnode:")) {
                return Data.getValueFactory().createBNode(uri.getLocalName());
            }
        }
        return value;
    }

    @Nullable
    private static BindingSet deskolemize(@Nullable final BindingSet bindings) {
        final QueryBindingSet result = new QueryBindingSet();
        for (final String name : bindings.getBindingNames()) {
            result.setBinding(name, deskolemize(bindings.getValue(name)));
        }
        return result;
    }

    private static CloseableIteration<BindingSet, QueryEvaluationException> deskolemize(
            final CloseableIteration<BindingSet, QueryEvaluationException> iter) {
        return new ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException>(iter) {

            @Override
            protected BindingSet convert(final BindingSet bindings)
                    throws QueryEvaluationException {
                return deskolemize(bindings);
            }

        };
    }

    private static final class LocalEvaluationStrategy extends EvaluationStrategyImpl {

        private final TripleTransaction transaction;

        @Nullable
        private final Long timeout;

        public LocalEvaluationStrategy(final TripleTransaction transaction, final Dataset dataset,
                @Nullable final Long timeout) {
            super(null, dataset);
            this.transaction = Preconditions.checkNotNull(transaction);
            this.timeout = timeout;
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final DescribeOperator expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return skolemize(new DescribeIteration(//
                    deskolemize(evaluate(expr.getArg(), bindings)), //
                    new DelegatingEvaluationStrategy(this.transaction, this.dataset, this.timeout,
                            true), //
                    expr.getBindingNames(), //
                    bindings));
        }

    }

    private static final class DelegatingEvaluationStrategy extends EvaluationStrategyImpl {

        private final TripleTransaction transaction;

        @Nullable
        private final Long timeout;

        private final boolean skolemize;

        public DelegatingEvaluationStrategy(final TripleTransaction transaction,
                final Dataset dataset, @Nullable final Long timeout, final boolean skolemize) {
            super(null, dataset);
            this.transaction = transaction;
            this.timeout = timeout;
            this.skolemize = skolemize;
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final DescribeOperator expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return skolemize(new DescribeIteration(//
                    deskolemize(evaluate(expr.getArg(), bindings)), //
                    this.skolemize ? this : new DelegatingEvaluationStrategy(this.transaction,
                            this.dataset, this.timeout, true), //
                    expr.getBindingNames(), //
                    bindings));
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final Projection expr, final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final BinaryTupleOperator expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final StatementPattern expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final Filter expr, final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final ZeroLengthPath expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final ArbitraryLengthPath expr, final BindingSet bindings)
                throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final Service expr, final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Slice expr,
                final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                final Distinct expr, final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Group expr,
                final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Order expr,
                final BindingSet bindings) throws QueryEvaluationException {
            return delegate(expr, bindings);
        }

        private CloseableIteration<BindingSet, QueryEvaluationException> delegate(
                final TupleExpr expr, final BindingSet bindings) throws QueryEvaluationException {

            try {
                // Skolemize bindings if necessary
                final BindingSet actualBindings = this.skolemize ? skolemize(bindings) : bindings;

                // Convert the algebraic sub-tree into a SelectQuery
                final SelectQuery query = SelectQuery.from(expr, this.dataset);

                // Delegate to TripleStore component
                return this.transaction.query(query, actualBindings, this.timeout);

            } catch (final IOException ex) {
                throw new QueryEvaluationException(ex);
            }
        }

    }

    private static class QNameProcessor extends ASTVisitorBase {

        private final Map<String, String> prefixMap;

        public QNameProcessor(final Map<String, String> prefixMap) {
            this.prefixMap = prefixMap;
        }

        @Override
        public Object visit(final ASTServiceGraphPattern node, final Object data)
                throws VisitorException {
            node.setPrefixDeclarations(this.prefixMap);
            return super.visit(node, data);
        }

        @Override
        public Object visit(final ASTQName qnameNode, final Object data) throws VisitorException {
            final String qname = qnameNode.getValue();
            final int colonIdx = qname.indexOf(':');
            assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;
            final String prefix = qname.substring(0, colonIdx);
            String localName = qname.substring(colonIdx + 1);
            String namespace = this.prefixMap.get(prefix);
            if (namespace == null) { // [FC] added lookup of default namespace
                namespace = Data.getNamespaceMap().get(prefix);
            }
            if (namespace == null) {
                throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
            }
            localName = processEscapesAndHex(localName);
            final ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
            iriNode.setValue(namespace + localName);
            qnameNode.jjtReplaceWith(iriNode);
            return null;
        }

        private String processEscapesAndHex(final String localName) {
            final StringBuffer unencoded = new StringBuffer();
            final Pattern hexPattern = Pattern.compile("([^\\\\]|^)(%[A-F\\d][A-F\\d])",
                    Pattern.CASE_INSENSITIVE);
            Matcher m = hexPattern.matcher(localName);
            boolean result = m.find();
            while (result) {
                final String previousChar = m.group(1);
                final String encoded = m.group(2);

                final int codePoint = Integer.parseInt(encoded.substring(1), 16);
                final String decoded = String.valueOf(Character.toChars(codePoint));

                m.appendReplacement(unencoded, previousChar + decoded);
                result = m.find();
            }
            m.appendTail(unencoded);
            final StringBuffer unescaped = new StringBuffer();
            final Pattern escapedCharPattern = Pattern
                    .compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");
            m = escapedCharPattern.matcher(unencoded.toString());
            result = m.find();
            while (result) {
                final String escaped = m.group();
                m.appendReplacement(unescaped, escaped.substring(1));
                result = m.find();
            }
            m.appendTail(unescaped);
            return unescaped.toString();
        }

    }

}