package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.ListBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.internal.rdf.CompactBindingSet;

/**
 * A {@code TripleStore} wrapper that rewrites URIs, adapting a prefix used externally in the KS
 * to a prefix used internally to the wrapped triple.
 * <p>
 * The wrapper intercepts calls to an underlying {@code TripleStore} and to the
 * {@code TripleTransaction}s it creates. It rewrites any incoming URI starting with a configured
 * <i>external</i> prefix, replacing that prefix with an <i>internal</i> replacement. At the same
 * time, the wrapper rewrites every outgoing URI starting with the <i>internal</i> prefix,
 * replacing that prefix with the <i>external</i> one. Rewrite occurs on every input / output data
 * item exchanged at the level of the {@code TripleStore} API, including RDF values, statements,
 * binding sets and SPARQL queries (query rewriting is done by manipulating their algebraic form).
 * </p>
 */
public final class RewritingTripleStore extends ForwardingTripleStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RewritingTripleStore.class);

    private final TripleStore delegate;

    private final Rewriter in; // rewriter for incoming data

    private final Rewriter out; // rewriter for outgoing data

    /**
     * Creates a new instance for the wrapped {@code TripleStore} specified.
     *
     * @param delegate
     *            the wrapped {@code TripleStore}
     * @param internalPrefix
     *            the internal (i.e., in the TripleStore) prefix of the URIs to rewrite
     * @param externalPrefix
     *            the external (i.e., in the KS API) prefix of the URIs to rewrite
     */
    public RewritingTripleStore(final TripleStore delegate, final String internalPrefix,
            final String externalPrefix) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.in = new Rewriter(externalPrefix, internalPrefix);
        this.out = new Rewriter(internalPrefix, externalPrefix);
        LOGGER.debug("{} configured", getClass().getSimpleName());
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws IOException {
        return new RewritingTripleTransaction(super.begin(readOnly));
    }

    @Override
    protected TripleStore delegate() {
        return this.delegate;
    }

    private class RewritingTripleTransaction extends ForwardingTripleTransaction {

        private final TripleTransaction delegate;

        RewritingTripleTransaction(final TripleTransaction delegate) {
            this.delegate = Preconditions.checkNotNull(delegate);
        }

        @Override
        protected TripleTransaction delegate() {
            return this.delegate;
        }

        @Override
        public CloseableIteration<? extends Statement, ? extends Exception> get(
                @Nullable final Resource subject, @Nullable final URI predicate,
                @Nullable final Value object, @Nullable final Resource context)
                throws IOException, IllegalStateException {
            return RewritingTripleStore.this.out.rewriteStatements(delegate().get(//
                    RewritingTripleStore.this.in.rewriteValue(subject), //
                    RewritingTripleStore.this.in.rewriteValue(predicate), //
                    RewritingTripleStore.this.in.rewriteValue(object), //
                    RewritingTripleStore.this.in.rewriteValue(context)));
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> query(
                final SelectQuery query, @Nullable final BindingSet bindings,
                @Nullable final Long timeout) throws IOException, UnsupportedOperationException {
            return RewritingTripleStore.this.out.rewriteBindings(//
                    ImmutableList.copyOf(query.getExpression().getBindingNames()), //
                    super.query(RewritingTripleStore.this.in.rewriteQuery(query),
                            RewritingTripleStore.this.in.rewriteBindings(bindings), timeout));
        }

        @Override
        public void infer(@Nullable final Handler<? super Statement> handler) throws IOException,
                IllegalStateException {
            delegate().infer(RewritingTripleStore.this.out.rewriteStatements(handler));
        }

        @Override
        public void add(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {
            delegate().add(RewritingTripleStore.this.in.rewriteStatements(statements));
        }

        @Override
        public void remove(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {
            delegate().remove(RewritingTripleStore.this.in.rewriteStatements(statements));
        }

    }

    private static class Rewriter {

        private final String fromPrefix;

        private final String toPrefix;

        Rewriter(final String fromPrefix, final String toPrefix) {
            this.fromPrefix = Preconditions.checkNotNull(fromPrefix);
            this.toPrefix = Preconditions.checkNotNull(toPrefix);
        }

        @Nullable
        SelectQuery rewriteQuery(@Nullable final SelectQuery query) {

            final TupleExpr expr = query.getExpression().clone();
            expr.visit(new QueryModelVisitorBase<RuntimeException>() {

                @Override
                public void meet(final BindingSetAssignment node) throws RuntimeException {
                    final List<BindingSet> bindingsList = Lists.newArrayList();
                    for (final BindingSet bindings : node.getBindingSets()) {
                        bindingsList.add(rewriteBindings(bindings));
                    }
                    node.setBindingSets(bindingsList);
                }

                @Override
                public void meet(final ValueConstant node) throws RuntimeException {
                    node.setValue(rewriteValue(node.getValue()));
                }

                @Override
                public void meet(final Var node) throws RuntimeException {
                    node.setValue(rewriteValue(node.getValue()));
                }

                // @Override
                // public void meet(FunctionCall node) throws RuntimeException {
                // }

                // @Override
                // public void meet(IRIFunction node) throws RuntimeException {
                // }

                // @Override
                // public void meet(Service node) throws RuntimeException {
                // }

            });

            return SelectQuery.from(expr, query.getDataset());
        }

        @Nullable
        <E extends Exception> CloseableIteration<BindingSet, E> rewriteBindings(
                final List<String> variables,
                @Nullable final CloseableIteration<? extends BindingSet, ? extends E> iteration) {

            if (iteration == null) {
                return null;
            }

            final CompactBindingSet.Builder builder = CompactBindingSet.builder(variables);
            return new ConvertingIteration<BindingSet, BindingSet, E>(iteration) {

                @Override
                protected BindingSet convert(final BindingSet bindings) throws E {
                    for (int i = 0; i < variables.size(); ++i) {
                        final String variable = variables.get(i);
                        builder.set(i, rewriteValue(bindings.getValue(variable)));
                    }
                    return builder.build();
                }

            };
        }

        @Nullable
        BindingSet rewriteBindings(@Nullable final BindingSet bindings) {

            if (bindings != null) {
                final Set<String> names = bindings.getBindingNames();
                final Value[] values = new Value[names.size()];
                boolean changed = false;
                int index = 0;
                for (final String name : names) {
                    final Value oldValue = bindings.getValue(name);
                    final Value newValue = rewriteValue(oldValue);
                    values[index++] = newValue;
                    changed |= oldValue != newValue;
                }
                if (changed) {
                    return new ListBindingSet(ImmutableList.copyOf(names), values);
                }
            }
            return bindings;
        }

        Handler<Statement> rewriteStatements(final Handler<? super Statement> handler) {

            return new Handler<Statement>() {

                @Override
                public void handle(final Statement statement) throws Throwable {
                    handler.handle(statement == null ? null : rewriteStatement(statement));
                }

            };
        }

        @Nullable
        <E extends Exception> CloseableIteration<Statement, E> rewriteStatements(
                @Nullable final CloseableIteration<? extends Statement, ? extends E> iteration) {

            return iteration == null ? null : new ConvertingIteration<Statement, Statement, E>(
                    iteration) {

                @Override
                protected Statement convert(final Statement statement) throws E {
                    return rewriteStatement(statement);
                }

            };
        }

        @Nullable
        Iterable<Statement> rewriteStatements(
                @Nullable final Iterable<? extends Statement> statements) {

            return statements == null ? null : Iterables.transform(statements,
                    new Function<Statement, Statement>() {

                        @Override
                        @Nullable
                        public Statement apply(@Nullable final Statement statement) {
                            return rewriteStatement(statement);
                        }

                    });
        }

        @Nullable
        Statement rewriteStatement(@Nullable final Statement statement) {

            if (statement == null) {
                return null;
            }

            final Resource oldSubj = statement.getSubject();
            final URI oldPred = statement.getPredicate();
            final Value oldObj = statement.getObject();
            final Resource oldCtx = statement.getContext();

            final Resource newSubj = rewriteValue(oldSubj);
            final URI newPred = rewriteValue(oldPred);
            final Value newObj = rewriteValue(oldObj);
            final Resource newCtx = rewriteValue(oldCtx);

            if (oldSubj == newSubj && oldPred == newPred && oldObj == newObj && oldCtx == newCtx) {
                return statement;
            } else if (newCtx != null) {
                return Data.getValueFactory().createStatement(newSubj, newPred, newObj, newCtx);
            } else {
                return Data.getValueFactory().createStatement(newSubj, newPred, newObj);
            }
        }

        @SuppressWarnings("unchecked")
        @Nullable
        <T extends Value> T rewriteValue(@Nullable final T value) {
            if (value instanceof URI) {
                final URI uri = (URI) value;
                final String string = uri.stringValue();
                if (string.startsWith(this.fromPrefix)) {
                    return (T) Data.getValueFactory().createURI(
                            this.toPrefix + string.substring(this.fromPrefix.length()));
                }
            }
            return value;
        }

    }

}
