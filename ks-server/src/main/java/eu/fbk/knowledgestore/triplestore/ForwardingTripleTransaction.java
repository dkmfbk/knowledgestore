package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.knowledgestore.data.Handler;

/**
 * A <tt>TripleTransaction</tt> that forwards all its method calls to another
 * <tt>TripleTransaction</tt>.
 * <p>
 * This class provides a starting point for implementing the decorator pattern on top of the
 * <tt>TripleTransaction</tt> interface. Subclasses must implement method {@link #delegate()} and
 * override the methods of <tt>TripleTransaction</tt> they want to decorate.
 * </p>
 */
public abstract class ForwardingTripleTransaction extends ForwardingObject implements
        TripleTransaction {

    @Override
    protected abstract TripleTransaction delegate();

    @Override
    public CloseableIteration<? extends Statement, ? extends Exception> get(
            @Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, @Nullable final Resource context) throws IOException,
            IllegalStateException {
        return delegate().get(subject, predicate, object, context);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> query(final SelectQuery query,
            @Nullable final BindingSet bindings, @Nullable final Long timeout) throws IOException,
            UnsupportedOperationException {
        return delegate().query(query, bindings, timeout);
    }

    @Override
    public void infer(@Nullable final Handler<? super Statement> handler) throws IOException,
            IllegalStateException {
        delegate().infer(handler);
    }

    @Override
    public void add(final Iterable<? extends Statement> statements) throws IOException,
            IllegalStateException {
        delegate().add(statements);
    }

    @Override
    public void remove(final Iterable<? extends Statement> statements) throws IOException,
            IllegalStateException {
        delegate().remove(statements);
    }

    @Override
    public void end(final boolean commit) throws IOException {
        delegate().end(commit);
    }

}
