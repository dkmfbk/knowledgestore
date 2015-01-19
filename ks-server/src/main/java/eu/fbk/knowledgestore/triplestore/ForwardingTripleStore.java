package eu.fbk.knowledgestore.triplestore;

import java.io.IOException;

import com.google.common.collect.ForwardingObject;


/**
 * A {@code TripleStore} that forwards all its method calls to another {@code TripleStore}.
 * <p>
 * This class provides a starting point for implementing the decorator pattern on top of the
 * {@code TripleStore} interface. Subclasses must implement method {@link #delegate()} and
 * override the methods of {@code TripleStore} they want to decorate.
 * </p>
 */
public abstract class ForwardingTripleStore extends ForwardingObject implements TripleStore {

    @Override
    protected abstract TripleStore delegate();

    @Override
    public void init() throws IOException {
        delegate().init();
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws IOException {
        return delegate().begin(readOnly);
    }

    @Override
    public void reset() throws IOException {
        delegate().reset();
    }

    @Override
    public void close() {
        delegate().close();
    }

}
