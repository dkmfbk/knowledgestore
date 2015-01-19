package eu.fbk.knowledgestore.datastore;

import java.io.IOException;

import com.google.common.collect.ForwardingObject;

/**
 * A {@code DataStore} forwarding all its method calls to another {@code DataStore}.
 * <p>
 * This class provides a starting point for implementing the decorator pattern on top of the
 * {@code DataStore} interface. Subclasses must implement method {@link #delegate()} and override
 * the methods of {@code DataStore} they want to decorate.
 * </p>
 */
public abstract class ForwardingDataStore extends ForwardingObject implements DataStore {

    @Override
    protected abstract DataStore delegate();

    @Override
    public void init() throws IOException {
        delegate().init();
    }

    @Override
    public DataTransaction begin(final boolean readOnly) throws IOException, IllegalStateException {
        return delegate().begin(readOnly);
    }

    @Override
    public void close() {
        delegate().close();
    }

}
