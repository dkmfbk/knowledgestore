package eu.fbk.knowledgestore;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

public abstract class ForwardingKnowledgeStore extends ForwardingObject implements KnowledgeStore {

    @Override
    protected abstract KnowledgeStore delegate();

    @Override
    public Session newSession() throws IllegalStateException {
        return delegate().newSession();
    }

    @Override
    public Session newSession(@Nullable final String username, @Nullable final String password)
            throws IllegalStateException {
        return delegate().newSession(username, password);
    }

    @Override
    public boolean isClosed() {
        return delegate().isClosed();
    }

    @Override
    public void close() {
        delegate().close();
    }

}
