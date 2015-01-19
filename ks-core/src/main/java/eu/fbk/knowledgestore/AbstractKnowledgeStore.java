package eu.fbk.knowledgestore;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKnowledgeStore implements KnowledgeStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKnowledgeStore.class);

    private final List<Session> sessions;

    private boolean closed;

    protected AbstractKnowledgeStore() {
        this.sessions = Lists.newLinkedList();
        this.closed = false;
    }

    @Override
    public final synchronized Session newSession() throws IllegalStateException {
        return newSession(null, null);
    }

    @Override
    public final synchronized Session newSession(@Nullable final String username,
            @Nullable final String password) throws IllegalStateException {
        synchronized (this.sessions) {
            evictClosedSessions();
            final Session session = doNewSession(username, password);
            this.sessions.add(session);
            return session;
        }
    }

    @Override
    public final synchronized void close() {
        if (!this.closed) {
            for (final Session session : this.sessions) {
                try {
                    session.close();
                } catch (final Throwable ex) {
                    LOGGER.error("Error closing session: " + ex.getMessage(), ex);
                }
            }
            try {
                doClose();
            } finally {
                this.closed = true;
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + (this.closed ? "closed" : "open") + ", "
                + this.sessions.size() + " sessions]";
    }

    @Override
    public final synchronized boolean isClosed() {
        return this.closed;
    }

    protected final void checkNotClosed() {
        if (this.closed) {
            throw new IllegalStateException("KnowledgeStore has been closed");
        }
    }

    protected final void evictClosedSessions() {
        synchronized (this.sessions) {
            for (int i = this.sessions.size() - 1; i >= 0; --i) {
                if (this.sessions.get(i).isClosed()) {
                    this.sessions.remove(i);
                }
            }
        }
    }

    protected abstract Session doNewSession(@Nullable String username, @Nullable String password);

    protected void doClose() {
    }

}
