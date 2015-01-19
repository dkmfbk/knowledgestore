package eu.fbk.knowledgestore;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.openrdf.model.URI;

import eu.fbk.knowledgestore.Operation.Count;
import eu.fbk.knowledgestore.Operation.Create;
import eu.fbk.knowledgestore.Operation.Delete;
import eu.fbk.knowledgestore.Operation.Download;
import eu.fbk.knowledgestore.Operation.Match;
import eu.fbk.knowledgestore.Operation.Merge;
import eu.fbk.knowledgestore.Operation.Retrieve;
import eu.fbk.knowledgestore.Operation.Sparql;
import eu.fbk.knowledgestore.Operation.Update;
import eu.fbk.knowledgestore.Operation.Upload;

public abstract class ForwardingSession extends ForwardingObject implements Session {

    @Override
    protected abstract Session delegate();

    @Override
    @Nullable
    public String getUsername() throws IllegalStateException {
        return delegate().getUsername();
    }

    @Override
    @Nullable
    public String getPassword() throws IllegalStateException {
        return delegate().getPassword();
    }

    @Override
    public Map<String, String> getNamespaces() throws IllegalStateException {
        return delegate().getNamespaces();
    }

    @Override
    public Download download(final URI resourceID) throws IllegalStateException {
        return delegate().download(resourceID);
    }

    @Override
    public Upload upload(final URI resourceID) throws IllegalStateException {
        return delegate().upload(resourceID);
    }

    @Override
    public Count count(final URI type) throws IllegalStateException {
        return delegate().count(type);
    }

    @Override
    public Retrieve retrieve(final URI type) throws IllegalStateException {
        return delegate().retrieve(type);
    }

    @Override
    public Create create(final URI type) throws IllegalStateException {
        return delegate().create(type);
    }

    @Override
    public Merge merge(final URI type) throws IllegalStateException {
        return delegate().merge(type);
    }

    @Override
    public Update update(final URI type) throws IllegalStateException {
        return delegate().update(type);
    }

    @Override
    public Delete delete(final URI type) throws IllegalStateException {
        return delegate().delete(type);
    }

    @Override
    public Match match() throws IllegalStateException {
        return delegate().match();
    }

    @Override
    public Sparql sparql(final String expression, final Object... arguments)
            throws IllegalStateException {
        return delegate().sparql(expression, arguments);
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
