package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;

/**
 * A {@code DataTransaction} forwarding all its method calls to another {@code DataTransaction}.
 * <p>
 * This class provides a starting point for implementing the decorator pattern on top of the
 * {@code DataTransaction} interface. Subclasses must implement method {@link #delegate()} and
 * override the methods of {@code DataTransaction} they want to decorate.
 * </p>
 */
public abstract class ForwardingDataTransaction extends ForwardingObject implements
        DataTransaction {

    @Override
    protected abstract DataTransaction delegate();

    @Override
    public Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
            @Nullable final Set<? extends URI> properties) throws IOException,
            IllegalArgumentException, IllegalStateException {
        return delegate().lookup(type, ids, properties);
    }

    @Override
    public Stream<Record> retrieve(final URI type, @Nullable final XPath condition,
            @Nullable final Set<? extends URI> properties) throws IOException,
            IllegalArgumentException, IllegalStateException {
        return delegate().retrieve(type, condition, properties);
    }

    @Override
    public long count(final URI type, @Nullable final XPath condition) throws IOException,
            IllegalArgumentException, IllegalStateException {
        return delegate().count(type, condition);
    }

    @Override
    public Stream<Record> match(final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
            final Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
        return delegate().match(conditions, ids, properties);
    }

    @Override
    public void store(final URI type, final Record record) throws IOException,
            IllegalStateException {
        delegate().store(type, record);
    }

    @Override
    public void delete(final URI type, final URI id) throws IOException, IllegalStateException {
        delegate().delete(type, id);
    }

    @Override
    public void end(final boolean commit) throws IOException, IllegalStateException {
        delegate().end(commit);
    }

}
