package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.triplestore.SelectQuery;
import eu.fbk.knowledgestore.triplestore.TripleStore;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;
import eu.fbk.knowledgestore.vocabulary.KS;

public final class TripleDataStore implements DataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(TripleDataStore.class);

    private final TripleStore tripleStore;

    private boolean initialized;

    private boolean closed;

    public TripleDataStore(final TripleStore tripleStore) {
        this.tripleStore = Preconditions.checkNotNull(tripleStore);
        this.initialized = false;
        this.closed = false;
        LOGGER.info("{} configured, triplestore={}", this, this.tripleStore);
    }

    @Override
    public synchronized void init() throws IOException, IllegalStateException {
        Preconditions.checkState(!this.initialized && !this.closed);
        this.initialized = true;
        LOGGER.info("{} initialized", this);
    }

    @Override
    public synchronized DataTransaction begin(final boolean readOnly)
            throws DataCorruptedException, IOException, IllegalStateException {
        Preconditions.checkState(this.initialized && !this.closed);
        return new TripleDataTransaction(this.tripleStore.begin(readOnly));
    }

    @Override
    public synchronized void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    private static final class TripleDataTransaction implements DataTransaction {

        private final TripleTransaction transaction;

        TripleDataTransaction(final TripleTransaction transaction) {
            this.transaction = transaction;
        }

        private Stream<Record> query(final String spoPattern, final URI type,
                @Nullable final Set<? extends URI> properties, @Nullable final XPath condition)
                throws IOException {

            // Compose the query
            final StringBuilder builder = new StringBuilder();
            if (KS.RESOURCE.equals(type)) {
                builder.append("SELECT ?s ?p ?o ?p1 ?o1 ?p2 ?o2 {\n" //
                        + "  ?s ?p ?o .\n" //
                        + "  OPTIONAL {\n    ?o ?p1 ?o1\n" //
                        + "    FILTER (?p = <http://dkm.fbk.eu/ontologies/knowledgestore#storedAs>)\n" //
                        + "    OPTIONAL {\n" //
                        + "      ?o1 ?p2 ?o2\n" //
                        + "      FILTER (?p1 = <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash>)\n" //
                        + "    }\n" //
                        + "  }\n ");
            } else {
                builder.append("SELECT ?s ?p ?o {\n" //
                        + "  ?s ?p ?o .\n");
            }
            builder.append("  ?s a ").append(Data.toString(type, null)).append(" .\n");
            builder.append(spoPattern);
            builder.append("\n}");
            final String query = builder.toString();

            // Issue the query
            final Stream<BindingSet> bindingStream = Stream.create(this.transaction.query(
                    SelectQuery.from(query), null, null));

            // Convert from bindings to statements
            final Stream<Statement> stmtStream;
            if (KS.RESOURCE.equals(type)) {
                stmtStream = bindingStream.transform(null,
                        new Function<Handler<Statement>, Handler<BindingSet>>() {

                            @Override
                            public Handler<BindingSet> apply(final Handler<Statement> handler) {
                                return new Handler<BindingSet>() {

                                    private final Set<Statement> set = Sets.newHashSet();

                                    private Resource subject = null;

                                    @Override
                                    public void handle(final BindingSet bindings) throws Throwable {
                                        if (bindings == null) {
                                            handler.handle(null);
                                            return;
                                        }
                                        final Resource s = (Resource) bindings.getValue("s");
                                        final URI p = (URI) bindings.getValue("p");
                                        final Value o = bindings.getValue("o");
                                        final URI p1 = (URI) bindings.getValue("p1");
                                        final Value o1 = bindings.getValue("o1");
                                        final URI p2 = (URI) bindings.getValue("p2");
                                        final Value o2 = bindings.getValue("o2");
                                        final ValueFactory vf = Data.getValueFactory();
                                        if (!s.equals(this.subject)) {
                                            this.set.clear();
                                            this.subject = s;
                                        }
                                        emit(handler, vf.createStatement(s, p, o));
                                        if (o1 != null) {
                                            emit(handler, vf.createStatement((URI) o, p1, o1));
                                            if (o2 != null) {
                                                emit(handler, vf.createStatement((URI) o1, p2, o2));
                                            }
                                        }
                                    }

                                    private void emit(final Handler<Statement> handler,
                                            final Statement statement) throws Throwable {
                                        if (this.set.add(statement)) {
                                            handler.handle(statement);
                                        }
                                    }

                                };

                            }

                        });
            } else {
                stmtStream = bindingStream.transform(new Function<BindingSet, Statement>() {

                    @Override
                    public Statement apply(final BindingSet bindings) {
                        final Resource s = (Resource) bindings.getValue("s");
                        final URI p = (URI) bindings.getValue("p");
                        final Value o = bindings.getValue("o");
                        return Data.getValueFactory().createStatement(s, p, o);
                    }

                }, 1);
            }

            // Convert from statements to records
            Stream<Record> recordStream = Record.decode(stmtStream, ImmutableList.of(type), true);

            // Apply condition, if specified
            if (condition != null) {
                recordStream = recordStream.filter(condition.asPredicate(), 1);
            }

            // Apply projection, if specified
            if (properties != null && !properties.isEmpty()) {
                final URI[] props = properties.toArray(new URI[properties.size()]);
                recordStream = recordStream.transform(new Function<Record, Record>() {

                    @Override
                    public Record apply(final Record record) {
                        record.retain(props);
                        return null;
                    }

                }, 1);
            }
            return recordStream;
        }

        @Override
        public Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
                final Set<? extends URI> properties) throws IOException, IllegalArgumentException,
                IllegalStateException {
            return Stream.concat(Stream.create(ids).chunk(64)
                    .transform(new Function<List<? extends URI>, Stream<Record>>() {

                        @Override
                        public Stream<Record> apply(final List<? extends URI> input) {
                            final StringBuilder builder = new StringBuilder();
                            builder.append("  VALUES ?s {");
                            for (final URI id : input) {
                                builder.append(" <").append(id.toString()).append(">");
                            }
                            builder.append(" }");
                            try {
                                return query(builder.toString(), type, properties, null);
                            } catch (final IOException ex) {
                                throw Throwables.propagate(ex);
                            }
                        }

                    }, 1));
        }

        @Override
        public Stream<Record> retrieve(final URI type, final XPath condition,
                final Set<? extends URI> properties) throws IOException, IllegalArgumentException,
                IllegalStateException {
            return query("  ?s a <" + type.toString() + "> .", type, properties, condition);
        }

        @Override
        public long count(final URI type, final XPath condition) throws IOException,
                IllegalArgumentException, IllegalStateException {
            return query("  ?s a <" + type.toString() + "> .", type, null, condition).count();
        }

        @Override
        public Stream<Record> match(final Map<URI, XPath> conditions,
                final Map<URI, Set<URI>> ids, final Map<URI, Set<URI>> properties)
                throws IOException, IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void store(final URI type, final Record record) throws IOException,
                IllegalStateException {

            // Delete existing data for the record URI
            delete(type, record.getID());

            // Add statements
            final List<Statement> statements = Record.encode(Stream.create(record),
                    ImmutableList.of(type)).toList();
            this.transaction.add(statements);
        }

        @Override
        public void delete(final URI type, final URI id) throws IOException, IllegalStateException {

            // Obtain the statements to delete throuh a lookup
            final List<Statement> statements = Record.encode(
                    lookup(type, ImmutableSet.of(id), null), ImmutableList.of(type)).toList();

            // Perform the deletion
            this.transaction.remove(statements);
        }

        @Override
        public void end(final boolean commit) throws DataCorruptedException, IOException,
                IllegalStateException {
            this.transaction.end(commit);
        }

    }

}
