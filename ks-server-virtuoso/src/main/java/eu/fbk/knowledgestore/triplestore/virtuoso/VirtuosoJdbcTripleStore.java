package eu.fbk.knowledgestore.triplestore.virtuoso;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.ListBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

import virtuoso.jdbc4.ConnectionWrapper;
import virtuoso.jdbc4.VirtuosoConnection;
import virtuoso.jdbc4.VirtuosoConnectionPoolDataSource;
import virtuoso.jdbc4.VirtuosoPooledConnection;
import virtuoso.sql.ExtendedString;
import virtuoso.sql.RdfBox;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.triplestore.SelectQuery;
import eu.fbk.knowledgestore.triplestore.TripleStore;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;

public final class VirtuosoJdbcTripleStore implements TripleStore {

    // see also the following resources for reference:
    // - https://newsreader.fbk.eu/trac/wiki/TripleStoreNotes
    // - http://docs.openlinksw.com/sesame/ (Virtuoso javadoc)
    // - http://www.openlinksw.com/vos/main/Main/VirtSesame2Provider

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoJdbcTripleStore.class);

    private static final String DEFAULT_HOST = "localhost";

    private static final int DEFAULT_PORT = 1111;

    private static final String DEFAULT_USERNAME = "dba";

    private static final String DEFAULT_PASSWORD = "dba";

    private static final int DEFAULT_FETCH_SIZE = 200;

    private static final long GRACE_PERIOD = 5000; // 5s more for client-side timeout

    private final VirtuosoConnectionPoolDataSource source;

    private final int fetchSize;

    private final AtomicLong transactionCounter;

    /**
     * Creates a new instance based on the supplied configuration properties.
     *
     * @param host
     *            the name / IP address of the host where virtuoso is running; if null defaults to
     *            localhost
     * @param port
     *            the port Virtuoso is listening to; if null defaults to 1111
     * @param username
     *            the username to login into Virtuoso; if null defaults to dba
     * @param password
     *            the password to login into Virtuoso; if null default to dba
     * @param fetchSize
     *            the number of results (solutions, triples, ...) to fetch from Virtuoso in a
     *            single operation when query results are iterated; if null defaults to 200
     * @param charset
     *            the charset to use for serializing / deserializing textual data exchanged with
     *            the server; if null defaults to UTF-8
     */
    public VirtuosoJdbcTripleStore(@Nullable final String host, @Nullable final Integer port,
            @Nullable final String username, @Nullable final String password,
            @Nullable final Integer fetchSize, @Nullable final String charset) {

        // Configure the data source
        // (see http://docs.openlinksw.com/virtuoso/VirtuosoDriverJDBC.html, section 7.4.4.2)
        this.source = new VirtuosoConnectionPoolDataSource();
        this.source.setServerName(MoreObjects.firstNonNull(host, DEFAULT_HOST));
        this.source.setPortNumber(MoreObjects.firstNonNull(port, DEFAULT_PORT));
        this.source.setUser(MoreObjects.firstNonNull(username, DEFAULT_USERNAME));
        this.source.setPassword(MoreObjects.firstNonNull(password, DEFAULT_PASSWORD));
        this.source.setCharset(charset != null ? charset : "UTF-8");

        // Configure and validate other parameters
        this.fetchSize = MoreObjects.firstNonNull(fetchSize, DEFAULT_FETCH_SIZE);
        this.transactionCounter = new AtomicLong(0L);
        Preconditions.checkArgument(this.fetchSize > 0);

        // Log relevant information
        LOGGER.info("VirtuosoTripleStore configured, URL={}, fetchSize={}",
                this.source.getServerName() + ":" + this.source.getPortNumber(), fetchSize);
    }

    @Override
    public void init() throws IOException {
        // Nothing to do here
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws DataCorruptedException,
            IOException {
        return new VirtuosoTransaction(readOnly);
    }

    @Override
    public void reset() throws IOException {
        Connection connection = null;
        try {
            connection = this.source.getConnection();
            connection.setReadOnly(false);
            connection.setAutoCommit(true);
            connection.prepareCall("RDF_GLOBAL_RESET ()").execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        } finally {
            Util.closeQuietly(connection);
        }
    }

    @Override
    public void close() {
        // no need to terminate pending transactions: this is done externally
        try {
            this.source.close();
        } catch (final SQLException ex) {
            LOGGER.error("Failed to shutdown Virtuoso driver", ex);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private static Value castValue(final Object value) throws IllegalArgumentException {

        final ValueFactory vf = Data.getValueFactory();

        if (value == null) {
            return null;
        } else if (value instanceof ExtendedString) {
            final ExtendedString es = (ExtendedString) value;
            String string = es.toString();
            try {
                if (es.getIriType() == ExtendedString.IRI && (es.getStrType() & 0x01) == 0x01) {
                    if (string.startsWith("_:")) {
                        string = string.substring(2);
                        return vf.createBNode(string);
                    } else if (string.indexOf(':') < 0) {
                        return vf.createURI(":" + string);
                    } else {
                        return vf.createURI(string);
                    }
                } else if (es.getIriType() == ExtendedString.BNODE) {
                    return vf.createBNode(string);
                } else {
                    return vf.createLiteral(string);
                }
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Invalid value from Virtuoso: \"" + string
                        + "\", STRTYPE = " + es.getIriType(), ex);
            }
        } else if (value instanceof RdfBox) {
            final RdfBox rb = (RdfBox) value;
            if (rb.getLang() != null) {
                return vf.createLiteral(rb.toString(), rb.getLang());
            } else if (rb.getType() != null) {
                return vf.createLiteral(rb.toString(), vf.createURI(rb.getType()));
            } else {
                return vf.createLiteral(rb.toString());
            }
        } else if (value instanceof Blob) {
            return vf.createLiteral(value.toString(), XMLSchema.HEXBINARY);
        } else if (value instanceof Date) {
            return Data.convert(new java.util.Date(((Date) value).getTime()), Value.class);
        } else if (value instanceof Timestamp) {
            return Data.convert(new Date(((Timestamp) value).getTime()), Value.class);
        } else if (value instanceof Time) {
            return vf.createLiteral(value.toString(), XMLSchema.TIME);
        } else {
            try {
                return Data.convert(value, Value.class);
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Could not parse value: " + value, ex);
            }
        }
    }

    private static String sqlForQuery(final String query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings) {

        // Start composing a 'sparql' SQL command
        final StringBuilder builder = new StringBuilder("sparql\n ");

        // Generate define directives for graphs in the FROM / FROM NAMED clauses
        if (dataset != null) {
            final Set<URI> empty = Collections.emptySet();
            for (final URI uri : MoreObjects.firstNonNull(dataset.getDefaultGraphs(), empty)) {
                builder.append(" define input:default-graph-uri <" + uri + "> \n");
            }
            for (final URI uri : MoreObjects.firstNonNull(dataset.getNamedGraphs(), empty)) {
                builder.append(" define input:named-graph-uri <" + uri + "> \n");
            }
        }

        // Apply variable bindings, i.e., replace certain variables with supplied values
        if (bindings != null && bindings.size() > 0) {
            int i = 0;
            final int length = query.length();
            while (i < query.length()) {
                final char ch = query.charAt(i++);
                if (ch == '\\' && i < length) {
                    builder.append(ch).append(query.charAt(i++));
                } else if (ch == '"' || ch == '\'') {
                    builder.append(ch);
                    while (i < length) {
                        final char c = query.charAt(i++);
                        builder.append(c);
                        if (c == ch) {
                            break;
                        }
                    }
                } else if ((ch == '?' || ch == '$') && i < length
                        && isVarFirstChar(query.charAt(i))) {
                    int j = i + 1;
                    while (j < length && isVarMiddleChar(query.charAt(j))) {
                        ++j;
                    }
                    final String name = query.substring(i, j);
                    final Value value = bindings.getValue(name);
                    if (value != null) {
                        builder.append(Data.toString(value, null));
                    } else {
                        builder.append(ch).append(name);
                    }
                    i = j;
                } else {
                    builder.append(ch);
                }
            }
        } else {
            builder.append(query);
        }

        // Return the resulting SQL statement
        return builder.toString();
    }

    private static boolean isVarFirstChar(final char c) {
        // Returns true if the supplied char can be used as first char in a variable name
        return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || c == '_'
                || 0x00C0 <= c && c <= 0x00D6 || 0x00D8 <= c && c <= 0x00F6 || 0x00F8 <= c
                && c <= 0x02FF || 0x0370 <= c && c <= 0x037D || 0x037F <= c && c <= 0x1FFF
                || 0x200C <= c && c <= 0x200D || 0x2070 <= c && c <= 0x218F || 0x2C00 <= c
                && c <= 0x2FEF || 0x3001 <= c && c <= 0xD7FF || 0xF900 <= c && c <= 0xFDCF
                || 0xFDF0 <= c && c <= 0xFFFD;
    }

    private static boolean isVarMiddleChar(final char c) {
        // Returns true if the supplied char can be used as i-th char (i > 1) in a variable name
        return isVarFirstChar(c) || c == 0x00B7 || 0x0300 <= c && c <= 0x036F || 0x203F <= c
                && c <= 0x2040;
    }

    private static boolean isPartialResultException(final Throwable ex) {
        // TODO: try to better implement this method without checking for string containment
        // (perhaps now we have access to some SQL code)
        return ex.getMessage() != null && ex.getMessage().contains("Returning incomplete results");
    }

    private static void killConnection(final Object connection) throws Throwable {
        if (connection instanceof ConnectionWrapper) {
            final Field field = ConnectionWrapper.class.getDeclaredField("pconn");
            field.setAccessible(true);
            killConnection(field.get(connection));
        } else if (connection instanceof VirtuosoPooledConnection) {
            killConnection(((VirtuosoPooledConnection) connection).getVirtuosoConnection());
        } else if (connection instanceof VirtuosoConnection) {
            final Field field = VirtuosoConnection.class.getDeclaredField("socket");
            field.setAccessible(true);
            final Closeable socket = (Closeable) field.get(connection);
            socket.close(); // as Virtuoso driver ignores polite interrupt
        } else {
            throw new Exception("Don't know how to kill connection "
                    + connection.getClass().getName());
        }
    }

    private final class VirtuosoTransaction implements TripleTransaction {

        private final Connection connection; // the underlying JDBC connection

        private final boolean readOnly; // whether only get() and query() are allowed

        private final String id; // transaction name (for logging purposes)

        VirtuosoTransaction(final boolean readOnly) throws IOException {

            // Setup ID and read-only setting
            this.readOnly = readOnly;
            this.id = "Virtuoso TX"
                    + VirtuosoJdbcTripleStore.this.transactionCounter.incrementAndGet();

            // Acquire a JDBC connection from the pool
            Connection connection = null;
            try {
                connection = VirtuosoJdbcTripleStore.this.source.getConnection();
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                connection.setReadOnly(readOnly);
                connection.setAutoCommit(true);
            } catch (final SQLException ex) {
                Util.closeQuietly(connection);
                throw new IOException("Could not connect to Virtuoso", ex);
            }
            this.connection = connection;
        }

        private void checkWritable() {
            if (this.readOnly) {
                throw new IllegalStateException(
                        "Write operation not allowed on read-only transaction");
            }
        }

        @Override
        public CloseableIteration<? extends Statement, ? extends Exception> get(
                final Resource subject, final URI predicate, final Value object,
                final Resource context) throws IOException, IllegalStateException {

            // Generate the query retrieving subset of variables ?s, ?p, ?o, ?c for req. stmts
            // NOTE: we always use GRAPH as triple in Virtuoso are *always* stored inside a graph
            final StringBuilder builder = new StringBuilder();
            builder.append("SELECT * WHERE { GRAPH ");
            builder.append(context == null ? "?c" : Data.toString(context, null));
            builder.append(" { ");
            builder.append(subject == null ? "?s" : Data.toString(subject, null));
            builder.append(' ');
            builder.append(predicate == null ? "?p" : Data.toString(predicate, null));
            builder.append(' ');
            builder.append(object == null ? "?o" : Data.toString(object, null));
            builder.append(" } }");
            final String query = builder.toString();

            // Execute the query, converting each result from BindingSet to Statement
            return new ConvertingIteration<BindingSet, Statement, Exception>(
                    new VirtuosoQueryIteration(query, null, null, null)) {

                @Override
                protected Statement convert(final BindingSet tuple) throws Exception {
                    final Resource s = subject != null ? subject : (Resource) tuple.getValue("s");
                    final URI p = predicate != null ? predicate : (URI) tuple.getValue("p");
                    final Value o = object != null ? object : tuple.getValue("o");
                    final Resource c = context != null ? context : (Resource) tuple.getValue("c");
                    return Data.getValueFactory().createStatement(s, p, o, c);
                }

            };
        }

        @Override
        public CloseableIteration<BindingSet, QueryEvaluationException> query(
                final SelectQuery query, final BindingSet bindings, final Long timeout)
                throws IOException, UnsupportedOperationException, IllegalStateException {

            // Delegate to VirtuosoQueryIteration
            return new VirtuosoQueryIteration(query.getString(), query.getDataset(), bindings,
                    timeout);
        }

        @Override
        public void infer(final Handler<? super Statement> handler) throws IOException,
                IllegalStateException {

            checkWritable();

            // No inference done at this level (to be implemented in a decorator).
            if (handler != null) {
                try {
                    handler.handle(null);
                } catch (final Throwable ex) {
                    Throwables.propagateIfPossible(ex, IOException.class);
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void add(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {
            update(true, statements);
        }

        @Override
        public void remove(final Iterable<? extends Statement> statements) throws IOException,
                IllegalStateException {
            update(false, statements);
        }

        private void update(final boolean insert, final Iterable<? extends Statement> statements)
                throws IOException, IllegalStateException {

            // Check arguments and state
            Preconditions.checkNotNull(statements);
            checkWritable();

            try {
                // Compose the SQL statement
                final StringBuilder builder = new StringBuilder();
                builder.append("SPARQL ");
                builder.append(insert ? "INSERT" : "DELETE");
                builder.append(" DATA {");
                for (final Statement stmt : statements) {
                    final Resource ctx = MoreObjects.firstNonNull(stmt.getContext(), SESAME.NIL);
                    builder.append("\n  GRAPH ");
                    builder.append(Data.toString(ctx, null));
                    builder.append(" { ");
                    builder.append(Data.toString(stmt.getSubject(), null));
                    builder.append(' ');
                    builder.append(Data.toString(stmt.getPredicate(), null));
                    builder.append(' ');
                    builder.append(Data.toString(stmt.getObject(), null));
                    builder.append(" }");
                }
                builder.append("\n}");

                // Issue the statement
                final java.sql.Statement statement = this.connection.createStatement();
                statement.executeUpdate(builder.toString());

            } catch (final SQLException ex) {
                throw new IOException(ex);
            }
        }

        @Override
        public void end(final boolean commit) throws DataCorruptedException, IOException,
                IllegalStateException {

            try {
                // Schedule a task for killing the connection by forcedly closing its socket
                final Future<?> future = Data.getExecutor().schedule(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            killConnection(VirtuosoTransaction.this.connection);
                            LOGGER.warn("{} - killed Virtuoso JDBC connection", this);
                        } catch (final Throwable ex) {
                            LOGGER.debug(this + " - failed to kill Virtuoso JDBC connection "
                                    + "(connection class is "
                                    + VirtuosoTransaction.this.connection.getClass() + ")", ex);
                        }
                    }

                }, 1000, TimeUnit.MILLISECONDS);

                // Perform commit or rollback, in case of a non-read-only transaction
                if (!this.readOnly) {
                    if (commit) {
                        this.connection.commit();
                    } else {
                        this.connection.rollback();
                    }
                }

                // Try to close the connection 'kindly'. On success, unschedule the killing job
                this.connection.close();
                future.cancel(false);

            } catch (final SQLException ex) {
                LOGGER.error(this + " - failed to close connection", ex);
            }
        }

        @Override
        public String toString() {
            return this.id;
        }

        private final class VirtuosoQueryIteration implements
                CloseableIteration<BindingSet, QueryEvaluationException> {

            private final List<String> variables; // output variables

            private java.sql.Statement statement; // to be closed at the end of the iteration

            private ResultSet cursor; // to be closed at the end of the iteration

            private BindingSet tuple; // next tuple to be returned

            public VirtuosoQueryIteration(final String query, @Nullable final Dataset dataset,
                    @Nullable final BindingSet bindings, @Nullable final Long timeout)
                    throws IOException {

                try {
                    // Convert the SPARQL query to the corresponding Virtuoso SQL command
                    final String sql = sqlForQuery(query, dataset, bindings);

                    // Set the server-side timeout that control returning of partial results
                    final int msTimeout = timeout == null ? 0 : timeout.intValue();
                    try {
                        VirtuosoTransaction.this.connection.prepareCall(
                                "set result_timeout = " + msTimeout).execute();
                    } catch (final Throwable ex) {
                        LOGGER.warn(VirtuosoTransaction.this
                                + " - failed to set result_timeout = " + msTimeout
                                + " on Virtuoso JDBC connection (proceeding anyway)", ex);
                    }

                    // Create and configure the SQL Statement object for executing the query
                    this.statement = VirtuosoTransaction.this.connection.createStatement();
                    this.statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                    this.statement.setFetchSize(VirtuosoJdbcTripleStore.this.fetchSize);
                    if (timeout != null) {
                        // Set the client-side timeout in seconds for getting the first result
                        this.statement.setQueryTimeout((int) ((timeout + GRACE_PERIOD) / 1000));
                    }

                    // Start with empty variable list and null output tuple (they will be changed
                    // upon successful query execution).
                    this.variables = Lists.newArrayList();
                    this.tuple = null;

                    // Execute the query (this may fail in case of partial results)
                    this.cursor = this.statement.executeQuery(sql);

                    // Retrieve output variables.
                    final ResultSetMetaData metadata = this.cursor.getMetaData();
                    for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                        this.variables.add(metadata.getColumnName(i));
                    }

                } catch (final Throwable ex) {
                    if (isPartialResultException(ex)) {
                        // Ignore the excetion and close immediately the allocated resources.
                        // An empty iteration will be returned
                        LOGGER.debug(
                                "{} -no results / partial results returned due to expired timeout",
                                VirtuosoTransaction.this);
                        Util.closeQuietly(this);
                    }
                    throw new IOException("Could not obtain query result set", ex);
                }
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                if (this.tuple == null) {
                    this.tuple = advance();
                }
                return this.tuple != null;
            }

            @Override
            public BindingSet next() throws QueryEvaluationException {
                if (this.tuple == null) {
                    this.tuple = advance();
                }
                if (this.tuple == null) {
                    throw new NoSuchElementException();
                }
                final BindingSet result = this.tuple;
                this.tuple = null;
                return result;
            }

            @Override
            public void remove() throws QueryEvaluationException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws QueryEvaluationException {
                if (this.statement != null) {

                    final Future<?> future = Data.getExecutor().schedule(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                end(false);
                                LOGGER.warn(VirtuosoTransaction.this
                                        + " - forced closure of Virtuoso transaction "
                                        + "after unsuccessfull attempt at closing Virtuoso iteration");
                            } catch (final Throwable ex) {
                                LOGGER.debug(VirtuosoTransaction.this
                                        + " - failed to close Virtuoso transaction after "
                                        + "unsuccessfull attempt at closing Virtuoso iteration",
                                        ex);
                            }
                        }

                    }, 1000, TimeUnit.MILLISECONDS);

                    try {
                        // Try to close 'politely' the Virtuoso cursor and the associated
                        // statements. After 1 second blocked in this operation, we will force
                        // closure of the whole triple transaction, which in turn may cause the
                        // killing of the Virtuoso JDBC connection (after one second waiting for
                        // politely closing it)
                        this.cursor.close();
                        this.statement.close();
                        future.cancel(false);

                    } catch (final SQLException e) {
                        throw new QueryEvaluationException(e);

                    } finally {
                        this.statement = null;
                        this.cursor = null;
                    }
                }
            }

            @Override
            protected void finalize() throws Throwable {
                Util.closeQuietly(this);
            }

            private BindingSet advance() throws QueryEvaluationException {
                try {
                    final BindingSet result = null;
                    if (this.cursor != null) {
                        if (this.cursor.next()) {
                            final int size = this.variables.size();
                            final Value[] values = new Value[size];
                            for (int i = 0; i < size; ++i) {
                                values[i] = castValue(this.cursor.getObject(this.variables.get(i)));
                            }
                            return new ListBindingSet(this.variables, values);
                        } else {
                            close();
                        }
                    }
                    return result;
                } catch (final Exception ex) {
                    if (isPartialResultException(ex)) {
                        // On partial results, terminate the iteration ignoring the exception
                        LOGGER.debug("{} - partial results returned due to expired timeout",
                                VirtuosoTransaction.this);
                        return null;
                    }
                    throw new QueryEvaluationException("Could not retrieve next query result", ex);
                }
            }

        }

    }

}
