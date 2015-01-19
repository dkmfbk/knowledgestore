package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;

/**
 * A {@code DataStore} wrapper that log calls to the operations of a wrapped {@code DataStore} and
 * their execution times.
 * <p>
 * This wrapper intercepts calls to an underlying {@code DataStore} and to the
 * {@code DataTransaction}s it creates, and logs request information and execution times via SLF4J
 * (level DEBUG, logger named after this class). The overhead introduced by this wrapper when
 * logging is disabled is negligible.
 * </p>
 */
public final class LoggingDataStore extends ForwardingDataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataStore.class);

    private final DataStore delegate;

    /**
     * Creates a new instance for the wrapped {@code DataStore} specified.
     * 
     * @param delegate
     *            the wrapped {@code DataStore}
     */
    public LoggingDataStore(final DataStore delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
        LOGGER.debug("{} configured", getClass().getSimpleName());
    }

    @Override
    protected DataStore delegate() {
        return this.delegate;
    }

    @Override
    public void init() throws IOException {

        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.init();
            LOGGER.debug("{} - initialized in {} ms", this, System.currentTimeMillis() - ts);
        } else {
            super.init();
        }
    }

    @Override
    public DataTransaction begin(final boolean readOnly) throws IOException, IllegalStateException {

        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            final DataTransaction transaction = new LoggingDataTransaction(super.begin(readOnly));
            LOGGER.debug("{} - started in {} mode in {} ms", transaction, readOnly ? "read-only"
                    : "read-write", System.currentTimeMillis() - ts);
            return transaction;
        } else {
            return super.begin(readOnly);
        }
    }

    @Override
    public void close() {

        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.close();
            LOGGER.debug("{} - closed in {} ms", this, System.currentTimeMillis() - ts);
        } else {
            super.close();
        }
    }

    private static final class LoggingDataTransaction extends ForwardingDataTransaction {

        private final DataTransaction delegate;

        LoggingDataTransaction(final DataTransaction delegate) {
            this.delegate = Preconditions.checkNotNull(delegate);
        }

        @Override
        protected DataTransaction delegate() {
            return this.delegate;
        }

        @Nullable
        private <T> Stream<T> logClose(@Nullable final Stream<T> stream, final String name,
                final long ts) {
            final AtomicLong count = new AtomicLong(0);
            final AtomicBoolean eof = new AtomicBoolean(false);
            return stream.track(count, eof).onClose(new Runnable() {

                @Override
                public void run() {
                    LOGGER.debug("{} - {} closed after {} ms, {} records, eof={}",
                            LoggingDataTransaction.this, name, System.currentTimeMillis() - ts,
                            count, eof);
                }

            });
        }

        @Override
        public Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
                @Nullable final Set<? extends URI> properties) throws IOException,
                IllegalArgumentException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final String name = "lookup() result stream for "
                        + Data.toString(type, Data.getNamespaceMap()) + ", " + ids.size() + " ids"
                        + (properties == null ? "" : ", " + properties.size() + " properties");
                final long ts = System.currentTimeMillis();
                final Stream<Record> result = super.lookup(type, ids, properties);
                LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis()
                        - ts);
                return logClose(result, name, ts);
            } else {
                return super.lookup(type, ids, properties);
            }
        }

        @Override
        public Stream<Record> retrieve(final URI type, @Nullable final XPath condition,
                @Nullable final Set<? extends URI> properties) throws IOException,
                IllegalArgumentException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final String name = "retrieve() result stream for "
                        + Data.toString(type, Data.getNamespaceMap())
                        + (condition == null ? "" : ", " + condition.toString())
                        + (properties == null ? "" : ", " + properties.size() + " properties");
                final long ts = System.currentTimeMillis();
                final Stream<Record> result = super.retrieve(type, condition, properties);
                LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis()
                        - ts);
                return logClose(result, name, ts);
            } else {
                return super.retrieve(type, condition, properties);
            }
        }

        @Override
        public long count(final URI type, @Nullable final XPath condition) throws IOException,
                IllegalArgumentException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} - counting {}{}", this,
                        Data.toString(type, Data.getNamespaceMap()), //
                        condition == null ? "" : ", " + condition.toString());
                final long ts = System.currentTimeMillis();
                final long result = super.count(type, condition);
                LOGGER.debug("{} - count result {} obtained in {} ms", this, result,
                        System.currentTimeMillis() - ts);
                return result;
            } else {
                return super.count(type, condition);
            }
        }

        @Override
        public Stream<Record> match(final Map<URI, XPath> conditions,
                final Map<URI, Set<URI>> ids, final Map<URI, Set<URI>> properties)
                throws IOException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} - evaluating match: conditions={}, ids={}, properties={}", this,
                        conditions, ids, properties);
                final String name = "match() result stream";
                final long ts = System.currentTimeMillis();
                final Stream<Record> result = super.match(conditions, ids, properties);
                LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis()
                        - ts);
                return logClose(result, name, ts);
            } else {
                return super.match(conditions, ids, properties);
            }
        }

        @Override
        public void store(final URI type, final Record record) throws IOException,
                IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final long ts = System.currentTimeMillis();
                super.store(type, record);
                LOGGER.debug("{} - {} {} stored in {} ms", this,
                        Data.toString(type, Data.getNamespaceMap()), record,
                        System.currentTimeMillis() - ts);
            } else {
                super.store(type, record);
            }
        }

        @Override
        public void delete(final URI type, final URI id) throws IOException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final long ts = System.currentTimeMillis();
                super.delete(type, id);
                LOGGER.debug("{} - {} {} deleted in {} ms", this,
                        Data.toString(type, Data.getNamespaceMap()), id,
                        System.currentTimeMillis() - ts);
            } else {
                super.delete(type, id);
            }
        }

        @Override
        public void end(final boolean commit) throws IOException, IllegalStateException {

            if (LOGGER.isDebugEnabled()) {
                final long ts = System.currentTimeMillis();
                super.end(commit);
                LOGGER.debug("{} - {} done in {} ms", this, commit ? "commit" : "rollback",
                        System.currentTimeMillis() - ts);
            } else {
                super.end(commit);
            }
        }

    }

}
