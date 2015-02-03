package eu.fbk.knowledgestore;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
import eu.fbk.knowledgestore.Outcome.Status;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

public abstract class AbstractSession implements Session {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSession.class);

    private static final String MDC_ATTRIBUTE = "context";

    private static final long GRACE_PERIOD = 5000; // 5 sec in addition to the timeout set

    private static final long CLOSE_WAIT_TIME = 1000;

    private static long invocationCounter = 0;

    // Session-scoped variables

    private final Map<String, String> namespaces;

    @Nullable
    private final String username;

    @Nullable
    private final String password;

    private final long creationTime;

    private final WeakHashMap<Closeable, ?> pendingCloseables;

    private final AtomicBoolean closed;

    // Request-scoped variables

    private long timestamp;

    @Nullable
    private String operation;

    @Nullable
    private URI recordType;

    @Nullable
    private Class<?> streamType;

    @Nullable
    private URI objectID;

    @Nullable
    private URI invocationID;

    @Nullable
    private String oldMDCAttribute;

    private long numCreated;

    private long numDeleted;

    private long numModified;

    private long numUnmodified;

    private final List<Outcome> failedOutcomes;

    @Nullable
    private ScheduledFuture<?> interruptFuture;

    @Nullable
    private Thread interruptThread;

    protected AbstractSession(@Nullable final Map<String, String> namespaces,
            @Nullable final String username, @Nullable final String password) {
        this.namespaces = namespaces != null ? namespaces : Data.newNamespaceMap();
        this.username = username;
        this.creationTime = System.currentTimeMillis();
        this.password = password;
        this.pendingCloseables = new WeakHashMap<Closeable, Object>();
        this.closed = new AtomicBoolean(false);
        this.failedOutcomes = Lists.newArrayList();
    }

    @Nullable
    private void start(final String operation, @Nullable final URI recordType,
            @Nullable final Class<?> streamType, @Nullable final URI objectID, final Long timeout) {

        this.timestamp = System.currentTimeMillis();
        this.operation = operation;
        this.recordType = recordType;
        this.streamType = streamType;
        this.objectID = objectID;
        this.numCreated = 0L;
        this.numDeleted = 0L;
        this.numModified = 0L;
        this.numUnmodified = 0L;
        this.failedOutcomes.clear();
        this.oldMDCAttribute = MDC.get(MDC_ATTRIBUTE);

        URI invocationID = null;
        if (this.oldMDCAttribute != null) {
            try {
                invocationID = Data.getValueFactory().createURI(this.oldMDCAttribute);
            } catch (final Throwable ex) {
                // not valid
            }
        }
        if (invocationID == null) {
            final long ts = System.currentTimeMillis();
            final long counter;
            synchronized (AbstractSession.class) {
                ++AbstractSession.invocationCounter;
                if (AbstractSession.invocationCounter < ts) {
                    AbstractSession.invocationCounter = ts;
                }
                counter = AbstractSession.invocationCounter;
            }
            invocationID = Data.getValueFactory().createURI("req:" + Long.toString(counter, 32));
        }
        setInvocationID(invocationID);

        if (timeout != null) {
            final Thread thread = Thread.currentThread();
            this.interruptFuture = Data.getExecutor().schedule(new Runnable() {

                @Override
                public void run() {
                    thread.interrupt();
                }

            }, timeout + GRACE_PERIOD, TimeUnit.MILLISECONDS);
        }

        synchronized (this.closed) {
            this.interruptThread = Thread.currentThread();
        }
    }

    private void end() throws OperationException {
        try {
            if (this.interruptFuture != null && !this.interruptFuture.isDone()) {
                if (!this.interruptFuture.cancel(false)) {
                    try {
                        this.interruptFuture.get(); // being interrupted; await interruption
                    } catch (final Throwable ex) {
                        // ignore
                    }
                }
            }
            if (!this.failedOutcomes.isEmpty()) {
                throw fail(null); // thrown only if operation completed with some failed outcomes
            }
        } finally {
            MDC.put(MDC_ATTRIBUTE, this.oldMDCAttribute);
            this.interruptFuture = null;
            this.oldMDCAttribute = null;
            synchronized (this.closed) {
                this.interruptThread = null;
                Thread.interrupted(); // cancel interrupted status
                this.closed.notify(); // notify thread blocked on close(), if any
            }
        }
    }

    private <T extends Closeable> T filter(@Nullable final T closeable) {
        if (closeable != null) {
            this.pendingCloseables.put(closeable, null);
        }
        return closeable;
    }

    @Nullable
    private <E> Stream<E> filter(@Nullable final Stream<E> stream) {
        if (stream == null) {
            return null;
        }
        Stream<E> result = stream;
        if (this.interruptFuture != null) {
            result = stream.setTimeout(System.currentTimeMillis()
                    + this.interruptFuture.getDelay(TimeUnit.MILLISECONDS));
        }
        this.pendingCloseables.put(result, null);

        return result;
    }

    private Handler<Outcome> filter(@Nullable final Handler<? super Outcome> handler) {
        return new Handler<Outcome>() {

            private boolean log = true;

            @Override
            public void handle(final Outcome outcome) throws Throwable {
                if (outcome != null) {
                    final Status status = outcome.getStatus();
                    if (status.isError()) {
                        AbstractSession.this.failedOutcomes.add(outcome);
                    } else if (status == Status.OK_CREATED) {
                        ++AbstractSession.this.numCreated;
                    } else if (status == Status.OK_DELETED) {
                        ++AbstractSession.this.numDeleted;
                    } else if (status == Status.OK_MODIFIED) {
                        ++AbstractSession.this.numModified;
                    } else {
                        ++AbstractSession.this.numUnmodified;
                    }
                }
                if (handler != null) {
                    try {
                        handler.handle(outcome);
                    } catch (final Throwable ex) {
                        if (this.log) {
                            this.log = false;
                            LOGGER.error("Handler failure: iteration being interrupted, "
                                    + "no additional exception from handler will be logged", ex);
                        }
                        Thread.currentThread().interrupt();
                    }
                }
            }

        };
    }

    private OperationException fail(@Nullable final Throwable ex) {
        if (!this.failedOutcomes.isEmpty()) {
            final int size = this.failedOutcomes.size();
            final List<Throwable> causes = Lists.newArrayListWithCapacity(size);
            for (final Outcome failedOutcome : this.failedOutcomes) {
                causes.add(new OperationException(failedOutcome));
            }
            this.failedOutcomes.clear();
            if (ex != null) {
                causes.add(ex);
            }
            return new OperationException(outcome(Status.ERROR_BULK, null), causes);
        } else if (ex instanceof OperationException) {
            return (OperationException) ex;
        } else if (ex != null) {
            final AtomicReference<String> message = new AtomicReference<String>(ex.getMessage());
            Status status = Status.ERROR_UNEXPECTED;
            try {
                status = doFail(ex, message);
            } catch (final Throwable ex2) {
                LOGGER.warn("Could not map " + ex.getClass().getSimpleName()
                        + " to status / message", ex);
            }
            return new OperationException(outcome(status, message.get()), ex);
        } else {
            return new OperationException(outcome(Status.ERROR_UNEXPECTED, null));
        }
    }

    private Outcome outcome(final Status status, @Nullable final String message) {
        final long elapsed = System.currentTimeMillis() - this.timestamp;
        final StringBuilder builder = new StringBuilder();
        final long numObjects = this.numCreated + this.numDeleted + this.numModified
                + this.numUnmodified;
        if (numObjects > 0 || status == Status.OK_BULK || status == Status.ERROR_BULK) {
            builder.append(numObjects).append(' ')
                    .append(this.recordType.getLocalName().toLowerCase()).append("(s) involved");
            if (this.numCreated > 0) {
                builder.append(", ").append(this.numCreated).append(" created");
            }
            if (this.numModified > 0) {
                builder.append(", ").append(this.numModified).append(" modified");
            }
            if (this.numUnmodified > 0) {
                builder.append(", ").append(this.numUnmodified).append(" unmodified");
            }
            if (this.numDeleted > 0) {
                builder.append(", ").append(this.numDeleted).append(" deleted");
            }
            if (!this.failedOutcomes.isEmpty()) {
                builder.append(", ").append(this.failedOutcomes.size()).append(" failed");
            }
            if (numObjects == 0) {
                if (message != null) {
                    builder.append(", ");
                }
            }
        }
        if (message != null) {
            builder.append(message);
        }
        if (builder.length() > 0) {
            builder.append(", ");
        }
        builder.append(elapsed).append(" ms");
        return Outcome.create(status, this.invocationID, this.objectID, builder.toString());
    }

    private void logRequest(final Object... args) {
        if (LOGGER.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.operation.toUpperCase());
            if (this.recordType != null) {
                builder.append(' ').append(this.recordType.getLocalName().toUpperCase());
            }
            String separator = " ";
            for (int i = 0; i < args.length; i += 2) {
                final Object name = args[i];
                final Object value = args[i + 1];
                if (value != null) {
                    builder.append(separator);
                    if (name != null) {
                        builder.append(name).append('=');
                    }
                    if (value instanceof Collection<?> && ((Collection<?>) value).size() > 10) {
                        builder.append("[...").append(((Collection<?>) value).size())
                                .append(" elements...]");
                    } else {
                        builder.append(value);
                    }
                    separator = ", ";
                }
            }
            LOGGER.info(builder.toString());
        }
    }

    private <T> T logResponse(final T result) {
        if (LOGGER.isInfoEnabled()) {
            final long elapsed = System.currentTimeMillis() - this.timestamp;
            if (result instanceof Outcome) {
                final Outcome outcome = (Outcome) result;
                final String message = outcome.getMessage();
                LOGGER.info("Result: {}{}, {} ms", outcome.getStatus(), message == null ? ""
                        : ", " + message, elapsed);
            } else if (result instanceof Long) {
                LOGGER.info("Result: {}, {} ms", result, elapsed);
            } else if (result instanceof Representation) {
                final Record meta = ((Representation) result).getMetadata();
                final String file = meta.getUnique(NFO.FILE_NAME, String.class, "unnamed file");
                final String type = meta.getUnique(NIE.MIME_TYPE, String.class, "unknown type");
                final long size = meta.getUnique(NFO.FILE_SIZE, Long.class, -1L);
                LOGGER.info("Result: {}, {}, {}, {} ms", file, type, size >= 0 ? size + " bytes"
                        : "unknown size", elapsed);
            } else if (result instanceof Stream) {
                LOGGER.info("Result: {} stream, {} ms",
                        this.streamType == BindingSet.class ? "tuple" : this.streamType
                                .getSimpleName().toLowerCase(), elapsed);
            } else if (result == null) {
                LOGGER.info("Result: null, {} ms", elapsed);
            }
        }
        return result;
    }

    @Override
    @Nullable
    public final String getUsername() throws IllegalStateException {
        checkNotClosed();
        return this.username;
    }

    @Override
    public final String getPassword() throws IllegalStateException {
        checkNotClosed();
        return this.password;
    }

    @Override
    public final Map<String, String> getNamespaces() throws IllegalStateException {
        checkNotClosed();
        return this.namespaces;
    }

    @Override
    public final Download download(final URI resourceID) throws IllegalStateException {
        checkNotClosed();
        return new Download(this.namespaces, resourceID) {

            @Override
            @Nullable
            protected Representation doExec(@Nullable final Long timeout, final URI id,
                    @Nullable final Set<String> mimeTypes, final boolean caching)
                    throws OperationException {

                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("DOWNLOAD", null, null, id, timeout);
                    try {
                        logRequest(null, id, "accept", mimeTypes, //
                                null, caching ? null : "no caching", "timeout", timeout);
                        return logResponse(filter(doDownload(timeout, id, mimeTypes, caching)));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }

            }

        };
    }

    @Override
    public final Upload upload(final URI resourceID) throws IllegalStateException {
        checkNotClosed();
        return new Upload(this.namespaces, resourceID) {

            @Override
            protected Outcome doExec(@Nullable final Long timeout, final URI id,
                    @Nullable final Representation representation) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("UPLOAD", null, null, id, timeout);
                    try {
                        logRequest(null, id, "content", representation != null, "timeout", timeout);
                        return logResponse(doUpload(timeout, resourceID, representation));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Count count(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Count(this.namespaces, type) {

            @Override
            protected long doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final XPath condition, @Nullable final Set<URI> ids)
                    throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("COUNT", type, null, null, timeout);
                    try {
                        logRequest(null, condition, "ids", ids, "timeout", timeout);
                        return logResponse(doCount(timeout, type, condition, ids));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Retrieve retrieve(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Retrieve(this.namespaces, type) {

            @Override
            protected Stream<Record> doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final XPath condition, @Nullable final Set<URI> ids,
                    @Nullable final Set<URI> properties, @Nullable final Long offset,
                    @Nullable final Long limit) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("RETRIEVE", type, Record.class, null, timeout);
                    try {
                        logRequest(null, condition, "ids", ids, "props", properties, //
                                "offset", offset, "limit", limit, "timeout", timeout);
                        return logResponse(doRetrieve(timeout, type, condition, ids, properties,
                                offset, limit));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Create create(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Create(this.namespaces, type) {

            @Override
            protected Outcome doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final Stream<? extends Record> records,
                    final Handler<? super Outcome> handler) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("CREATE", type, null, null, timeout);
                    try {
                        logRequest("timeout", timeout);
                        doCreate(timeout, type, records, filter(handler));
                        return logResponse(outcome(Status.OK_BULK, null));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Merge merge(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Merge(this.namespaces, type) {

            @Override
            protected Outcome doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final Stream<? extends Record> records,
                    @Nullable final Criteria criteria, final Handler<? super Outcome> handler)
                    throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("MERGE", type, null, null, timeout);
                    try {
                        logRequest(null, criteria, "timeout", timeout);
                        doMerge(timeout, type, records, criteria, filter(handler));
                        return logResponse(outcome(Status.OK_BULK, null));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Update update(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Update(this.namespaces, type) {

            @Override
            protected Outcome doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final XPath condition, @Nullable final Set<URI> ids,
                    @Nullable final Record record, @Nullable final Criteria criteria,
                    final Handler<? super Outcome> handler) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("UPDATE", type, null, null, timeout);
                    try {
                        logRequest(null, criteria, null, condition, "ids", ids, "timeout", timeout);
                        doUpdate(timeout, type, condition, ids, record, criteria, filter(handler));
                        return logResponse(outcome(Status.OK_BULK, null));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Delete delete(final URI type) throws IllegalStateException {
        checkNotClosed();
        return new Delete(this.namespaces, type) {

            @Override
            protected Outcome doExec(@Nullable final Long timeout, final URI type,
                    @Nullable final XPath condition, @Nullable final Set<URI> ids,
                    final Handler<? super Outcome> handler) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("DELETE", type, null, null, timeout);
                    try {
                        logRequest(null, condition, "ids", ids, "timeout", timeout);
                        doDelete(timeout, type, condition, ids, filter(handler));
                        return logResponse(outcome(Status.OK_BULK, null));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Match match() throws IllegalStateException {
        checkNotClosed();
        return new Match(this.namespaces) {

            @Override
            protected Stream<Record> doExec(@Nullable final Long timeout,
                    final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
                    final Map<URI, Set<URI>> properties) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("MATCH", null, Record.class, null, timeout);
                    try {
                        logRequest(null, conditions, "ids", ids, "props", //
                                properties, "timeout", timeout);
                        return logResponse(filter(doMatch(timeout, conditions, ids, properties)));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final Sparql sparql(final String expression, final Object... arguments)
            throws IllegalStateException {
        checkNotClosed();
        return new Sparql(this.namespaces, expression, arguments) {

            @Override
            protected <T> Stream<T> doExec(@Nullable final Long timeout, final Class<T> type,
                    final String expression, @Nullable final Set<URI> defaultGraphs,
                    @Nullable final Set<URI> namedGraphs) throws OperationException {
                synchronized (AbstractSession.this) {
                    checkNotClosed();
                    start("SPARQL", null, type, null, timeout);
                    try {
                        logRequest("from", defaultGraphs, "from-named", namedGraphs, //
                                "timeout", timeout, null, expression);
                        return logResponse(filter(doSparql(timeout, type, expression,
                                defaultGraphs, namedGraphs)));
                    } catch (final Throwable ex) {
                        throw fail(ex);
                    } finally {
                        end();
                    }
                }
            }

        };
    }

    @Override
    public final boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public final void close() {
        if (this.closed.compareAndSet(false, true)) {
            synchronized (this.closed) {
                if (this.interruptThread != null) {
                    this.interruptThread.interrupt(); // attempt interrupting current operation
                    try {
                        this.closed.wait(CLOSE_WAIT_TIME);

                    } catch (final InterruptedException ex) {
                        // ignore and proceed with closing resources
                    }
                }
            }
            for (final Closeable closeable : this.pendingCloseables.keySet()) {
                try {
                    closeable.close();
                } catch (final Throwable ex) {
                    LOGGER.error("Error closing " + closeable.getClass().getSimpleName()
                            + " after session has been closed", ex);
                }
            }
            doClose(); // custom close actions
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + (isClosed() ? "closed" : "open") + ", "
                + this.username + ", " + new Date(this.creationTime).toString() + ")";
    }

    protected final void checkNotClosed() {
        if (this.closed.get()) {
            throw new IllegalStateException("Session has been closed");
        }
    }

    protected final URI getInvocationID() {
        return this.invocationID;
    }

    protected final void setInvocationID(final URI invocationID) {
        this.invocationID = invocationID;
        MDC.put(MDC_ATTRIBUTE, invocationID == null ? null : invocationID.stringValue());
    }

    protected Status doFail(final Throwable ex, final AtomicReference<String> message)
            throws Throwable {
        return Status.ERROR_UNEXPECTED;
    }

    protected abstract Representation doDownload(@Nullable Long timeout, final URI id,
            @Nullable final Set<String> mimeTypes, final boolean useCaches) throws Throwable;

    protected abstract Outcome doUpload(@Nullable Long timeout, final URI resourceID,
            @Nullable final Representation representation) throws Throwable;

    protected abstract long doCount(@Nullable Long timeout, final URI type,
            @Nullable final XPath condition, @Nullable final Set<URI> ids) throws Throwable;

    protected abstract Stream<Record> doRetrieve(@Nullable Long timeout, final URI type,
            @Nullable final XPath condition, @Nullable final Set<URI> ids,
            @Nullable final Set<URI> properties, @Nullable Long offset, @Nullable Long limit)
            throws Throwable;

    protected abstract void doCreate(@Nullable Long timeout, final URI type,
            @Nullable final Stream<? extends Record> records,
            final Handler<? super Outcome> handler) throws Throwable;

    protected abstract void doMerge(@Nullable Long timeout, final URI type,
            @Nullable final Stream<? extends Record> records, @Nullable final Criteria criteria,
            final Handler<? super Outcome> handler) throws Throwable;

    protected abstract void doUpdate(@Nullable Long timeout, final URI type,
            @Nullable final XPath condition, @Nullable final Set<URI> ids,
            @Nullable final Record record, @Nullable final Criteria criteria,
            final Handler<? super Outcome> handler) throws Throwable;

    protected abstract void doDelete(@Nullable Long timeout, final URI type,
            @Nullable final XPath condition, @Nullable final Set<URI> ids,
            final Handler<? super Outcome> handler) throws Throwable;

    protected abstract Stream<Record> doMatch(@Nullable Long timeout,
            final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
            final Map<URI, Set<URI>> properties) throws Throwable;

    protected abstract <T> Stream<T> doSparql(@Nullable Long timeout, final Class<T> type,
            final String expression, @Nullable final Set<URI> defaultGraphs,
            @Nullable final Set<URI> namedGraphs) throws Throwable;

    protected void doClose() {
    }

}
