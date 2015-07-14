package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.runtime.Synchronizer;

/**
 * A {@code FileStore} wrapper that synchronizes and enforces a proper access to an another
 * {@code FileStore}.
 * <p>
 * This wrapper provides the following guarantees with respect to external access to the wrapped
 * {@link FileStore}:
 * <ul>
 * <li>operations are started and committed according to the synchronization strategy enforced by
 * a supplied {@link Synchronizer};</li>
 * <li>access to the wrapped {@code DataStore} and its {@code DataTransaction} is enforced to
 * occur strictly in adherence with the lifecycle defined for {@link Component}s (
 * {@code IllegalStateException}s are thrown to the caller otherwise);</li>
 * <li>before the {@code FileStore} is closed, all pending {@code FileStore#list()} operations are
 * terminated.</li>
 * </ul>
 * </p>
 */
public final class SynchronizedFileStore extends ForwardingFileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedFileStore.class);

    private static final int NUM_LOCKS = 255;

    private static final int NEW = 0;

    private static final int INITIALIZED = 1;

    private static final int CLOSED = 2;

    private final FileStore delegate;

    private final Synchronizer synchronizer;

    private final AtomicInteger state;

    private final Object[] fileLocks;

    private final Set<Stream<String>> pendingListStreams;

    /**
     * Creates a new instance for the wrapped {@code FileStore} and the {@code Synchronizer}
     * specification string supplied.
     *
     * @param delegate
     *            the wrapped {@code FileStore}
     * @param synchronizerSpec
     *            the synchronizer specification string (see {@link Synchronizer})
     */
    public SynchronizedFileStore(final FileStore delegate, final String synchronizerSpec) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.synchronizer = Synchronizer.create(synchronizerSpec);
        this.state = new AtomicInteger(NEW);
        this.fileLocks = new Object[NUM_LOCKS];
        for (int i = 0; i < NUM_LOCKS; ++i) {
            this.fileLocks[i] = new Object();
        }
        this.pendingListStreams = Sets.newHashSet();
    }

    @Override
    protected FileStore delegate() {
        return this.delegate;
    }

    private void checkState(final int expected) {
        final int state = this.state.get();
        if (state != expected) {
            throw new IllegalStateException("FileStore "
                    + (state == NEW ? "not initialized"
                            : state == INITIALIZED ? "already initialized" : "already closed"));
        }
    }

    private Object lockFor(final String fileName) {
        return this.fileLocks[fileName.hashCode() % 0x7FFFFFFF % NUM_LOCKS];
    }

    @Override
    public void init() throws IOException {
        checkState(NEW);
        super.init();
        this.state.set(INITIALIZED);
    }

    @Override
    public InputStream read(final String fileName) throws FileMissingException, IOException {
        checkState(INITIALIZED);
        Preconditions.checkNotNull(fileName);
        this.synchronizer.beginTransaction(true);
        try {
            checkState(INITIALIZED);
            synchronized (lockFor(fileName)) {
                return super.read(fileName);
            }
        } finally {
            this.synchronizer.endTransaction(true);
        }
    }

    @Override
    public OutputStream write(final String fileName) throws FileExistsException, IOException {
        checkState(INITIALIZED);
        Preconditions.checkNotNull(fileName);
        this.synchronizer.beginTransaction(false);
        try {
            checkState(INITIALIZED);
            synchronized (lockFor(fileName)) {
                return super.write(fileName);
            }
        } finally {
            this.synchronizer.endTransaction(false);
        }
    }

    @Override
    public void delete(final String fileName) throws FileMissingException, IOException {
        checkState(INITIALIZED);
        Preconditions.checkNotNull(fileName);
        this.synchronizer.beginTransaction(false);
        try {
            checkState(INITIALIZED);
            synchronized (lockFor(fileName)) {
                super.delete(fileName);
            }
        } finally {
            this.synchronizer.endTransaction(false);
        }
    }

    @Override
    public Stream<String> list() throws IOException {

        checkState(INITIALIZED);

        this.synchronizer.beginTransaction(true);
        try {
            checkState(INITIALIZED);
            final Stream<String> stream = super.list();
            synchronized (this.pendingListStreams) {
                this.pendingListStreams.add(stream);
            }
            stream.onClose(new Runnable() {

                @Override
                public void run() {
                    SynchronizedFileStore.this.synchronizer.endTransaction(true);
                    synchronized (SynchronizedFileStore.this.pendingListStreams) {
                        SynchronizedFileStore.this.pendingListStreams.remove(this);
                    }
                }

            });
            return stream;

        } catch (final Throwable ex) {
            this.synchronizer.endTransaction(true);
            Throwables.propagateIfPossible(ex, IOException.class);
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public void close() {
        if (!this.state.compareAndSet(INITIALIZED, CLOSED)
                && !this.state.compareAndSet(NEW, CLOSED)) {
            return;
        }
        List<Stream<String>> streamsToEnd;
        synchronized (this.pendingListStreams) {
            streamsToEnd = Lists.newArrayList(this.pendingListStreams);
        }
        try {
            for (final Stream<String> stream : streamsToEnd) {
                try {
                    LOGGER.warn("Forcing closure of stream due to FileStore closure");
                    stream.close();
                } catch (final Throwable ex) {
                    LOGGER.error("Exception caught while closing stream: " + ex.getMessage(), ex);
                }
            }
        } finally {
            super.close();
        }
    }

}
