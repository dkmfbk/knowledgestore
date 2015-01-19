package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Util;

/**
 * A {@code FileStore} wrapper that log calls to the operations of a wrapped {@code FileStore} and
 * their execution times.
 * <p>
 * This wrapper intercepts calls to an underlying {@code FileStore} and logs request information
 * and execution times via SLF4J (level DEBUG, logger named after this class). The overhead
 * introduced by this wrapper when logging is disabled is negligible.
 * </p>
 */
public class LoggingFileStore extends ForwardingFileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFileStore.class);

    private final FileStore delegate;

    /**
     * Creates a new instance for the wrapped {@code FileStore} specified.
     * 
     * @param delegate
     *            the wrapped {@code FileStore}
     */
    public LoggingFileStore(final FileStore delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
        LOGGER.debug("{} configured", getClass().getSimpleName());
    }

    @Override
    protected FileStore delegate() {
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
                LOGGER.debug("{} - {} closed after {} ms, {} files, eof={}",
                        LoggingFileStore.this, name, System.currentTimeMillis() - ts, count, eof);
            }

        });
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
    public InputStream read(final String filename) throws FileMissingException, IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            final InputStream result = Util.interceptClose(super.read(filename), new Runnable() {

                @Override
                public void run() {
                    LOGGER.debug("{} - {} input stream closed in {} ms", LoggingFileStore.this,
                            filename, System.currentTimeMillis() - ts);
                }

            });
            LOGGER.debug("{} - {} opened for read in {} ms", this, filename,
                    System.currentTimeMillis() - ts);
            return result;
        } else {
            return super.read(filename);
        }
    }

    @Override
    public OutputStream write(final String filename) throws FileExistsException, IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            final OutputStream result = Util.interceptClose(super.write(filename), new Runnable() {

                @Override
                public void run() {
                    LOGGER.debug("{} - {} output stream closed in {} ms", LoggingFileStore.this,
                            filename, System.currentTimeMillis() - ts);
                }

            });
            LOGGER.debug("{} - {} opened for write in {} ms", this, filename,
                    System.currentTimeMillis() - ts);
            return result;
        } else {
            return super.write(filename);
        }
    }

    @Override
    public void delete(final String filename) throws FileMissingException, IOException {
        if (LOGGER.isDebugEnabled()) {
            final long ts = System.currentTimeMillis();
            super.delete(filename);
            LOGGER.debug("{} - {} deleted in {} ms", this, filename, //
                    System.currentTimeMillis() - ts);
        } else {
            super.delete(filename);
        }
    }

    @Override
    public Stream<String> list() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final String name = "list() result stream";
            final long ts = System.currentTimeMillis();
            final Stream<String> result = logClose(super.list(), name, ts);
            LOGGER.debug("{} - {} obtained in {} ms", this, name, System.currentTimeMillis() - ts);
            return result;
        } else {
            return super.list();
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
}
