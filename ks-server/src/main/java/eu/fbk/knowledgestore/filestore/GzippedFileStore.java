package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Stream;

/**
 * A {@code FileStore} decorator that GZIPs all the compressible files written to it.
 * <p>
 * A {@code GzippedFileStore} intercepts reads and writes to an underlying {@code FileStore},
 * respectively applying GZIP compression and decompression to stored files in case they are
 * compressible. Compression level and size of buffer used for compression / decompression can be
 * configured by the user. Whether a file can be compressed is detected starting from its
 * extension and the matching Internet MIME type using the facilities of {@link Data}. In case
 * compression is applied, the name of the stored file is changed adding a {@code .gz} suffix.
 * </p>
 */
public final class GzippedFileStore extends ForwardingFileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(GzippedFileStore.class);

    private static final int DEFAULT_COMPRESSION_LEVEL = Deflater.DEFAULT_COMPRESSION;

    private static final int DEFAULT_BUFFER_SIZE = 512;

    private final FileStore delegate;

    private final int compressionLevel;

    private final int bufferSize;

    /**
     * Creates a new instance wrapping the {@code FileStore} supplied and using the default
     * compression level and buffer size.
     *
     * @param delegate
     *            the wrapped {@code FileStore}
     * @see #GzippedFileStore(FileStore, Integer, Integer)
     */
    public GzippedFileStore(final FileStore delegate) {
        this(delegate, null, null);
    }

    /**
     * Creates a new instance wrapping the {@code FileStore} supplied and using the specified
     * compression level.
     *
     * @param delegate
     *            the wrapped {@code FileStore}, not null
     * @param compressionLevel
     *            the desired compression level for compressible files, on a 0-9 scale (from no
     *            compression to best compression) or -1 for default compression; if null defaults
     *            to -1
     * @param bufferSize
     *            the size of the buffer used by the GZIP inflaters / deflaters; if null defaults
     *            to 512
     */
    public GzippedFileStore(final FileStore delegate, @Nullable final Integer compressionLevel,
            @Nullable final Integer bufferSize) {

        Preconditions.checkNotNull(delegate);
        Preconditions.checkArgument(compressionLevel == null
                || compressionLevel == Deflater.DEFAULT_COMPRESSION //
                || compressionLevel >= Deflater.BEST_SPEED
                && compressionLevel <= Deflater.BEST_COMPRESSION);
        Preconditions.checkArgument(bufferSize > 0);

        this.delegate = Preconditions.checkNotNull(delegate);
        this.compressionLevel = MoreObjects.firstNonNull(compressionLevel,
                DEFAULT_COMPRESSION_LEVEL);
        this.bufferSize = MoreObjects.firstNonNull(bufferSize, DEFAULT_BUFFER_SIZE);

        LOGGER.info("GZippedFileStore configured, compression={}, buffer={}", compressionLevel,
                bufferSize);
    }

    @Override
    protected FileStore delegate() {
        return this.delegate;
    }

    @Override
    public InputStream read(final String filename) throws FileMissingException, IOException {
        final String internalFilename = toInternalFilename(filename);
        if (internalFilename.equals(filename)) {
            return super.read(filename);
        } else {
            return new GZIPInputStream(super.read(internalFilename), this.bufferSize);
        }
    }

    @Override
    public OutputStream write(final String filename) throws FileExistsException, IOException {
        final String internalFilename = toInternalFilename(filename);
        if (internalFilename.equals(filename)) {
            return super.write(filename);
        } else {
            return new GZIPOutputStream(super.write(internalFilename), this.bufferSize) {

                {
                    this.def.setLevel(GzippedFileStore.this.compressionLevel);
                }

            };
        }
    }

    @Override
    public void delete(final String filename) throws FileMissingException, IOException {
        super.delete(toInternalFilename(filename));
    }

    @Override
    public Stream<String> list() throws IOException {
        return super.list().transform(new Function<String, String>() {

            @Override
            public String apply(final String filename) {
                return toExternalFilename(filename);
            }

        }, 0);
    }

    private static String toInternalFilename(final String filename) {
        final String mimeType = Data.extensionToMimeType(filename);
        return Data.isMimeTypeCompressible(mimeType) ? filename + ".gz" : filename;
    }

    private static String toExternalFilename(final String filename) {
        return filename.endsWith(".gz") ? filename.substring(0, filename.length() - 3) : filename;
    }

}
