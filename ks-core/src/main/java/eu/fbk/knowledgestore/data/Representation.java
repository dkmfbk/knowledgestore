package eu.fbk.knowledgestore.data;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Date;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import com.google.common.net.MediaType;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.rdfpro.util.IO;

/**
 * A digital representation of a resource.
 * <p>
 * A {@code Representation} object provides access to the binary or character representation of a
 * resource, including associated (mutable) representation metadata ({@link #getMetadata()}) and
 * the resource ID ( {@link #getResourceID()}).
 * </p>
 * <p>
 * Representation data can be consumed either as a stream of bytes or as a stream of characters.
 * Conversion from one stream to another is performed if necessary using the charset encoded by
 * metadata attribute {@link NIE#MIME_TYPE}.
 * </p>
 * <p>
 * A representation encapsulates an open {@code InputStream} or {@code Reader} that provide access
 * to the representation data (see methods {@link #getInputStream()} and {@link #getReader()}),
 * hence it is a {@code Closeable} object that MUST be closed after use. In addition to these
 * methods, a number of {@code writeToXXX()} helper methods allow to consume the representation
 * data in different ways:
 * </p>
 * <ul>
 * <li>{@link #writeToByteArray()} returns a byte array with all the representation data;</li>
 * <li>{@link #writeToString()} returns a string with all the representation characater data.</li>
 * <li>{@link #writeTo(OutputStream)} writes all the binary representation data to a supplied
 * {@code OutputStream};</li>
 * <li>{@link #writeTo(Appendable)} writes the character representation data to a supplied
 * {@code Appendable};</li>
 * </ul>
 * <p>
 * Note that these methods exhaust the {@code InputStream} / {@code Reader} associated to the
 * representation. Moreover, in case some data has already been read, it will not be written by
 * those methods.
 * </p>
 * <p>
 * Representation objects are created via {@code create()} factory methods that take care of
 * acquiring both an {@code InputStream} / {@code Reader} over the data and the associated
 * metadata starting from a number of sources:
 * </p>
 * <ul>
 * <li>{@link #create(URI, InputStream)} builds a representation out of an {@code InputStream};</li>
 * <li>{@link #create(URI, Reader)} builds a representation out of a {@code Reader} ;</li>
 * <li>{@link #create(URI, byte[])} builds a representation out of a byte array, including
 * metadata about the representation length;</li>
 * <li>{@link #create(URI, CharSequence)} builds a representation out of a {@code CharSequence}
 * (e.g., a {@code String});</li>
 * <li>{@link #create(URI, File)} builds a representation out of a file, including metadata about
 * the file name, size, mime type (from the extension) and last modified time;</li>
 * <li>{@link #create(URI, URL)} builds a representation out of a resolvable URL, including
 * metadata about the file name, file size, mime type (from the extension), MD5 hash, last
 * modified time.</li>
 * </ul>
 * <p>
 * Representation objects are mutable but thread safe. Object equality is used for
 * {@code equals()} and {@code hashCode()}.
 * </p>
 */
public final class Representation implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Representation.class);

    private final Closeable data; // InputStream or Reader

    private final Record metadata;

    private Representation(final Closeable data) {
        this.data = Preconditions.checkNotNull(data);
        this.metadata = Record.create(null, KS.REPRESENTATION);
    }

    private Charset getCharset() {
        final String mimeType = this.metadata.getUnique(NIE.MIME_TYPE, String.class);
        if (mimeType == null) {
            return Charsets.UTF_8;
        }
        try {
            return MediaType.parse(mimeType).charset().or(Charsets.UTF_8);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Invalid mime type in metadata: " + mimeType, ex);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    /**
     * Creates a representation based on the {@code InputStream} specified. Note that the supplied
     * {@code InputStream} is never closed by this class: it MUST be closed externally under the
     * responsibility of the caller.
     *
     * @param stream
     *            the {@code InputStream}, not null
     * @return the created representation
     */
    public static Representation create(final InputStream stream) {
        return new Representation(stream);
    }

    /**
     * Creates a representation based on the byte array specified. The length of the byte array
     * will be reflected in the returned representation metadata (property {@link NFO#FILE_SIZE}).
     * Note that the byte array should not be changed after calling this method, as modification
     * could be (partially) reflected in the returned representation.
     *
     * @param bytes
     *            the byte array containing the binary data of the representation
     * @return the created representation
     */
    public static Representation create(final byte[] bytes) {
        final Representation representation = new Representation(new ByteArrayInputStream(bytes));
        representation.metadata.set(NFO.FILE_SIZE, (long) bytes.length);
        return representation;
    }

    /**
     * Creates a representation based on the {@code File} specified. The file length, size,
     * creation time and MIME type will be reflected in the returned representation metadata
     * (respectively, properties {@link NFO#FILE_SIZE}, {@link NFO#FILE_NAME},
     * {@link NFO#FILE_LAST_MODIFIED}, {@link NIE#MIME_TYPE}). Note that this method causes the
     * file to be opened for reading.
     *
     * @param file
     *            the file containing the binary data of the representation
     * @param autoDecompress
     *            automatically decompress the file, if compressed with gzip, bzip2, xz, 7z or lz4
     * @return the created representation
     * @throws IllegalArgumentException
     *             in case the file does not exist
     */
    public static Representation create(final File file, final boolean autoDecompress)
            throws IllegalArgumentException {
        try {
            String name = file.getName();
            final Representation representation;
            if (autoDecompress) {
                byte[] bytes = ByteStreams.toByteArray(IO.read(file.getAbsolutePath()));
                representation = new Representation(new ByteArrayInputStream(bytes));
                if (name.endsWith(".gz") || name.endsWith(".xz") || name.endsWith(".7z")) {
                    name = name.substring(0, name.length() - 3);
                } else if (name.endsWith(".bz2") || name.endsWith(".lz4")) {
                    name = name.substring(0, name.length() - 4);
                }
            } else {
                representation = new Representation(IO.buffer(new FileInputStream(file)));
            }
            representation.metadata.set(NFO.FILE_SIZE, file.length());
            representation.metadata.set(NFO.FILE_NAME, name);
            representation.metadata.set(NFO.FILE_LAST_MODIFIED, new Date(file.lastModified()));
            representation.metadata.set(NIE.MIME_TYPE, Data.extensionToMimeType(name));
            return representation;
        } catch (final FileNotFoundException ex) {
            throw new IllegalArgumentException("Not a file: " + file.getAbsolutePath());
        } catch (final IOException e) {
            throw new IllegalArgumentException("IOException on file: " + file.getAbsolutePath());
        }
    }

    /**
     * Creates a representation based on the resolvable URL specified. This method has the effect
     * of acquiring a connection to the supplied URL, from which the representation stream and a
     * number of metadata attributes are extracted. These attributes include the last modified
     * timestamp ({@link NFO#FILE_LAST_MODIFIED}), the MIME type ({@link NIE#MIME_TYPE}), the file
     * size ({@link NFO#FILE_SIZE}), the file name ({@link NFO#FILE_NAME}) and the MD5 hash (
     * {@link NFO#HAS_HASH}); all of these attributes are optional and are extracted only if
     * available.
     *
     * @param url
     *            the URL that, resolved, will produced the binary data of the representation
     * @return the created representation
     * @throws IllegalArgumentException
     *             in case acquiring a connection to the supplied URL fails
     */
    public static Representation create(final URL url) throws IllegalArgumentException {

        // Acquire a connection and open an InputStream over its entity content.
        URLConnection connection;
        InputStream stream;
        try {
            connection = url.openConnection();
            connection.connect();
            stream = connection.getInputStream();
        } catch (final IOException ex) {
            throw new IllegalArgumentException("Cannot acquire a connection to URL " + url, ex);
        }

        // Wrap the stream in a Representation object.
        final Representation representation = new Representation(stream);

        try {
            // Extract last modified.
            final long lastModified = connection.getLastModified();
            if (lastModified != 0) {
                representation.metadata.set(NFO.FILE_LAST_MODIFIED, new Date(lastModified));
            }

            // Extract MIME type from "Content-Type".
            String mimeType = connection.getContentType();
            if (mimeType == null) {
                mimeType = Data.extensionToMimeType(url.getFile());
            }
            representation.metadata.set(NIE.MIME_TYPE, mimeType);

            // Extract length from "Content-Length";
            final int length = connection.getContentLength();
            if (length >= 0) {
                representation.metadata.set(NFO.FILE_SIZE, length);
            }

            // Extract the filename either from "Content-Disposition" header or from URL.
            String filename = null;
            final String disposition = connection.getHeaderField("Content-Disposition");
            if (disposition != null && disposition.contains("filename")) {
                final int start = Math.max(disposition.indexOf('\"'), disposition.indexOf('\''));
                if (start > 0) {
                    final int end = Math.max(disposition.lastIndexOf('\"'),
                            disposition.lastIndexOf('\''));
                    if (end > 0) {
                        filename = disposition.substring(start + 1, end);
                    }
                }
            }
            if (filename == null) {
                final String path = url.getPath();
                final int index = path.lastIndexOf('/');
                if (index >= 0) {
                    filename = path.substring(index + 1);
                }

            }
            representation.metadata.set(NFO.FILE_NAME, filename);

            // Extract the MD5 hash from "Content-MD5".
            final String md5 = connection.getHeaderField("Content-MD5");
            if (md5 != null) {
                final Record hash = Record.create();
                hash.set(NFO.HASH_ALGORITHM, "MD5");
                hash.set(NFO.HASH_VALUE, md5);
                representation.metadata.set(NFO.HAS_HASH, hash);
            }

            // Return the representation built.
            return representation;

        } catch (final Throwable ex) {
            // Ensure to close the connection if something goes wrong.
            try {
                connection.getInputStream().close();
            } catch (final Throwable ex2) {
                // ignore
            }
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Creates a representation based on the {@code Reader} specified. Note that the supplied
     * {@code Reader} is never closed by this class: it MUST be closed externally under the
     * responsibility of the caller. Upon request (e.g., invocation of {@link #getInputStream()}),
     * character data produced by the {@code Reader} will be translated into byte data either
     * using the charset specified in the representation metadata (property {@link NIE#MIME_TYPE})
     * or by using UTF-8.
     *
     * @param reader
     *            the reader producing the character data of the representation
     * @return the created representation
     */
    public static Representation create(final Reader reader) {
        Preconditions.checkNotNull(reader);
        return new Representation(reader);
    }

    /**
     * Creates a representation based on the {@code CharSequence} specified. Upon request (e.g.,
     * invocation of {@link #getInputStream()}), character data produced by the {@code Reader}
     * will be translated into byte data either using the charset specified in the representation
     * metadata (property {@link NIE#MIME_TYPE}) or by using UTF-8.
     *
     * @param sequence
     *            the {@code CharSequence} with the character data of the representation
     * @return the created representation
     */
    public static Representation create(final CharSequence sequence) {
        try {
            return new Representation(CharSource.wrap(sequence).openStream());
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    /**
     * Returns the metadata about this representation.
     *
     * @return the representation metadata, not null
     */
    public Record getMetadata() {
        return this.metadata;
    }

    /**
     * Returns an {@code InputStream} over the binary data of this representation object.
     * Conversion from character to byte data, if required, is performed according to the charset
     * specified by the MIME type metadata property ({@link NIE#MIME_TYPE}).
     *
     * @return an {@code InputStream} over the binary content of this representation
     */
    public InputStream getInputStream() {
        if (this.data instanceof InputStream) {
            return (InputStream) this.data;
        } else {
            final Reader reader = (Reader) this.data;
            return new ReaderInputStream(reader, getCharset());
        }
    }

    /**
     * Returns a {@code Reader} over the character data of this representation object. Conversion
     * from byte to character data, if required, is performed according to the charset specified
     * by the MIME type metadata property ({@link NIE#MIME_TYPE}).
     *
     * @return a {@code Reader} providing access to the character data of the representation.
     */
    public Reader getReader() {
        if (this.data instanceof Reader) {
            return (Reader) this.data;
        } else {
            final InputStream stream = (InputStream) this.data;
            return new InputStreamReader(stream, getCharset());
        }
    }

    /**
     * Writes all the binary data of this representation to a {@code byte[]} object. Conversion
     * from character to byte data, if required, is performed according to the charset specified
     * by the MIME type metadata property ({@link NIE#MIME_TYPE}). If some data has been already
     * read via {@code getInputStream()} or {@code getReaer()}, it will not be returned in the
     * result.
     *
     * @return a byte array with the binary content of this representation
     * @throws IOException
     *             in case access to binary data fails
     */
    public byte[] writeToByteArray() throws IOException {
        final InputStream stream = getInputStream();
        try {
            return ByteStreams.toByteArray(stream);
        } finally {
            stream.close();
        }
    }

    /**
     * Writes all the character data of this representation to a {@code String} object. Conversion
     * from byte to character data, if required, is performed according to the charset specified
     * by the MIME type metadata property ({@link NIE#MIME_TYPE}). If some data has been already
     * read via {@code getInputStream()} or {@code getReaer()}, it will not be returned in the
     * result.
     *
     * @return a {@code String} containg the full character-based content of this representation
     * @throws IOException
     *             in case access to binary data fails
     */
    public String writeToString() throws IOException {
        final Reader reader = getReader();
        try {
            return CharStreams.toString(reader);
        } finally {
            reader.close();
        }
    }

    /**
     * Writes all the binary data of this representation to the {@code OutputStream} sink
     * specified. Conversion from character to byte data, if required, is performed according to
     * the charset specified by the MIME type metadata property ({@link NIE#MIME_TYPE}). If some
     * data has been already read via {@code getInputStream()} or {@code getReaer()}, it will not
     * be written to the supplied sink.
     *
     * @param sink
     *            the sink where to write binary data to
     * @throws IOException
     *             in case access to binary data fails
     */
    public void writeTo(final OutputStream sink) throws IOException {
        final InputStream in = getInputStream();
        try {
            ByteStreams.copy(in, sink);
        } finally {
            in.close();
        }
    }

    /**
     * Writes all the character data of this representation to the {@code Appendable} sink
     * specified. Conversion from byte to character data, if required, is performed according to
     * the charset specified by the MIME type metadata property ({@link NIE#MIME_TYPE}). If some
     * data has been already read via {@code getInputStream()} or {@code getReaer()}, it will not
     * be written to the supplied sink.
     *
     * @param sink
     *            the sink where to write character data to
     * @throws IOException
     *             in case access to binary data fails
     */
    public void writeTo(final Appendable sink) throws IOException {
        final Reader reader = getReader();
        try {
            CharStreams.copy(reader, sink);
        } finally {
            reader.close();
        }
    }

    @Override
    public void close() {
        try {
            this.data.close();
        } catch (final Exception ex) {
            LOGGER.warn("Exception caught while closing representation", ex);
        }
    }

    /**
     * {@inheritDoc} The returned representation contains the associated resource ID.
     */
    @Override
    public String toString() {
        final String file = this.metadata.getUnique(NFO.FILE_NAME, String.class, "unnamed file");
        final String type = this.metadata.getUnique(NIE.MIME_TYPE, String.class, "unknown type");
        final long size = this.metadata.getUnique(NFO.FILE_SIZE, Long.class, -1L);
        return file + ", " + type + ", " + (size >= 0 ? size + " bytes" : "unknown size");
    }

    // Source: org.apache.commons.io.input.ReaderInputStream
    private class ReaderInputStream extends InputStream {

        private static final int BUFFER_SIZE = 1024;

        private final Reader reader;

        private final CharsetEncoder enc;

        private final CharBuffer encIn;

        private final ByteBuffer encOut;

        private CoderResult lastCoderResult;

        private boolean eof;

        ReaderInputStream(final Reader reader, final Charset charset) {

            this.reader = reader;
            this.enc = charset.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
            this.encIn = CharBuffer.allocate(BUFFER_SIZE);
            this.encIn.flip();
            this.encOut = ByteBuffer.allocate(128);
            this.encOut.flip();
        }

        private void fillBuffer() throws IOException {

            if (!this.eof && (this.lastCoderResult == null || this.lastCoderResult.isUnderflow())) {
                this.encIn.compact();
                final int p = this.encIn.position();
                final int c = this.reader.read(this.encIn.array(), p, this.encIn.remaining());
                if (c == -1) {
                    this.eof = true;
                } else {
                    this.encIn.position(p + c);
                }
                this.encIn.flip();
            }

            this.encOut.compact();
            this.lastCoderResult = this.enc.encode(this.encIn, this.encOut, this.eof);
            this.encOut.flip();
        }

        @Override
        public int read(final byte[] b, final int offset, final int length) throws IOException {

            Preconditions.checkNotNull(b);
            Preconditions.checkPositionIndex(offset, b.length);
            Preconditions.checkPositionIndex(offset + length, b.length);

            int read = 0;
            int o = offset;
            int l = length;

            while (l > 0) {
                if (this.encOut.hasRemaining()) {
                    final int c = Math.min(this.encOut.remaining(), l);
                    this.encOut.get(b, o, c);
                    o += c;
                    l -= c;
                    read += c;
                } else {
                    fillBuffer();
                    if (this.eof && !this.encOut.hasRemaining()) {
                        break;
                    }
                }
            }

            return read > 0 || !this.eof ? read : l == 0 ? 0 : -1;
        }

        @Override
        public int read() throws IOException {

            for (;;) {
                if (this.encOut.hasRemaining()) {
                    return this.encOut.get() & 0xFF;
                } else {
                    fillBuffer();
                    if (this.eof && !this.encOut.hasRemaining()) {
                        return -1;
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
        }

    }

}
