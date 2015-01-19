package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.runtime.Component;

/**
 * A storage for resource files, supporting CRUD operations.
 * <p>
 * A {@code FileStore} abstracts the way resource files are stored in the KnowledgeStore allowing
 * different implementations to be plugged in. A {@code FileStore} stores a list of files each
 * identified by a filename, with no (visible) directory structure. Files can be listed (
 * {@link #list()}), written ({@link #write(String)}), read ( {@link #read(String)}) or deleted (
 * {@link #delete(String)}), but not modified after they are written the first time. Note that a
 * {@code FileStore} obeys the general contract and lifecycle of {@link Component}.
 * </p>
 */
public interface FileStore extends Component {

    /**
     * Read a file stored on the {@code FileStore}. The method returns an {@code InputStream} over
     * the content of the file, starting from its first byte. Sequential access and forward
     * seeking ( {@link InputStream#skip(long)}) are supported. Note that multiple concurrent read
     * operations on the same file are allowed.
     * 
     * @param filename
     *            the name of the file
     * @return an {@code InputStream} allowing access to the content of the file
     * @throws FileMissingException
     *             in case no file exists for the name specified, resulting either from a caller
     *             error or from an external modification (deletion) to the {@code FileStore}
     * @throws IOException
     *             in case the file cannot be accessed for whatever reason, with no implication on
     *             the fact that the file exists or does not exist (e.g., the whole
     *             {@code FileStore} may temporarily be not accessible)
     */
    InputStream read(String filename) throws FileMissingException, IOException;

    /**
     * Writes a new file to the {@code FileStore}. The method creates a file with the name
     * specified, and returns an {@code OutputStream} that can be used to write the content of the
     * file. Writing is completed by closing that stream, which forces written data to be flushed
     * to disk; errors in writing the file may result in the stream being forcedly closed and a
     * truncated file to be written. The file being written may be listed by {@link #list()}
     * (depending on the {@code FileStore} implementation) but should not be accessed for read or
     * deletion until writing is completed; the consequences of doing so are not specified.
     * 
     * @param filename
     *            the name of the file
     * @return an {@code OutputStream} where the content of the file can be written to
     * @throws FileExistsException
     *             in case a file with the same name already exists, resulting either from a
     *             caller error or from an external modification (file creation) to the
     *             {@code FileStore}.
     * @throws IOException
     *             in case the file cannot be created for whatever reason, with no implication on
     *             the fact that another file with the same name already exists
     */
    OutputStream write(String filename) throws FileExistsException, IOException;

    /**
     * Deletes a file on the {@code FileStore}. An exception is thrown if the file specified does
     * not exist. After the method returns, the file cannot be accessed anymore from
     * {@link #read(String)} or {@link #delete(String)}. Deleting a file being read is allowed and
     * eventually results in the deletion of the file; whether the concurrent read operation fails
     * or is permitted to complete is an implementation detail.
     * 
     * @param filename
     *            the name of the file
     * @throws FileMissingException
     *             in case the file specified does not exist, possibly because of an external
     *             modification to the {@code FileStore}
     * @throws IOException
     *             in case the file cannot be accessed for whatever reason, with no implication on
     *             the fact that the file specified actually exists
     */
    void delete(String filename) throws FileMissingException, IOException;

    /**
     * Lists all the files stored on the {@code FileStore}. The method returns a {@code Stream}
     * supporting streaming access to the names of the files stored in the {@code FileStore}.
     * Concurrent modifications to the {@code FileStore} ({@link #write(String)},
     * {@link #delete(String)}) may be supported, depending on the implementation: in that case,
     * the stream may either reflect the status before or after the concurrent modification.
     * 
     * @return a stream over the names of the files stored in the {@code FileStore}
     * @throws IOException
     *             in case of failure, for whatever reason
     */
    Stream<String> list() throws IOException;

}
