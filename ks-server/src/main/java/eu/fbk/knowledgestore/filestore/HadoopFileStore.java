package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Stream;

/**
 * A {@code FileStore} implementation based on the Hadoop API.
 * <p>
 * An {@code HadoopFileStore} stores its files in an Hadoop {@link FileSystem}, under a certain,
 * configurable root path; the filesystem can be any of the filesystems supported by the Hadoop
 * API, including the local (raw) filesystem and the distributed HDFS filesystem.
 * </p>
 * <p>
 * Files are stored in a a two-level directory structure, where first level directories reflect
 * the MIME types of stored files, and second level directories are buckets of files whose name is
 * obtained by hashing the filename; buckets are used in order to equally split a large number of
 * files in several subdirectories, overcoming possible filesystem limitations in terms of maximum
 * number of files storable in a directory.
 * </p>
 */
public class HadoopFileStore implements FileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFileStore.class);

    private static final String DEFAULT_PATH = "files";

    private final FileSystem fileSystem;

    private final Path rootPath;

    /**
     * Creates a new {@code HadoopFileStore} storing files in the {@code FileSystem} and under the
     * {@code rootPath} specified.
     *
     * @param fileSystem
     *            the file system, not null
     * @param path
     *            the root path where to store files, possibly relative to the filesystem working
     *            directory; if null, the default root path {@code files} will be used
     */
    public HadoopFileStore(final FileSystem fileSystem, @Nullable final String path) {
        this.fileSystem = Preconditions.checkNotNull(fileSystem);
        this.rootPath = new Path(MoreObjects.firstNonNull(path, DEFAULT_PATH))
                .makeQualified(this.fileSystem); // resolve wrt workdir
        LOGGER.info("{} configured, path={}", getClass().getSimpleName(), this.rootPath);
    }

    @Override
    public void init() throws IOException {
        if (!this.fileSystem.exists(this.rootPath)) {
            this.fileSystem.mkdirs(this.rootPath);
        }
    }

    @Override
    public InputStream read(final String fileName) throws FileMissingException, IOException {
        final Path path = getFullPath(fileName);
        try {
            final InputStream stream = this.fileSystem.open(path);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Reading file " + getRelativePath(path));
            }
            return stream;
        } catch (final IOException ex) {
            if (!this.fileSystem.exists(path)) {
                throw new FileMissingException(fileName, "Cannot read non-existing file");
            }
            throw ex;
        }
    }

    @Override
    public OutputStream write(final String fileName) throws FileExistsException, IOException {
        final Path path = getFullPath(fileName);
        try {
            final OutputStream stream = this.fileSystem.create(path, false);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Creating file " + getRelativePath(path));
            }
            return stream;
        } catch (final IOException ex) {
            if (this.fileSystem.exists(path)) {
                throw new FileExistsException(fileName, "Cannot overwrite file");
            }
            throw ex;
        }
    }

    @Override
    public void delete(final String fileName) throws FileMissingException, IOException {
        final Path path = getFullPath(fileName);
        boolean deleted = false;
        try {
            deleted = this.fileSystem.delete(path, false);
            if (deleted) {
                final Path parent = path.getParent();
                if (this.fileSystem.listStatus(parent).length == 0) {
                    this.fileSystem.delete(parent, false);
                }
            }

        } finally {
            if (!deleted && !this.fileSystem.exists(path)) {
                throw new FileMissingException(fileName, "Cannot delete non-existing file.");
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Deleted file " + getRelativePath(path));
        }
    }

    @Override
    public Stream<String> list() throws IOException {
        return Stream.create(new HadoopIterator());
    }

    @Override
    public void close() {
        // Nothing to do here. FileSystems are cached and closed by Hadoop at shutdown.
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private Path getFullPath(final String fileName) {
        final String typeDirectory = MoreObjects.firstNonNull(Data.extensionToMimeType(fileName),
                "application/octet-stream").replace('/', '_');
        final String bucketDirectory = Data.hash(fileName).substring(0, 2);
        return new Path(this.rootPath, typeDirectory + "/" + bucketDirectory + "/" + fileName);
    }

    private String getRelativePath(final Path path) {
        return path.toString().substring(this.rootPath.toString().length());
    }

    private class HadoopIterator extends AbstractIterator<String> {

        private final FileStatus[] typeDirectories;

        private FileStatus[] bucketDirectories;

        private FileStatus[] files;

        private int typeIndex;

        private int bucketIndex;

        private int fileIndex;

        HadoopIterator() throws IOException {
            this.typeDirectories = HadoopFileStore.this.fileSystem
                    .listStatus(HadoopFileStore.this.rootPath);
            this.bucketDirectories = new FileStatus[] {};
            this.files = new FileStatus[] {};
        }

        @Override
        protected String computeNext() {
            try {
                while (true) {
                    if (this.fileIndex < this.files.length) {
                        final FileStatus file = this.files[this.fileIndex++];
                        if (!file.isDir()) {
                            return file.getPath().getName();
                        }
                    } else if (this.bucketIndex < this.bucketDirectories.length) {
                        final FileStatus bucketDirectory;
                        bucketDirectory = this.bucketDirectories[this.bucketIndex++];
                        if (bucketDirectory.isDir()) {
                            this.files = HadoopFileStore.this.fileSystem
                                    .listStatus(bucketDirectory.getPath());
                            this.fileIndex = 0;
                        }
                    } else if (this.typeIndex < this.typeDirectories.length) {
                        final FileStatus typeDirectory;
                        typeDirectory = this.typeDirectories[this.typeIndex++];
                        if (typeDirectory.isDir()) {
                            this.bucketDirectories = HadoopFileStore.this.fileSystem
                                    .listStatus(typeDirectory.getPath());
                            this.bucketIndex = 0;
                        }
                    } else {
                        return endOfData();
                    }
                }
            } catch (final Throwable ex) {
                throw Throwables.propagate(ex);
            }
        }

    }

}
