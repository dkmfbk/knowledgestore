package eu.fbk.knowledgestore.runtime;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.*;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for dealing with files using the Hadoop API.
 */
public final class Files {

    private static FileSystem rawLocalFileSystem = null;

    public static FileSystem getFileSystem(final String url, final Map<String, String> properties)
            throws IOException {

        final URI uri = URI.create(url.replace('\\', '/'));

        org.apache.hadoop.conf.Configuration conf;
        conf = new org.apache.hadoop.conf.Configuration(true);
        conf.set("fs.default.name", uri.toString());
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String name = entry.getKey();
            final String value = entry.getValue();
            conf.set(name, value);
        }

		FileSystem fs;

		if (url.startsWith("file://")) {
			fs = getRawLocalFileSystem();
		}
		else {
			fs = FileSystem.get(conf);
		}

        fs.setWorkingDirectory(new Path(uri.getPath()));
        return fs;
    }

    public static FileSystem getRawLocalFileSystem() {

        synchronized (Files.class) {
            if (Files.rawLocalFileSystem == null) {
                LoggerFactory.getLogger(Files.class).debug(
                        "You can safely ignore the following reported IOException - it's the way "
                                + "Hadoop people report where the Configuration is initialized )");
                final org.apache.hadoop.conf.Configuration conf;
                conf = new org.apache.hadoop.conf.Configuration(true);
                Files.rawLocalFileSystem = new RawLocalFileSystem();
                try {
                    Files.rawLocalFileSystem.initialize(//
                            new File(System.getProperty("user.dir")).toURI(), //
                            conf);
                } catch (final IOException ex) {
                    throw new Error("Failed to initialize local raw filesystem (!)", ex);
                }
            }
            return Files.rawLocalFileSystem;
        }
    }

    @Nullable
    public static FSDataInputStream readWithBackup(final FileSystem fs, final Path path)
            throws IOException {

        // we keep track of filesystem exceptions (but it's unclear when they are thrown)
        IOException exception = null;

        try {
            // 1. try to read the requested file
            final FSDataInputStream result = fs.open(path);
            if (result != null) {
                return result;
            }
        } catch (final IOException ex) {
            exception = ex;
        }

        final Path backupPath = new Path(path.getParent() + "/." + path.getName() + ".backup");
        try {
            // 2. on failure, try to read its backup
            final FSDataInputStream result = fs.open(backupPath);
            if (result != null) {
                return result;
            }
        } catch (final IOException ex) {
            if (exception == null) {
                exception = ex;
            }
        }

        // 3. only on failure check whether the two files exist
        final boolean fileExists = fs.exists(path);
        final boolean backupExists = fs.exists(backupPath);

        // 4. if they don't exist it's ok, just report this returning null
        if (!fileExists && !backupExists) {
            return null;
        }

        // 5. otherwise we throw an exception (possibly the ones got before)
        if (exception == null) {
            exception = new IOException("Cannot read " + (fileExists ? path : backupExists)
                    + " (file reported to exist)");
        }
        throw exception;
    }

    public static FSDataOutputStream writeWithBackup(final FileSystem fs, final Path path)
            throws IOException {

        // compute paths of new and backup files
        final Path newPath = new Path(path.getParent() + "/." + path.getName() + ".new");
        final Path backupPath = new Path(path.getParent() + "/." + path.getName() + ".backup");

        // 1. delete filename.new if it exists
        Files.delete(fs, newPath);

        // 2. if filename exists, rename it to filename.backup (deleting old backup)
        if (fs.exists(path)) {
            Files.delete(fs, backupPath);
            Files.rename(fs, path, backupPath);
        }

        // 3. create filename.new, returning a stream for writing its content
        return new FSDataOutputStream(fs.create(newPath), null) {

            @Override
            public void close() throws IOException {
                super.close();

                // 4. rename filename.new to filename
                Files.rename(fs, newPath, path);
            }

        };
    }

    public static void delete(final FileSystem fs, final Path path) throws IOException {

        IOException exception = null;

        try {
            if (fs.delete(path, false)) {
                return;
            }
        } catch (final IOException ex) {
            exception = ex;
        }

        if (fs.exists(path)) {
            throw exception != null ? exception : new IOException("Cannot delete " + path);
        }
    }

    public static void rename(final FileSystem fs, final Path from, final Path to)
            throws IOException {

        if (from.equals(to)) {
            return;
        }

        final boolean renamed = fs.rename(from, to);

        if (!renamed) {
            String message = "Cannot rename " + from + " to " + to;
            if (fs.exists(to)) {
                message += ": destination already exists";
            } else if (fs.exists(from)) {
                message += ": source does not exist";
            }
            throw new IOException(message);
        }
    }

    @Nullable
    public static FileStatus stat(final FileSystem fs, final Path path) throws IOException {

        try {
            final FileStatus status = fs.getFileStatus(path);
            if (status != null) {
                return status;
            }

        } catch (final IOException ex) {
            if (fs.exists(path)) {
                throw ex;
            }
        }

        return null;
    }

    private Files() {
    }

}
