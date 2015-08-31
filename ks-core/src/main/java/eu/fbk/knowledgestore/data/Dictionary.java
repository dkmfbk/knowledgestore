package eu.fbk.knowledgestore.data;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.fbk.rdfpro.util.IO;

// NOTE: the current implementation rewrites the dictionary file each time a mapping is added,
// keeping always a backup copy. This is secure, however performances are severely limited if a
// lot of insertions are performed. A better scheme using a log file should be used

/**
 * A persistent, synchronized, monotonic (add-only) dictionary mapping positive {@code int} keys
 * to {@code Serializable} objects.
 */
public abstract class Dictionary<T extends Serializable> {

    private static final long MAX_CLOCK_SKEW = 60 * 1000; // 60 sec

    private final Class<T> clazz;

    private final String url;

    private volatile List<T> keyToObjectIndex; // immutable, replaced on reload

    private volatile Map<T, Integer> objectToKeyIndex; // immutable, replaced on reload

    private long lastAccessed;

    public static <T extends Serializable> Dictionary<T> createLocalDictionary(
            final Class<T> objectClass, final File file) throws IOException {

        // Check parameters
        Preconditions.checkNotNull(objectClass);
        Preconditions.checkNotNull(file);

        // Build, initialize and return the dictionary
        final Dictionary<T> dictionary = new LocalDictionary<T>(objectClass, file.toURI()
                .toString(), file);
        dictionary.reload();
        return dictionary;
    }

    public static <T extends Serializable> Dictionary<T> createHadoopDictionary(
            final Class<T> objectClass, final String fileURL) throws IOException {

        // Check parameters
        Preconditions.checkNotNull(objectClass);
        Preconditions.checkNotNull(fileURL);

        // Resolve the supplied Hadoop URL, retrieving FileSystem and Path objects
        final FileSystem fs = FileSystem.get(URI.create(fileURL),
                new org.apache.hadoop.conf.Configuration(true));
        final Path path = new Path(URI.create(fileURL).getPath());

        // Build normalized URL
        String urlBase = fs.getUri().toString();
        String urlPath = path.toString();
        if (urlBase.endsWith("/")) {
            urlBase = urlBase.substring(0, urlBase.length() - 1);
        }
        if (!urlPath.startsWith("/")) {
            urlPath = "/" + urlPath;
        }
        final String url = urlBase + urlPath;

        // Build a Dictionary using the Hadoop API for all the I/O
        final Dictionary<T> dictionary = new HadoopDictionary<T>(objectClass, url, fs, path);

        // Load dictionary data and return the initialized dictionary
        dictionary.reload();
        return dictionary;
    }

    Dictionary(final Class<T> objectClass, final String url) {
        this.clazz = Preconditions.checkNotNull(objectClass);
        this.url = Preconditions.checkNotNull(url);
        this.keyToObjectIndex = Lists.newArrayList();
        this.objectToKeyIndex = Maps.newHashMap();
    }

    @Nullable
    abstract Long lastModified(String suffix) throws IOException;

    abstract InputStream read(String suffix) throws IOException;

    abstract OutputStream write(String suffix) throws IOException;

    abstract void delete(String suffix) throws IOException;

    abstract void rename(String oldSuffix, String newSuffix) throws IOException;

    public Class<T> getObjectClass() {
        return this.clazz;
    }

    public String getDictionaryURL() {
        return this.url;
    }

    public T objectFor(final int key) throws IOException, NoSuchElementException {
        return this.objectFor(key, true);
    }

    @Nullable
    public T objectFor(final int key, final boolean mustExist) throws IOException,
            NoSuchElementException {

        Preconditions.checkArgument(key > 0, "Non-positive key %d", key);

        // local cache of keyToObjectIndex, which may change concurrently
        List<T> index = this.keyToObjectIndex;

        if (key > index.size()) {
            this.reload(); // object might have been added by another process
            index = this.keyToObjectIndex; // pick up updated index
        }

        if (key <= index.size()) {
            return index.get(key - 1); // 1-based indexes
        } else if (!mustExist) {
            return null;
        }

        throw new NoSuchElementException("No object for key " + key);
    }

    public List<T> objectsFor(final Iterable<? extends Integer> keys, final boolean mustExist)
            throws IOException, NoSuchElementException {

        // local cache of keyToObjectIndex, which may change within for cycles
        List<T> index = this.keyToObjectIndex;

        for (final int key : keys) {
            Preconditions.checkArgument(key > 0, "Non-positive key %d", key);
            if (key > index.size()) {
                this.reload(); // missing objects might have been added by other processes
                index = this.keyToObjectIndex;
                break;
            }
        }

        final List<T> result = Lists.newArrayListWithCapacity(Iterables.size(keys));
        List<Integer> missing = null;
        for (final int key : keys) {
            if (key <= index.size()) {
                result.add(index.get(key - 1)); // 1-based indexes
            } else if (mustExist) {
                if (missing == null) {
                    missing = Lists.newArrayList();
                }
                missing.add(key);
            }
        }

        if (missing != null) {
            throw new NoSuchElementException("No objects for keys "
                    + Joiner.on(", ").join(missing));
        }

        return result;
    }

    public Integer keyFor(final T object) throws IOException {
        final Integer key = this.keyFor(object, true);
        assert key != null;
        return key;
    }

    @Nullable
    public Integer keyFor(final T object, final boolean mayGenerate) throws IOException {

        Preconditions.checkNotNull(object);

        Integer key = this.objectToKeyIndex.get(object);

        if (key == null && mayGenerate) {
            this.update(Collections.singletonList(object));
            key = this.objectToKeyIndex.get(object);
        }

        return key;
    }

    public List<Integer> keysFor(final Iterable<? extends T> objects, final boolean mayGenerate)
            throws IOException {

        Preconditions.checkNotNull(objects);

        // local cache of objectToKeyIndex, which may change within for cycles
        Map<T, Integer> index = this.objectToKeyIndex;

        final List<Integer> result = Lists.newArrayListWithCapacity(Iterables.size(objects));

        List<T> missingObjects = null;
        List<Integer> missingOffsets = null;

        for (final T object : objects) {
            final Integer key = index.get(object);
            result.add(key);
            if (key == null) {
                Preconditions.checkNotNull(object);
                if (missingOffsets == null) {
                    missingObjects = Lists.newArrayList();
                    missingOffsets = Lists.newArrayList();
                }
                assert missingObjects != null; // to make Eclipse happy :-(
                missingObjects.add(object);
                missingOffsets.add(result.size());
            }
        }

        if (missingObjects != null && mayGenerate) {
            assert missingOffsets != null; // to make Eclipse happy :-(
            this.update(missingObjects);
            index = this.objectToKeyIndex; // pick up updated index
            for (int i = 0; i < missingObjects.size(); ++i) {
                final int offset = missingOffsets.get(i);
                final T object = missingObjects.get(i);
                final Integer key = this.objectToKeyIndex.get(object);
                result.set(offset, key);
            }
        }

        return result;
    }

    public <M extends Map<? super Integer, ? super T>> M toMap(@Nullable final M map)
            throws IOException {

        @SuppressWarnings("unchecked")
        final M actualMap = map != null ? map : (M) Maps.newHashMap();

        this.reload(); // make sure to read the most recently persisted data

        // local cache of objectToKeyIndex, which may change within the for cycle
        final List<T> index = this.keyToObjectIndex;

        for (int i = 0; i < index.size(); ++i) {
            actualMap.put(i, index.get(i));
        }
        return actualMap;
    }

    public <L extends List<? super T>> L toList(@Nullable final L list) throws IOException {

        @SuppressWarnings("unchecked")
        final L actualList = list != null ? list : (L) Lists.newArrayList();

        this.reload(); // make sure to read the most recently persisted data

        actualList.addAll(this.keyToObjectIndex);
        return actualList;
    }

    private synchronized void reload() throws IOException {

        // abort if the file was not modified after the last time we loaded it
        final Long lastModified = lastModifiedWithBackup();
        if (lastModified == null || lastModified < this.lastAccessed - Dictionary.MAX_CLOCK_SKEW) {
            return;
        }

        // prepare two builders for re-creating the in-memory indexes
        final ImmutableList.Builder<T> keyToObjectIndexBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<T, Integer> objectToKeyIndexBuilder = ImmutableMap.builder();

        // read from the file, putting data in the builders
        final ObjectInputStream stream = new ObjectInputStream(readWithBackup());
        assert stream != null;

        T object = null;
        try {
            final int size = stream.readInt();
            for (int key = 1; key <= size; ++key) {
                object = this.clazz.cast(stream.readObject());
                keyToObjectIndexBuilder.add(object);
                objectToKeyIndexBuilder.put(object, key);
            }

        } catch (final ClassCastException ex) {
            assert object != null;
            throw new IOException("Cannot read from " + this.url + ": found "
                    + object.getClass().getName() + ", expected " + this.clazz.getName());

        } catch (final ClassNotFoundException ex) {
            throw new IOException("Cannot read from " + this.url + ": either the content is "
                    + "malformed, or it encodes data of another dictionary using classes not "
                    + "available in this JVM");
        } finally {
            stream.close();
        }

        // on success, build the new in-memory indexes and store them in the object fields
        this.keyToObjectIndex = keyToObjectIndexBuilder.build();
        this.objectToKeyIndex = objectToKeyIndexBuilder.build();

        // update the last accessed time
        this.lastAccessed = System.currentTimeMillis();
    }

    private synchronized void update(final Iterable<T> newObjects) throws IOException {

        // make sure to have the most recent data (we rely on the fact locking is reentrant)
        this.reload();

        // access current versions of in-memory indexes (after the reload)
        List<T> keyToObjectIndex = this.keyToObjectIndex;
        Map<T, Integer> objectToKeyIndex = this.objectToKeyIndex;

        // detect missing objects. nothing to do if there are no missing objects
        final List<T> missing = Lists.newArrayList();
        for (final T object : newObjects) {
            if (!objectToKeyIndex.containsKey(object)) {
                missing.add(object);
            }
        }
        if (missing.isEmpty()) {
            return;
        }

        // create new key -> object index that includes the missing objects
        keyToObjectIndex = ImmutableList.copyOf(Iterables.concat(keyToObjectIndex, missing));

        // create new object -> key index that includes the missing objects
        final ImmutableMap.Builder<T, Integer> builder = ImmutableMap.builder();
        builder.putAll(objectToKeyIndex);
        int key = objectToKeyIndex.size();
        for (final T object : missing) {
            builder.put(object, ++key);
        }
        objectToKeyIndex = builder.build();

        // write the new index to the file
        final ObjectOutputStream stream = new ObjectOutputStream(writeWithBackup());
        try {
            stream.writeInt(keyToObjectIndex.size());
            for (final T object : keyToObjectIndex) {
                stream.writeObject(object);
            }
        } finally {
            stream.close();
        }

        // update last accessed time
        this.lastAccessed = System.currentTimeMillis();

        // update index member variables
        this.keyToObjectIndex = keyToObjectIndex;
        this.objectToKeyIndex = objectToKeyIndex;
    }

    private InputStream readWithBackup() throws IOException {

        // we keep track of filesystem exceptions (but it's unclear when they are thrown)
        IOException exception = null;

        try {
            // 1. try to read the requested file
            final InputStream result = read("");
            if (result != null) {
                return result;
            }
        } catch (final IOException ex) {
            exception = ex;
        }

        try {
            // 2. on failure, try to read its backup
            final InputStream result = read(".backup");
            if (result != null) {
                return result;
            }
        } catch (final IOException ex) {
            if (exception == null) {
                exception = ex;
            }
        }

        // 3. only on failure check whether the two files exist
        final boolean fileExists = lastModified("") != null;
        final boolean backupExists = lastModified(".backup") != null;

        // 4. if they don't exist it's ok, just report this returning null
        if (!fileExists && !backupExists) {
            return null;
        }

        // 5. otherwise we throw an exception (possibly the ones got before)
        if (exception == null) {
            exception = new IOException("Cannot read "
                    + (fileExists ? this.url : this.url + ".backup") + " (file reported to exist)");
        }
        throw exception;
    }

    private OutputStream writeWithBackup() throws IOException {

        // 1. delete filename.new if it exists
        delete(".new");

        // 2. if filename exists, rename it to filename.backup (deleting old backup)
        if (lastModified("") != null) {
            delete(".backup");
            rename("", ".backup");
        }

        // 3. create filename.new, returning a stream for writing its content
        return new FilterOutputStream(write(".new")) {

            @Override
            public void close() throws IOException {
                super.close();
                rename(".new", "");
            }

        };
    }

    private Long lastModifiedWithBackup() throws IOException {

        Long lastModified = lastModified("");
        if (lastModified == null) {
            lastModified = lastModified(".backup");
        }
        return lastModified;
    }

    private static final class LocalDictionary<T extends Serializable> extends Dictionary<T> {

        private final File file;

        LocalDictionary(final Class<T> objectClass, final String url, final File file) {
            super(objectClass, url);
            this.file = file;
        }

        @Override
        @Nullable
        Long lastModified(final String suffix) throws IOException {
            final long modifiedTime = applySuffix(suffix).lastModified();
            return modifiedTime > 0 ? modifiedTime : null;
        }

        @Override
        InputStream read(final String suffix) throws IOException {
            return IO.read(applySuffix(suffix).getAbsolutePath());
        }

        @Override
        OutputStream write(final String suffix) throws IOException {
            return IO.write(applySuffix(suffix).getAbsolutePath());
        }

        @Override
        void delete(final String suffix) throws IOException {
            applySuffix(suffix).delete();
        }

        @Override
        void rename(final String oldSuffix, final String newSuffix) throws IOException {
            java.nio.file.Files.move(applySuffix(oldSuffix).toPath(), applySuffix(newSuffix)
                    .toPath());
        }

        private File applySuffix(final String suffix) {
            return Strings.isNullOrEmpty(suffix) ? this.file : new File(
                    this.file.getAbsolutePath() + suffix);
        }

    }

    private static final class HadoopDictionary<T extends Serializable> extends Dictionary<T> {

        private final FileSystem fs;

        private final Path path;

        HadoopDictionary(final Class<T> objectClass, final String url, final FileSystem fs,
                final Path path) {
            super(objectClass, url);
            this.fs = fs;
            this.path = path;
        }

        @Override
        @Nullable
        Long lastModified(final String suffix) throws IOException {
            final Path path = applySuffix(suffix);
            try {
                final FileStatus status = this.fs.getFileStatus(path);
                if (status != null) {
                    return status.getModificationTime();
                }

            } catch (final IOException ex) {
                if (this.fs.exists(path)) {
                    throw ex;
                }
            }
            return null;
        }

        @Override
        InputStream read(final String suffix) throws IOException {
            return this.fs.open(applySuffix(suffix));
        }

        @Override
        OutputStream write(final String suffix) throws IOException {
            return this.fs.create(applySuffix(suffix));
        }

        @Override
        void delete(final String suffix) throws IOException {
            final Path path = applySuffix(suffix);
            IOException exception = null;
            try {
                if (this.fs.delete(path, false)) {
                    return;
                }
            } catch (final IOException ex) {
                exception = ex;
            }
            if (this.fs.exists(path)) {
                throw exception != null ? exception : new IOException("Cannot delete " + path);
            }
        }

        @Override
        void rename(final String oldSuffix, final String newSuffix) throws IOException {
            if (oldSuffix.equals(newSuffix)) {
                return;
            }
            final Path from = applySuffix(oldSuffix);
            final Path to = applySuffix(newSuffix);
            final boolean renamed = this.fs.rename(from, to);
            if (!renamed) {
                String message = "Cannot rename " + from + " to " + to;
                if (this.fs.exists(to)) {
                    message += ": destination already exists";
                } else if (this.fs.exists(from)) {
                    message += ": source does not exist";
                }
                throw new IOException(message);
            }
        }

        private Path applySuffix(final String suffix) {
            return Strings.isNullOrEmpty(suffix) ? this.path : new Path(this.path.getParent()
                    + "/" + this.path.getName() + suffix);
        }

    }

}
