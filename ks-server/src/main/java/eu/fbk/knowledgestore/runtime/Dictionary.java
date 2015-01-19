package eu.fbk.knowledgestore.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A persistent, synchronized, monotonic (add-only) dictionary mapping positive {@code int} keys
 * to {@code Serializable} objects.
 */
public final class Dictionary<T extends Serializable> {

    private static final long MAX_CLOCK_SKEW = 60 * 1000; // 60 sec

    private Class<T> clazz;

    private final FileSystem fs;

    private final Path path;

    private volatile List<T> keyToObjectIndex; // immutable, replaced on reload

    private volatile Map<T, Integer> objectToKeyIndex; // immutable, replaced on reload

    private long lastAccessed;

    public Dictionary(final Class<T> objectClass, final String fileURL) throws IOException {

        Preconditions.checkNotNull(objectClass);
        Preconditions.checkNotNull(fileURL);

        this.clazz = objectClass;
        this.fs = FileSystem.get(URI.create(fileURL), new org.apache.hadoop.conf.Configuration(
                true));
        this.path = new Path(URI.create(fileURL).getPath());

        this.keyToObjectIndex = Lists.newArrayList();
        this.objectToKeyIndex = Maps.newHashMap();

        this.reload();
    }

    public Dictionary(final Class<T> objectClass, final FileSystem fs, final Path path)
            throws IOException {

        Preconditions.checkNotNull(objectClass);
        Preconditions.checkNotNull(fs);
        Preconditions.checkNotNull(path);

        this.clazz = objectClass;
        this.fs = fs;
        this.path = path;

        this.keyToObjectIndex = Lists.newArrayList();
        this.objectToKeyIndex = Maps.newHashMap();

        this.reload();
    }

    public Class<T> getObjectClass() {
        return this.clazz;
    }

    public String getDictionaryURL() {
        String base = this.fs.getUri().toString();
        String path = this.path.toString();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return base + path;
    }

    private synchronized void reload() throws IOException {

        // abort if the file was not modified after the last time we loaded it
        final FileStatus stat = Files.stat(this.fs, this.path);
        if (stat == null || stat.getModificationTime() < //
                this.lastAccessed - Dictionary.MAX_CLOCK_SKEW) {
            return;
        }

        // prepare two builders for re-creating the in-memory indexes
        final ImmutableList.Builder<T> keyToObjectIndexBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<T, Integer> objectToKeyIndexBuilder = ImmutableMap.builder();

        // read from the file, putting data in the builders
        final ObjectInputStream stream = new ObjectInputStream(Files.readWithBackup(this.fs,
                this.path));
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
            throw new IOException("Cannot read from " + this.path + ": found "
                    + object.getClass().getName() + ", expected " + this.clazz.getName());

        } catch (final ClassNotFoundException ex) {
            throw new IOException("Cannot read from " + this.path + ": either the content is "
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
        final ObjectOutputStream stream = new ObjectOutputStream(Files.writeWithBackup(this.fs,
                this.path));
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

}
