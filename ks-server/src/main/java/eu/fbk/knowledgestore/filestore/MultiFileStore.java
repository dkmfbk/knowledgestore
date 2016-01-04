package eu.fbk.knowledgestore.filestore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.IO;

public class MultiFileStore extends ForwardingFileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiFileStore.class);

    private static final int DEFAULT_BUCKET_SIZE = 10;

    private static final String INDEX_FILENAME = "mfs.idx.bin";

    private static final String BUCKET_PREFIX = "mfs.b";

    private static final String BUCKET_EXT = ".bin";

    private static final long MERGE_PERIOD = 5000L; // ms

    private final FileStore delegate;

    private final int bucketSize;

    private final ReadWriteLock lock;

    private Future<?> mergeFuture;

    private Index index;

    private boolean dirty;

    public MultiFileStore(final FileStore delegate, @Nullable final Integer bucketSize) {
        this.delegate = Objects.requireNonNull(delegate);
        this.bucketSize = bucketSize != null ? bucketSize : DEFAULT_BUCKET_SIZE;
        this.lock = new ReentrantReadWriteLock(true);
        this.mergeFuture = null;
        this.index = null;
        this.dirty = false;
        LOGGER.info("{} configured, bucket size = {}", getClass().getSimpleName(), this.bucketSize);
    }

    @Override
    protected FileStore delegate() {
        return this.delegate;
    }

    @Override
    public void init() throws IOException, IllegalStateException {

        // Initialize underlying file store
        super.init();

        try {
            // Initialize index
            this.index = new Index();
            try {
                // If a previously saved index file is available, populate the index using it
                try (final InputStream stream = this.delegate.read(INDEX_FILENAME)) {
                    LOGGER.debug("Loading saved index from {}", INDEX_FILENAME);
                    this.index.read(stream);
                    this.dirty = false;
                }

            } catch (final FileMissingException ex) {
                // Otherwise, populate the index by scanning files in the underlying file store
                LOGGER.debug("Creating index (no file {} found)", INDEX_FILENAME);
                for (final String filename : this.delegate.list()) {
                    if (!filename.startsWith(BUCKET_PREFIX)) {
                        this.index.initBucketForFilename(filename, -1);
                    } else {
                        final int bucket = Integer.parseInt(filename.substring(
                                BUCKET_PREFIX.length(), filename.length() - BUCKET_EXT.length()));
                        this.index.allocateBucket(bucket);
                        try (InputStream stream = this.delegate.read(filename)) {
                            Entry entry;
                            while ((entry = Entry.read(stream)) != null) {
                                this.index.initBucketForFilename(entry.getFilename(), bucket);
                            }
                        }
                    }
                }
                merge(); // merge if necessary
                this.dirty = true; // index file will be created at close time
            }

            // Log status
            if (LOGGER.isDebugEnabled()) {
                final int numFilenames = this.index.countFilenames();
                final int numStandaloneFilenames = this.index.getStandaloneFilenames().size();
                final int numBuckets = this.index.countAllocatedBuckets();
                LOGGER.debug("{} files ({} in {} buckets, {} standalone)", numFilenames,
                        numFilenames - numStandaloneFilenames, numBuckets, numStandaloneFilenames);
            }

            // Schedule a periodic merge task
            LOGGER.debug("Scheduling merge task every {} ms", MERGE_PERIOD);
            this.mergeFuture = Data.getExecutor().scheduleWithFixedDelay(() -> {
                try {
                    merge();
                } catch (final Throwable ex) {
                    LOGGER.error("Merge failed", ex);
                }
            }, MERGE_PERIOD, MERGE_PERIOD, TimeUnit.MILLISECONDS);

        } catch (final Throwable ex) {
            // Close underlying file store and propagate
            IO.closeQuietly(this.delegate);
            Throwables.propagateIfPossible(ex, IOException.class);
            Throwables.propagate(ex);
        }
    }

    @Override
    public InputStream read(final String filename) throws FileMissingException, IOException {

        // This prevents concurrent write/delete/merge operations to occur
        this.lock.readLock().lock();

        try {
            // Lookup the bucket for the filename in the index; throw error if file not exist
            final int bucket = this.index.getBucketForFilename(filename);

            // Either read a singleton file (delegating) or read from bucket. In both cases, we
            // read all the data in advance so that there are no pending read operation to take
            // into consideration when doing write/delete/merge
            if (bucket < 0) {
                LOGGER.debug("Reading {} from standalone file", filename);
                try (InputStream in = this.delegate.read(filename)) {
                    final byte[] bytes = ByteStreams.toByteArray(in);
                    return new ByteArrayInputStream(bytes);
                }
            } else {
                final String bucketName = BUCKET_PREFIX + bucket + BUCKET_EXT;
                LOGGER.debug("Reading {} from bucket file {}", filename, bucketName);
                try (InputStream in = this.delegate.read(bucketName)) {
                    Entry entry;
                    while ((entry = Entry.read(in)) != null) {
                        if (entry.getFilename().equals(filename)) {
                            return new ByteArrayInputStream(entry.getData());
                        }
                    }
                }
                throw new IOException("Cannot find '" + filename + "' in bucket file '"
                        + bucketName + "' - perhaps the file was changed by another application. "
                        + "Consider restarting the system rebuilding the index");
            }

        } finally {
            // Always release the lock
            this.lock.readLock().unlock();
        }
    }

    @Override
    public OutputStream write(final String filename) throws FileExistsException, IOException {

        // Check there is no file stored with the same filename
        this.lock.readLock().lock();
        try {
            this.index.getBucketForFilename(filename); // throw error if file exists
        } finally {
            this.lock.readLock().unlock();
        }

        // Log beginning of operation
        LOGGER.debug("Writing {} to memory buffer", filename);

        // Return a ByteArrayOutputStream that collect data in memory. Once writing is done, data
        // is written to the FileStore and the hash table is modified accordingly
        return new ByteArrayOutputStream() {

            private final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void close() throws IOException {

                // Discard extra invocations of close()
                if (!this.closed.compareAndSet(false, true)) {
                    return;
                }

                // Delegate
                super.close();

                // This prevents any other read/write/delete/merge operation to occur
                MultiFileStore.this.lock.writeLock().lock();

                try {
                    // Drop index file and schedule its re-creation at close time
                    markDirty();

                    // Log beginning of operation
                    LOGGER.debug("Writing {} ({} bytes) to standalone file", filename, this.count);

                    // Write a standalone file to the underlying file store
                    try (OutputStream stream = MultiFileStore.this.delegate.write(filename)) {
                        stream.write(this.buf, 0, this.count);
                    }

                    // Update the index
                    MultiFileStore.this.index.initBucketForFilename(filename, -1);

                } catch (final FileExistsException ex) {
                    // Propagate with a more useful error message
                    throw new IOException("Write rejected: another file with the same name '"
                            + filename + "' has been written concurrently");

                } finally {
                    // Always release the lock
                    MultiFileStore.this.lock.writeLock().unlock();
                }
            }

        };
    }

    @Override
    public void delete(final String filename) throws FileMissingException, IOException {

        // This prevents any other read/write/delete/merge operation to occur
        this.lock.writeLock().lock();

        try {
            // Lookup the bucket for the filename in the index; throw error if file not exist
            final int bucket = this.index.getBucketForFilename(filename);

            // Drop index files and schedule their re-creation at close time
            markDirty();

            if (bucket < 0) {
                // If there is no bucket, delete a singleton file delegating to wrapped FileStore
                LOGGER.debug("Deleting {} in standalone file", filename);
                this.delegate.delete(filename);
                this.index.updateBucketForFilename(filename, 0);

            } else {
                // Otherwise, try to replace the bucket file with a set of exploded singleton
                // files, collecting their names and trying to undo changes on error
                final String bucketName = BUCKET_PREFIX + bucket + BUCKET_EXT;
                LOGGER.debug("Deleting {} in bucket file {}", filename, bucketName);
                final List<String> explodedFilenames = Lists.newArrayList();
                try (InputStream in = this.delegate.read(bucketName)) {
                    Entry entry;
                    while ((entry = Entry.read(in)) != null) {
                        try (OutputStream out = this.delegate.write(entry.getFilename())) {
                            if (!entry.getFilename().equals(filename)) {
                                LOGGER.debug(
                                        "Extracting standalone file {} ({} bytes) from bucket file {}",
                                        entry.getFilename(), entry.getData().length, bucketName);
                                out.write(entry.getData());
                                explodedFilenames.add(entry.getFilename());
                            }
                        }
                    }
                    LOGGER.debug("Deleting bucket file {}", bucketName);
                    this.delegate.delete(bucketName);
                } catch (final Throwable ex) {
                    LOGGER.debug("Explosion of bucket {} failed - attempting recovery", bucketName);
                    for (final String explodedFilename : explodedFilenames) {
                        try {
                            LOGGER.debug("Deleting exploded standalone file {}", explodedFilename);
                            this.delegate.delete(explodedFilename);
                        } catch (final Throwable ex2) {
                            LOGGER.error("Recovery error: cannot delete '" + explodedFilename
                                    + "' - you should delete it manually", ex2);
                        }
                    }
                    throw new IOException("Cannot explode bucket " + bucketName, ex);
                }

                // Update index
                for (final String explodedFilename : explodedFilenames) {
                    this.index.updateBucketForFilename(explodedFilename, -1);
                }
                this.index.updateBucketForFilename(filename, 0);
                this.index.releaseBucket(bucket);
            }

        } finally {
            // Always release the lock
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public Stream<String> list() throws IOException {

        // Define a function mapping each filename in underlying file store to the filenames it
        // corresponds to: standalone filenames are mapped to themselves, bucket filenames are
        // mapped to their content filenames; the index file is ignored
        final Function<String, Stream<String>> transformer = new Function<String, Stream<String>>() {

            @Override
            public Stream<String> apply(final String filename) {
                if (filename.equals(INDEX_FILENAME)) {
                    return Stream.create();
                } else if (!filename.startsWith(BUCKET_PREFIX)) {
                    return Stream.create(filename);
                } else {
                    final List<String> filenames = Lists.newArrayList();
                    try (InputStream in = MultiFileStore.this.delegate.read(filename)) {
                        Entry entry;
                        while ((entry = Entry.read(in)) != null) {
                            filenames.add(entry.getFilename());
                        }
                    } catch (final IOException ex) {
                        Throwables.propagate(ex);
                    }
                    return Stream.create(filenames);
                }
            }

        };

        // Return a stream that applies the transformer lazily during the iteration
        return Stream.concat(this.delegate.list().transform(transformer, 1));
    }

    @Override
    public void close() {

        try {
            // Unschedule the periodic merge task
            LOGGER.debug("Unscheduling merge task");
            this.mergeFuture.cancel(false);

            // Save index data if necessary
            if (this.dirty) {
                LOGGER.debug("Saving modified index to {}", INDEX_FILENAME);
                try (OutputStream stream = this.delegate.write(INDEX_FILENAME)) {
                    this.index.write(stream);
                }
            }

        } catch (final Throwable ex) {
            // Wrap and propagate
            Throwables.propagate(ex);

        } finally {
            // Release memory and delegate
            this.index = null;
            this.delegate.close();
        }
    }

    private void merge() throws IOException {

        // This prevents any other read/write/delete/merge operation to occur
        this.lock.writeLock().lock();

        try {
            // Abort if there are not enough standalone files to merge
            if (this.index.getStandaloneFilenames().size() < this.bucketSize) {
                return;
            }

            // Drop index files and schedule their re-creation at close time
            markDirty();

            // Sort standalone filenames (balance mix of raw and annotation files in bucket)
            final List<String> sortedFilenames = Ordering.natural().sortedCopy(
                    this.index.getStandaloneFilenames());
            LOGGER.debug("Merging started - {} files to merge", sortedFilenames.size());

            // Create a bucket for each chunk of bucketSize files
            for (int i = 0; i <= sortedFilenames.size() - this.bucketSize; i += this.bucketSize) {

                // Obtain the standalone filenames (sorted) to be put in a new bucket
                final List<String> filenames = sortedFilenames.subList(i, i + this.bucketSize);

                // Obtain a free bucket number
                final int bucket = this.index.allocateBucket(-1);

                // Create the bucket file. On failure, delete it and propagate
                final String bucketFilename = BUCKET_PREFIX + bucket + BUCKET_EXT;
                LOGGER.debug("Creating bucket file {}", bucketFilename);
                try (OutputStream out = this.delegate.write(bucketFilename)) {
                    for (final String filename : filenames) {
                        try (InputStream in = this.delegate.read(filename)) {
                            final byte[] data = ByteStreams.toByteArray(in);
                            final Entry entry = new Entry(filename, data);
                            Entry.write(out, entry);
                        }
                    }
                } catch (final Throwable ex) {
                    try {
                        this.delegate.delete(bucketFilename);
                    } catch (final Throwable ex2) {
                        LOGGER.error("Recovery error: cannot delete '" + bucketFilename
                                + "' - you should delete it manually", ex2);
                    }
                    throw new IOException("Cannot create bucket " + bucketFilename
                            + " with files " + Joiner.on(", ").join(filenames), ex);
                }

                // Delete standalone file that have been merged
                for (final String filename : filenames) {
                    try {
                        this.delegate.delete(filename);
                    } catch (final Throwable ex) {
                        LOGGER.error("Cannot delete standalone file " + bucketFilename
                                + " after creation of bucket " + bucketFilename
                                + " - you should delete it manually", ex);
                    }
                }

                // Update hash table and set of standalone files
                for (final String filename : filenames) {
                    this.index.updateBucketForFilename(filename, bucket);
                }
            }

            // Log status
            LOGGER.debug("Merging done - {} standalone files remaining", this.index
                    .getStandaloneFilenames().size());

        } finally {
            // Always release the lock
            this.lock.writeLock().unlock();
        }
    }

    private void markDirty() throws IOException {
        if (!this.dirty) {
            this.dirty = true;
            LOGGER.info("Index has changed. Deleting index file {}. "
                    + "Will be recreated at close time", INDEX_FILENAME);
            try {
                this.delegate.delete(INDEX_FILENAME);
            } catch (final FileMissingException ex) {
                // Ignore
            } catch (final Throwable ex) {
                throw new IOException("Cannot delete stale index file", ex);
            }
        }
    }

    private static final class Index {

        private static final long DELETED = 0xFFFFFFFFFFFFFFFFL;

        private long[] tableHashes;

        private int[] tableBuckets;

        private int tableSize;

        private int size;

        private final Set<Integer> unusedBuckets;

        private int maxBucket;

        private final Set<String> standaloneFilenames;

        public Index() {
            this.tableHashes = new long[16 * 2];
            this.tableBuckets = new int[16];
            this.tableSize = 0;
            this.size = 0;
            this.unusedBuckets = Sets.newHashSet();
            this.maxBucket = 0;
            this.standaloneFilenames = Sets.newHashSet();
        }

        public int countFilenames() {
            return this.size;
        }

        public int countAllocatedBuckets() {
            return this.maxBucket - this.unusedBuckets.size();
        }

        public int allocateBucket(int bucket) {

            // Identify the bucket to allocate in case a valid bucket number was not given
            if (bucket < 0) {
                if (!this.unusedBuckets.isEmpty()) {
                    bucket = this.unusedBuckets.iterator().next();
                } else {
                    bucket = this.maxBucket + 1;
                }
            }

            // Update unused bucket information
            if (bucket > this.maxBucket) {
                for (int i = this.maxBucket + 1; i < bucket; ++i) {
                    this.unusedBuckets.add(i);
                }
                this.maxBucket = bucket;
            } else {
                this.unusedBuckets.remove(bucket);
            }

            // Return the bucket number actually allocated
            return bucket;
        }

        public void releaseBucket(final int bucket) {

            if (bucket < this.maxBucket) {
                this.unusedBuckets.add(bucket);
            } else {
                do {
                    --this.maxBucket;
                } while (this.unusedBuckets.remove(this.maxBucket));
            }
        }

        public int getBucketForFilename(final String filename) throws FileMissingException {

            // Identify the table slot for the filename supplied; throw error if not found
            final Hash hash = hash(filename);
            int slot = ((int) hash.getLow() & 0x7FFFFFFF) % this.tableBuckets.length;
            while (true) {
                final long lo = this.tableHashes[slot * 2];
                final long hi = this.tableHashes[slot * 2 + 1];
                if (lo == 0L && hi == 0L) {
                    throw new FileMissingException(filename, null);
                } else if (lo == hash.getLow() && hi == hash.getHigh()) {
                    break;
                }
                slot = (slot + 1) % this.tableBuckets.length;
            }

            // Return the bucket number stored in the slot
            return this.tableBuckets[slot];
        }

        public void initBucketForFilename(final String filename, final int bucket)
                throws FileExistsException {

            // Assign a table slot to the filename supplied; throw error if already stored
            final Hash hash = hash(filename);
            int slot = ((int) hash.getLow() & 0x7FFFFFFF) % this.tableBuckets.length;
            while (true) {
                final long lo = this.tableHashes[slot * 2];
                final long hi = this.tableHashes[slot * 2 + 1];
                if (lo == 0L && hi == 0L) {
                    break;
                } else if (lo == hash.getLow() && hi == hash.getHigh()) {
                    throw new FileExistsException(filename, null);
                }
                slot = (slot + 1) % this.tableBuckets.length;
            }

            // Update set of standalone filenames
            if (bucket < 0) {
                this.standaloneFilenames.add(filename);
            }

            // Update table and counters
            this.tableHashes[slot * 2] = hash.getLow();
            this.tableHashes[slot * 2 + 1] = hash.getHigh();
            this.tableBuckets[slot] = bucket;
            ++this.tableSize;
            ++this.size;

            // Rehash the map if too big
            if (this.tableSize > this.tableBuckets.length * 2 / 3) {
                rehash();
            }
        }

        public void updateBucketForFilename(final String filename, final int bucket)
                throws FileMissingException {

            // Identify the table slot for the filename supplied; throw error if not found
            final Hash hash = hash(filename);
            int slot = ((int) hash.getLow() & 0x7FFFFFFF) % this.tableBuckets.length;
            while (true) {
                final long lo = this.tableHashes[slot * 2];
                final long hi = this.tableHashes[slot * 2 + 1];
                if (lo == 0L && hi == 0L) {
                    throw new FileMissingException(filename, null);
                } else if (lo == hash.getLow() && hi == hash.getHigh()) {
                    break;
                }
                slot = (slot + 1) % this.tableBuckets.length;
            }

            // Update set of standalone filenames
            if (this.tableBuckets[slot] < 0 && bucket >= 0) {
                this.standaloneFilenames.remove(filename);
            } else if (this.tableBuckets[slot] >= 0 && bucket < 0) {
                this.standaloneFilenames.add(filename);
            }

            // Update table and counters
            this.tableBuckets[slot] = bucket;
            if (bucket == 0) {
                this.tableHashes[slot * 2] = DELETED;
                this.tableHashes[slot * 2 + 1] = DELETED;
                --this.size;
            }
        }

        public Set<String> getStandaloneFilenames() {
            return this.standaloneFilenames;
        }

        public void read(final InputStream stream) throws IOException {

            // Wrap the stream
            final DataInputStream in = new DataInputStream(stream);

            // Read information about unused buckets
            this.maxBucket = in.readInt();
            final int numUnusedBuckets = in.readInt();
            for (int i = 0; i < numUnusedBuckets; ++i) {
                this.unusedBuckets.add(in.readInt());
            }

            // Read standalone filenames
            final int numStandaloneFilenames = in.readInt();
            for (int i = 0; i < numStandaloneFilenames; ++i) {
                final int len = in.readInt();
                final byte[] filename = new byte[len];
                in.readFully(filename);
                this.standaloneFilenames.add(new String(filename, Charsets.UTF_8));
            }

            // Read number of elements in the hash table
            this.size = in.readInt();
            this.tableSize = this.size;

            // Resize hash table
            int capacity = 16;
            while (this.size >= capacity * 2 / 3) {
                capacity *= 2;
            }
            this.tableHashes = new long[capacity * 2];
            this.tableBuckets = new int[capacity];

            // Load hash table
            for (int i = 0; i < this.size; ++i) {
                final long lo = in.readLong();
                final long hi = in.readLong();
                final int bucket = in.readInt();
                int slot = ((int) lo & 0x7FFFFFFF) % capacity;
                while (true) {
                    if (this.tableHashes[slot * 2] == 0L && this.tableHashes[slot * 2 + 1] == 0L) {
                        this.tableHashes[slot * 2] = lo;
                        this.tableHashes[slot * 2 + 1] = hi;
                        this.tableBuckets[slot] = bucket;
                        break;
                    } else {
                        slot = (slot + 1) % capacity;
                    }
                }
            }
        }

        public void write(final OutputStream stream) throws IOException {

            // Wrap the stream
            final DataOutputStream out = new DataOutputStream(stream);

            // Write information about unused buckets
            out.writeInt(this.maxBucket);
            out.writeInt(this.unusedBuckets.size());
            for (final Integer unusedBucket : this.unusedBuckets) {
                out.writeInt(unusedBucket);
            }

            // Write standalone filenames
            out.writeInt(this.standaloneFilenames.size());
            for (final String filename : this.standaloneFilenames) {
                final byte[] bytes = filename.getBytes(Charsets.UTF_8);
                out.write(bytes.length);
                out.write(bytes);
            }

            // Write hash table
            out.writeInt(this.size);
            for (int slot = 0; slot < this.tableBuckets.length; ++slot) {
                final long lo = this.tableHashes[slot * 2];
                final long hi = this.tableHashes[slot * 2 + 1];
                if (lo == 0L && hi == 0L || lo == DELETED && hi == DELETED) {
                    continue;
                }
                out.writeLong(lo);
                out.writeLong(hi);
                out.writeInt(this.tableBuckets[slot]);
            }
        }

        private void rehash() {

            // Allocate data structures for the new hash table
            final int newCapacity = this.tableBuckets.length * 2;
            final long[] newTableHashes = new long[newCapacity * 2];
            final int[] newTableBuckets = new int[newCapacity];

            // Populate the new hash table
            for (int slot = 1; slot < this.tableBuckets.length; ++slot) {

                // Retrieve <lo, hi, bucket> triple, skipping null and deleted entries
                final long lo = this.tableHashes[slot * 2];
                final long hi = this.tableHashes[slot * 2 + 1];
                if (lo == 0L && hi == 0L || lo == DELETED && hi == DELETED) {
                    continue;
                }
                final int bucket = this.tableBuckets[slot];

                // Reindex the triple in the new hash table
                int newSlot = ((int) lo & 0x7FFFFFFF) % newCapacity;
                while (true) {
                    if (newTableHashes[newSlot * 2] == 0L && newTableHashes[newSlot * 2 + 1] == 0L) {
                        newTableHashes[newSlot * 2] = lo;
                        newTableHashes[newSlot * 2 + 1] = hi;
                        newTableBuckets[newSlot] = bucket;
                        break;
                    } else {
                        newSlot = (newSlot + 1) % newCapacity;
                    }
                }
            }

            // Replace old hash table with new one
            this.tableHashes = newTableHashes;
            this.tableBuckets = newTableBuckets;
            this.tableSize = this.size;
        }

        private static Hash hash(final String filename) {
            final Hash hash = Hash.murmur3(filename);
            if (hash.getLow() == 0L && hash.getHigh() == 0L //
                    || hash.getLow() == DELETED && hash.getHigh() == DELETED) {
                return Hash.fromLongs(0L, 1L); // avoid 0,0 and DELETED,DELETED
            }
            return hash;
        }

    }

    public static final class Entry {

        private final String filename;

        private final byte[] data;

        public Entry(final String filename, final byte[] data) {
            this.filename = Objects.requireNonNull(filename);
            this.data = Objects.requireNonNull(data);
        }

        public String getFilename() {
            return this.filename;
        }

        public byte[] getData() {
            return this.data;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Entry)) {
                return false;
            }
            final Entry other = (Entry) object;
            return this.filename.equals(other.filename);
        }

        @Override
        public int hashCode() {
            return this.filename.hashCode();
        }

        @Override
        public String toString() {
            return this.filename + " (" + this.data + " bytes)";
        }

        @Nullable
        public static Entry read(final InputStream stream) throws IOException {

            try {
                // Wrap the stream
                final DataInputStream in = new DataInputStream(stream);

                // Read the filename
                final int nameLength = in.readShort();
                final byte[] name = new byte[nameLength];
                in.readFully(name);

                // Read the file content
                final int dataLength = in.readInt();
                final byte[] data = new byte[dataLength];
                in.readFully(data);

                // Return corresponding entry
                return new Entry(new String(name, Charsets.UTF_8), data);

            } catch (final EOFException ex) {
                // Ignore EOF and return a null result
                return null;
            }
        }

        public static void write(final OutputStream stream, final Entry entry) throws IOException {

            // Retrieve name and data byte arrays from the entry
            final byte[] name = entry.getFilename().getBytes(Charsets.UTF_8);
            final byte[] data = entry.getData();

            // Write name length, name bytes, data length, data bytes
            final DataOutputStream out = new DataOutputStream(stream);
            out.writeShort(name.length);
            out.write(name);
            out.writeInt(data.length);
            out.write(data);
        }

    }

}
