package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;

// TODO: global cache should store byte[] rather than Record object trees, so that a larger cache
// size can be used; this need to move serialization logic (no more Avro-based, please!) in
// ks-core

/**
 * A {@code DataStore} wrapper providing a transactional and a global cache for looked up and
 * modified records.
 * <p>
 * This wrapper aims at improving the performances of record lookups (
 * {@link DataTransaction#lookup(URI, Set, Set) lookup} calls) and modifications (
 * {@link DataTransaction#store(URI, Record) store} and {@link DataTransaction#delete(URI, URI)
 * delete} calls) through a two-level caching mechanism.
 * </p>
 * <p>
 * A global cache holds records previously looked up by transactions, up to a configurable
 * {@code maxSize} number of records for each record type. This cache provides optimal
 * performances in a read-only load. However, in presence of read-write transactions a record may
 * have multiple versions (one committed and other locally modified in active transactions),
 * therefore it may be not possible for all the transactions to look up / place data in the global
 * cache (this is enforced via a revision number mechanism).
 * </p>
 * <p>
 * To overcome the limits of the global cache with read-write transactions, each transaction is
 * also given a local cache that stores records looked up and modified locally by the transaction;
 * this cache is synchronized with the global cache upon a successful commit (either by copying
 * modified records or invalidating them). Note that synchronization is possible only if the
 * complete set of records modified by the transaction is known. When this is impossible because
 * the transaction modified more than {@code maxChanges} records to allow them to be stored in
 * memory, then an invalidation of the global cache is mandatory in order to avoid dirty reads
 * (this degrades performances!). The local cache is not just used for lookups but also to
 * implement a write-back mechanism, where up to {@code maxBufferedChanges} records modified by
 * the transaction are kept locally and flushed to the underlying data store only when strictly
 * necessary. More precisely, changes are flushed at commit time and every time operations
 * {@link DataTransaction#retrieve(URI, XPath, Set) retrieve},
 * {@link DataTransaction#count(URI, XPath) count} and
 * {@link DataTransaction#match(Map, Map, Map) match} are called.
 * </p>
 * <p>
 * Some statistics about the number of cache hits (local and global caches), fetches, changes and
 * flushes are logged at close time.
 * </p>
 */
public class CachingDataStore extends ForwardingDataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachingDataStore.class);

    private static final int DEFAULT_MAX_SIZE = 1024;

    private static final int DEFAULT_MAX_CHANGES = 1024;

    private static final int DEFAULT_MAX_BUFFERED_CHANGES = 1024;

    private static final Record NULL = Record.create();

    private final DataStore delegate;

    private final int maxChanges;

    private final int maxBufferedChanges;

    private final ReadWriteLock globalLock;

    private final Map<URI, Cache<URI, Record>> globalCaches;

    private long globalRevision;

    // Counters for statistics

    private final AtomicLong globalHitCount;

    private final AtomicLong localHitCount;

    private final AtomicLong fetchCount;

    private final AtomicLong changeCount;

    private final AtomicLong flushCount;

    /**
     * Creates a new instance for the wrapped {@code DataStore} specified.
     * 
     * @param delegate
     *            the wrapped {@code DataStore}
     * @param maxSize
     *            the maximum size of global per-record-type caches ( number of records); if null
     *            defaults to 1024
     * @param maxChanges
     *            the max number (per-type) of records that a transaction can change before
     *            modification tracking is aborted forcing the invalidation of global caches upon
     *            commit; if null defaults to 1024
     * @param maxBufferedChanges
     *            the max number (per-type) of records changed by a transactions that are buffered
     *            locally, before being flushed to the underlying {@code DataTransaction}; if null
     *            defaults to 1024
     */
    public CachingDataStore(final DataStore delegate, @Nullable final Integer maxSize,
            @Nullable final Integer maxChanges, @Nullable final Integer maxBufferedChanges) {

        final int actualMaxSize = MoreObjects.firstNonNull(maxSize, DEFAULT_MAX_SIZE);
        final int actualMaxChanges = MoreObjects.firstNonNull(maxChanges, DEFAULT_MAX_CHANGES);
        final int actualMaxBufferedChanges = MoreObjects.firstNonNull(maxBufferedChanges,
                DEFAULT_MAX_BUFFERED_CHANGES);

        Preconditions.checkArgument(actualMaxSize > 0);
        Preconditions.checkArgument(actualMaxChanges > 0);

        this.delegate = Preconditions.checkNotNull(delegate);
        this.maxChanges = actualMaxChanges;
        this.maxBufferedChanges = actualMaxBufferedChanges;
        this.globalLock = new ReentrantReadWriteLock(true);
        this.globalCaches = Maps.newHashMap();
        this.globalRevision = 0L;
        this.globalHitCount = new AtomicLong(0);
        this.localHitCount = new AtomicLong(0);
        this.fetchCount = new AtomicLong(0);
        this.changeCount = new AtomicLong(0);
        this.flushCount = new AtomicLong(0);

        for (final URI type : DataStore.SUPPORTED_TYPES) {
            // Original setting (may cause OutOfMemory if maximimum value is inappropriate
            // this.globalCaches.put(type, CacheBuilder.newBuilder().maximumSize(actualMaxSize)
            // .<URI, Record>build());
            this.globalCaches.put(type,
                    CacheBuilder.newBuilder().softValues().maximumSize(actualMaxSize)
                            .<URI, Record>build());
        }

        CachingDataStore.LOGGER.info("{} configured", this.getClass().getSimpleName());
    }

    @Override
    protected DataStore delegate() {
        return this.delegate;
    }

    @Override
    public DataTransaction begin(final boolean readOnly) throws IOException, IllegalStateException {

        // Need to acquire an exclusive lock to prevent commits in the meanwhile
        CachingDataStore.this.globalLock.readLock().lock();
        try {
            final long revision = CachingDataStore.this.globalRevision;
            final DataTransaction tx = delegate().begin(readOnly);
            return new CachingDataTransaction(tx, readOnly, revision);
        } finally {
            CachingDataStore.this.globalLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            LOGGER.info("{} - {} local cache hits, {} global cache hits, {} fetches, "
                    + "{} changes, {} flushes", this.getClass().getSimpleName(),
                    this.localHitCount, this.globalHitCount, this.fetchCount, this.changeCount,
                    this.flushCount);
        } finally {
            super.close();
        }
    }

    private class CachingDataTransaction extends ForwardingDataTransaction {

        private final DataTransaction delegate;

        @Nullable
        private final Set<URI> dirty; // contains types for which a flush has been done

        @Nullable
        private final Map<URI, Map<URI, Record>> changes; // null if read-only

        @Nullable
        private final Map<URI, Set<URI>> invalidated; // null if read-only

        private final Map<URI, Cache<URI, Record>> localCaches;

        private final long localRevision;

        CachingDataTransaction(final DataTransaction delegate, final boolean readOnly,
                final long revision) {

            this.delegate = Preconditions.checkNotNull(delegate);

            if (readOnly) {
                this.dirty = null;
                this.changes = null;
                this.invalidated = null;
            } else {
                this.dirty = Sets.newHashSet();
                this.changes = Maps.newHashMap();
                this.invalidated = Maps.newHashMap();
                for (final URI type : DataStore.SUPPORTED_TYPES) {
                    this.changes.put(type, Maps.<URI, Record>newHashMap());
                    this.invalidated.put(type, Sets.<URI>newHashSet());
                }
            }

            this.localRevision = revision;
            this.localCaches = Maps.newHashMap();
            for (final URI type : DataStore.SUPPORTED_TYPES) {
                this.localCaches.put(type, CacheBuilder.newBuilder().softValues()
                        .<URI, Record>build());
            }
        }

        @Override
        protected DataTransaction delegate() {
            return this.delegate;
        }

        @Override
        public Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
                final Set<? extends URI> properties) throws IOException, IllegalArgumentException,
                IllegalStateException {

            final long globalRevision = CachingDataStore.this.globalRevision;
            final Cache<URI, Record> globalCache = CachingDataStore.this.globalCaches.get(type);
            final Cache<URI, Record> localCache = this.localCaches.get(type);
            final List<Record> result = Lists.newArrayList();

            // Determine if there is a chance to use the global cache.
            // The global cache cannot be used if we lost track of what we changed in current
            // transaction, or if it got polluted with changes from other concurrent transaction
            // (based on revision number)
            final boolean mightUseGlobalCache = this.localRevision == globalRevision
                    && (this.dirty == null || !this.dirty.contains(type));

            // Lookup in local cache
            final Set<URI> missingIDs = Sets.newHashSet();
            for (final URI id : ids) {
                final Record record = localCache.getIfPresent(id);
                if (record == null) {
                    missingIDs.add(id);
                } else if (record != CachingDataStore.NULL) {
                    CachingDataStore.this.localHitCount.incrementAndGet();
                    result.add(Record.create(record, true)); // clone to preserve cached one
                }
            }

            // Lookup in global cache, if possible. Need to check revision number holding a shared
            // lock on the global cache
            if (mightUseGlobalCache) {
                CachingDataStore.this.globalLock.readLock().lock();
                try {
                    if (this.localRevision == globalRevision) {
                        for (final Iterator<URI> i = missingIDs.iterator(); i.hasNext();) {
                            final URI id = i.next();
                            final Record record = globalCache.getIfPresent(id);
                            if (record != null) {
                                CachingDataStore.this.globalHitCount.incrementAndGet();
                                localCache.put(id, record); // propagate to local cache
                                result.add(Record.create(record, true)); // clone record
                                i.remove(); // ID no more missing
                            }
                        }

                    }
                } finally {
                    CachingDataStore.this.globalLock.readLock().unlock();
                }
            }

            // Fetch missing records (possibly NOP)
            final List<Record> fetched = missingIDs.isEmpty() ? ImmutableList.<Record>of() : //
                    delegate().lookup(type, missingIDs, null).toList();
            CachingDataStore.this.fetchCount.addAndGet(missingIDs.size());

            // Add fetched records to result (cloning them) and to local cache; update missing IDs
            for (final Record record : fetched) {
                result.add(Record.create(record, true));
                localCache.put(record.getID(), record);
                missingIDs.remove(record.getID());
            }

            // Non-existing records are also tracked in local cache for efficiency reasons
            for (final URI id : missingIDs) {
                localCache.put(id, CachingDataStore.NULL);
            }

            // If possible, fetched data is also put in the global cache. To access it, need to
            // acquire a shared lock and check again the revision number.
            if (mightUseGlobalCache) {
                CachingDataStore.this.globalLock.readLock().lock();
                try {
                    if (this.localRevision == CachingDataStore.this.globalRevision) {
                        for (final Record record : fetched) {
                            globalCache.put(record.getID(), record);
                        }
                    }
                } finally {
                    CachingDataStore.this.globalLock.readLock().unlock();
                }
            }

            // All the data is here. Perform projection, if required
            if (properties != null && !properties.isEmpty()) {
                for (final Record record : result) {
                    final URI[] projection = properties.toArray(new URI[properties.size()]);
                    record.retain(projection);
                }
            }

            // Return a stream over the requested records
            return Stream.create(result);
        }

        @Override
        public Stream<Record> retrieve(final URI type, final XPath condition,
                final Set<? extends URI> properties) throws IOException, IllegalArgumentException,
                IllegalStateException {

            if (this.changes != null) {
                flushChanges(type);
            }

            return delegate().retrieve(type, condition, properties);
        }

        @Override
        public long count(final URI type, final XPath condition) throws IOException,
                IllegalArgumentException, IllegalStateException {

            if (this.changes != null) {
                flushChanges(type);
            }

            return delegate().count(type, condition);
        }

        @Override
        public Stream<Record> match(final Map<URI, XPath> conditions,
                final Map<URI, Set<URI>> ids, final Map<URI, Set<URI>> properties)
                throws IOException, IllegalStateException {

            if (this.changes != null) {
                flushChanges(KS.RESOURCE);
                flushChanges(KS.MENTION);
                flushChanges(KS.ENTITY);
                flushChanges(KS.AXIOM);
            }

            return delegate().match(conditions, ids, properties);
        }

        @Override
        public void store(final URI type, final Record record) throws IOException,
                IllegalStateException {
            Preconditions.checkState(this.changes != null, "Read-only DataTransaction");
            registerChange(type, record.getID(), record);
        }

        @Override
        public void delete(final URI type, final URI id) throws IOException, IllegalStateException {
            Preconditions.checkState(this.changes != null, "Read-only DataTransaction");
            registerChange(type, id, CachingDataStore.NULL);
        }

        @Override
        public void end(final boolean commit) throws IOException, IllegalStateException {

            // Simply delegate if read-only or on rollback
            if (this.changes == null || !commit) {
                this.delegate.end(commit);
                return;
            }

            // On read/write commit, start by flushing pending changes
            for (final URI type : DataStore.SUPPORTED_TYPES) {
                flushChanges(type);
            }

            // Then perform the commit and synchronize the global cache by holding an exclusive
            // lock, so to properly handle revision numbers. Pre-existing transactions will be
            // forced to stop using the global cache.
            CachingDataStore.this.globalLock.writeLock().lock();
            try {
                delegate().end(true);
                ++CachingDataStore.this.globalRevision;
                for (final URI type : DataStore.SUPPORTED_TYPES) {
                    synchronizeCaches(//
                            this.invalidated.get(type), //
                            this.localCaches.get(type), //
                            CachingDataStore.this.globalCaches.get(type));
                }
            } finally {
                CachingDataStore.this.globalLock.writeLock().unlock();
            }
        }

        private void synchronizeCaches(@Nullable final Set<URI> invalidatedIDs,
                final Cache<URI, Record> localCache, final Cache<URI, Record> globalCache) {

            if (invalidatedIDs == null) {
                globalCache.invalidateAll();
                return;
            }

            globalCache.invalidateAll(invalidatedIDs);

            for (final Map.Entry<URI, Record> entry : localCache.asMap().entrySet()) {
                final URI id = entry.getKey();
                final Record record = entry.getValue();
                if (record != CachingDataStore.NULL) {
                    globalCache.put(id, record);
                }
            }
        }

        private void registerChange(final URI type, final URI id, final Record record)
                throws IOException {

            assert this.changes != null && this.invalidated != null; // need read/write tx

            CachingDataStore.this.changeCount.incrementAndGet();

            this.localCaches.get(type).put(id, record);

            final Map<URI, Record> changeMap = this.changes.get(type);
            changeMap.put(id, record);
            if (changeMap.size() > CachingDataStore.this.maxBufferedChanges) {
                flushChanges(type);
            }

            final Set<URI> invalidatedIDs = this.invalidated.get(type);
            if (invalidatedIDs != null) {
                invalidatedIDs.add(id);
                if (invalidatedIDs.size() > CachingDataStore.this.maxChanges) {
                    this.invalidated.put(type, null);
                }
            }
        }

        private void flushChanges(final URI type) throws IOException {

            assert this.changes != null && this.invalidated != null; // need read/write tx

            final Map<URI, Record> map = this.changes.get(type);
            if (map.isEmpty()) {
                return;
            }

            this.dirty.add(type);

            CachingDataStore.this.flushCount.addAndGet(map.size());

            for (final Map.Entry<URI, Record> entry : map.entrySet()) {
                final URI id = entry.getKey();
                final Record record = entry.getValue();
                if (record == CachingDataStore.NULL) {
                    delegate().delete(type, id);
                } else {
                    delegate().store(type, record);
                }
            }
            map.clear();
        }

    }

}
