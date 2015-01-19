package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.runtime.Files;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * A {@code DataStore} implementations that keeps all data in memory, with persistence provided by
 * loading / saving data to file.
 * <p>
 * This class realizes a low-performance, functional implementation of the {@code DataStore}
 * component. Record data is loaded at startup from a configurable file and then indexed in
 * memory; data is written back at shutdown. Each (read-write) transaction works on its copy of
 * data, and changes are merged back in the component upon successful commit, although data is
 * written back to disk only at shutdown.
 * </p>
 */
public class MemoryDataStore implements DataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryDataStore.class);

    private static final String PATH_DEFAULT = "datastore.ttl";

    private Map<URI, Map<URI, Record>> tables;

    private int revision;

    private boolean initialized;

    private boolean closed;

    private final FileSystem fileSystem;

    private final Path filePath;

    /**
     * Creates a new {@code MemoryDataStore} instance loading/storing data in the file at the path
     * and file system specified.
     * 
     * @param fileSystem
     *            the filesystem containing the file where to read/write data
     * @param path
     *            the path of the file where to read/write data, possibly relative to the file
     *            system working directory; if null defaults to {@code datastore.ttl}
     */
    public MemoryDataStore(final FileSystem fileSystem, @Nullable final String path) {
        this.fileSystem = Preconditions.checkNotNull(fileSystem);
        this.filePath = new Path(Objects.firstNonNull(path, MemoryDataStore.PATH_DEFAULT))
                .makeQualified(this.fileSystem); // resolve against working directory
        this.tables = Maps.newHashMap();
        this.revision = 1;
        this.initialized = false;
        this.closed = false;
        for (final URI supportedType : DataStore.SUPPORTED_TYPES) {
            this.tables.put(supportedType, Maps.<URI, Record>newLinkedHashMap());
        }
        MemoryDataStore.LOGGER.info("{} configured, path={}", this.getClass().getSimpleName(),
                this.filePath);
    }

    @Override
    public synchronized void init() throws IOException, IllegalStateException {
        Preconditions.checkState(!this.initialized && !this.closed);
        this.initialized = true;

        InputStream stream = null;
        try {
            if (this.fileSystem.exists(this.filePath)) {
                stream = Files.readWithBackup(this.fileSystem, this.filePath);
                final RDFFormat format = RDFFormat.forFileName(this.filePath.getName());
                final List<Record> records = Record.decode(
                        RDFUtil.readRDF(stream, format, null, null, false),
                        ImmutableSet.of(KS.RESOURCE, KS.MENTION, KS.ENTITY, KS.CONTEXT), false)
                        .toList();
                for (final Record record : records) {
                    final URI id = Preconditions.checkNotNull(record.getID());
                    final URI type = Preconditions.checkNotNull(record.getSystemType());
                    MemoryDataStore.this.tables.get(type).put(id, record);
                }
                MemoryDataStore.LOGGER.info("{} initialized, {} records loaded", this.getClass()
                        .getSimpleName(), records.size());
            } else {
                MemoryDataStore.LOGGER.info("{} initialized, no record loaded", this.getClass()
                        .getSimpleName());
            }
        } finally {
            Util.closeQuietly(stream);
        }
    }

    @Override
    public synchronized DataTransaction begin(final boolean readOnly) throws IOException,
            IllegalStateException {
        Preconditions.checkState(this.initialized && !this.closed);
        return new MemoryDataTransaction(readOnly);
    }

    @Override
    public synchronized void close() {

        if (this.closed) {
            return;
        }
        this.closed = true;

    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    private synchronized void update(final Map<URI, Map<URI, Record>> tables, final int revision)
            throws IOException {
        if (this.revision != revision) {
            throw new IOException("Commit failed due to concurrent modifications " + this.revision
                    + ", " + revision);
        }

        OutputStream stream = null;
        try {
            stream = Files.writeWithBackup(this.fileSystem, this.filePath);
            final List<Record> records = Lists.newArrayList();
            for (final URI type : tables.keySet()) {
                records.addAll(tables.get(type).values());
            }
            final RDFFormat format = RDFFormat.forFileName(this.filePath.getName());
            RDFUtil.writeRDF(stream, format, Data.getNamespaceMap(), null,
                    Record.encode(Stream.create(records), ImmutableSet.<URI>of()));
            ++this.revision;
            this.tables = tables;
            MemoryDataStore.LOGGER.info("MemoryDataStore updated, {} records persisted",
                    records.size());

        } catch (final Throwable ex) {
            MemoryDataStore.LOGGER.error("MemoryDataStore update failed", ex);

        } finally {
            Util.closeQuietly(stream);
        }
    }

    private class MemoryDataTransaction implements DataTransaction {

        private final Map<URI, Map<URI, Record>> tables;

        private final int revision;

        private final boolean readOnly;

        private boolean ended;

        MemoryDataTransaction(final boolean readOnly) {

            Map<URI, Map<URI, Record>> tables = MemoryDataStore.this.tables;
            if (!readOnly) {
                tables = Maps.newHashMap();
                for (final Map.Entry<URI, Map<URI, Record>> entry : MemoryDataStore.this.tables
                        .entrySet()) {
                    tables.put(entry.getKey(), Maps.newLinkedHashMap(entry.getValue()));
                }
            }

            this.tables = tables;
            this.revision = MemoryDataStore.this.revision;
            this.readOnly = readOnly;
            this.ended = false;
        }

        private Map<URI, Record> getTable(final URI type) {
            final Map<URI, Record> table = this.tables.get(type);
            if (table != null) {
                return table;
            }
            throw new IllegalArgumentException("Unsupported type " + type);
        }

        private Stream<Record> select(final Map<URI, Record> table,
                final Stream<? extends URI> stream) {
            return stream.transform(new Function<URI, Record>() {

                @Override
                public Record apply(final URI id) {
                    return table.get(id);
                }

            }, 0);
        }

        private Stream<Record> filter(final Stream<Record> stream, @Nullable final XPath xpath) {
            if (xpath == null) {
                return stream;
            }
            return stream.filter(xpath.asPredicate(), 0);
        }

        private Stream<Record> project(final Stream<Record> stream,
                @Nullable final Iterable<? extends URI> properties) {
            final URI[] array = properties == null ? null : Iterables.toArray(properties,
                    URI.class);
            return stream.transform(new Function<Record, Record>() {

                @Override
                public final Record apply(final Record input) {
                    final Record result = Record.create(input, true);
                    if (array != null) {
                        result.retain(array);
                    }
                    return result;
                }

            }, 0);
        }

        @Override
        public synchronized Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
                @Nullable final Set<? extends URI> properties) throws IOException,
                IllegalArgumentException, IllegalStateException {
            Preconditions.checkState(!this.ended);
            final Map<URI, Record> table = this.getTable(type);
            return this.project(this.select(table, Stream.create(ids)), properties);
        }

        @Override
        public synchronized Stream<Record> retrieve(final URI type,
                @Nullable final XPath condition, @Nullable final Set<? extends URI> properties)
                throws IOException, IllegalArgumentException, IllegalStateException {
            Preconditions.checkState(!this.ended);
            final Map<URI, Record> table = this.getTable(type);
            return this.project(this.filter(Stream.create(table.values()), condition), properties);
        }

        @Override
        public synchronized long count(final URI type, @Nullable final XPath condition)
                throws IOException, IllegalArgumentException, IllegalStateException {
            Preconditions.checkState(!this.ended);
            final Map<URI, Record> table = this.getTable(type);
            return this.filter(Stream.create(table.values()), condition).count();
        }

        @Override
        public Stream<Record> match(final Map<URI, XPath> conditions,
                final Map<URI, Set<URI>> ids, final Map<URI, Set<URI>> properties)
                throws IOException, IllegalStateException {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public void store(final URI type, final Record record) throws IOException,
                IllegalStateException {
            Preconditions.checkState(!this.ended);
            Preconditions.checkState(!this.readOnly);
            Preconditions.checkArgument(record.getID() != null);
            final Map<URI, Record> table = this.getTable(type);
            table.put(record.getID(), Record.create(record, true));
        }

        @Override
        public void delete(final URI type, final URI id) throws IOException, IllegalStateException {
            Preconditions.checkState(!this.ended);
            Preconditions.checkState(!this.readOnly);
            Preconditions.checkArgument(id != null);
            final Map<URI, Record> table = this.getTable(type);
            table.remove(id);
        }

        @Override
        public synchronized void end(final boolean commit) throws IOException,
                IllegalStateException {
            if (!this.ended) {
                this.ended = true;
                if (commit && !this.readOnly) {
                    MemoryDataStore.this.update(this.tables, this.revision);
                }
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

    }

}
