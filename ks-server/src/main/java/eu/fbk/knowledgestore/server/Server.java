package eu.fbk.knowledgestore.server;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.html.HtmlEscapers;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.FileBackedOutputStream;
import com.google.common.net.MediaType;
import com.google.common.net.UrlEscapers;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.knowledgestore.AbstractKnowledgeStore;
import eu.fbk.knowledgestore.AbstractSession;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.Outcome.Status;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.DataStore;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.filestore.FileStore;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.triplestore.TripleStore;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

// TODO file garbage collection

public final class Server extends AbstractKnowledgeStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private static final int DEFAULT_CHUNK_SIZE = 1024;

    private static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

    private static long fileVersionCounter = 0L;

    private final FileStore fileStore;

    private final DataStore dataStore;

    private final TripleStore tripleStore;

    private final int chunkSize;

    private final int bufferSize;

    private Server(final Builder builder) {

        boolean success = false;

        this.fileStore = Preconditions.checkNotNull(builder.fileStore);
        this.dataStore = Preconditions.checkNotNull(builder.dataStore);
        this.tripleStore = Preconditions.checkNotNull(builder.tripleStore);

        try {
            this.chunkSize = Objects.firstNonNull(builder.chunkSize, DEFAULT_CHUNK_SIZE);
            this.bufferSize = Objects.firstNonNull(builder.bufferSize, DEFAULT_BUFFER_SIZE);
            Preconditions.checkArgument(this.chunkSize > 0);
            Preconditions.checkArgument(this.bufferSize > 0);

            // TODO
            try {
                this.fileStore.init();
                this.dataStore.init();
                this.tripleStore.init();
            } catch (final Exception ex) {
                throw new Error(ex);
            }

            success = true;

        } finally {
            if (!success) {
                closeQuietly(this.fileStore);
                closeQuietly(this.dataStore);
                closeQuietly(this.tripleStore);
            }
        }
    }

    @Override
    protected Session doNewSession(@Nullable final String username, @Nullable final String password) {
        return new SessionImpl(username, password);
    }

    @Override
    protected void doClose() {
        closeQuietly(this.fileStore);
        closeQuietly(this.dataStore);
        closeQuietly(this.tripleStore);
    }

    private static void closeQuietly(@Nullable final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Throwable ex) {
                LOGGER.error(
                        "Error closing " + closeable.getClass().getSimpleName() + ": "
                                + ex.getMessage(), ex);
            }
        }
    }

    private final class SessionImpl extends AbstractSession {

        SessionImpl(@Nullable final String username, @Nullable final String password) {
            super(Data.newNamespaceMap(Data.newNamespaceMap(), Data.getNamespaceMap()), username,
                    password);
        }

        private void check(final boolean condition, final Status status,
                @Nullable final URI objectID, @Nullable final String message, final Object... args)
                throws OperationException {
            if (!condition) {
                throw newException(status, objectID,
                        message == null ? null : String.format(message, args));
            }
        }

        private Outcome newOutcome(@Nullable final Status status, @Nullable final URI objectID,
                @Nullable final String message, final Object... args) {
            return Outcome.create(status == null ? Status.ERROR_UNEXPECTED : status,
                    getInvocationID(), objectID,
                    message == null ? null : String.format(message, args));
        }

        private OperationException newException(@Nullable final Status status,
                @Nullable final URI objectID, @Nullable final String message,
                final Throwable... causes) {
            return new OperationException(newOutcome(status, objectID, message), causes);
        }

        private <T> Stream<T> attach(final DataTransaction transaction, final Stream<T> stream) {
            return stream.onClose(new Closeable() {

                @Override
                public void close() throws IOException {
                    transaction.end(true);
                }

            });
        }

        private <T> Stream<T> attach(final TripleTransaction transaction, final Stream<T> stream) {
            return stream.onClose(new Closeable() {

                @Override
                public void close() throws IOException {
                    transaction.end(true);
                }

            });
        }

        @Override
        protected Representation doDownload(@Nullable final Long timeout, final URI resourceID,
                @Nullable final Set<String> mimeTypes, final boolean useCaches) throws Throwable {

            // Note: no caches used at this moment, so useCaches is ignored

            // Start a new read-only datastore TX to retrieve file metadata
            final DataTransaction transaction = Server.this.dataStore.begin(true);

            try {
                // Retrieve file metadata stored as part of the resource record
                final Record resource = transaction.lookup(KS.RESOURCE,
                        ImmutableSet.of(resourceID), ImmutableSet.of(KS.STORED_AS)).getUnique();

                // Return null if resource does not exist
                if (resource == null) {
                    return null; // resource does not exist
                }

                // Retrieve the file metadata; return null if there is no file stored
                final Record metadata = resource.getUnique(KS.STORED_AS, Record.class);
                if (metadata == null) {
                    return null;
                }

                // Retrieve the stored file name (must exist)
                final String fileName = metadata.getUnique(NFO.FILE_NAME, String.class);
                check(fileName != null, null, resourceID, "No filename stored for resource (!)");

                // Check mimeType constraint, if any
                String transformToType = null;
                final String fileTypeString = metadata.getUnique(NIE.MIME_TYPE, String.class);
                if (mimeTypes != null) {
                    check(fileTypeString != null, Status.ERROR_NOT_ACCEPTABLE, resourceID,
                            "No MIME type stored for file %s", fileName);
                    boolean compatible = false;
                    final MediaType fileType = MediaType.parse(fileTypeString);
                    for (final String type : mimeTypes) {
                        try {
                            final boolean matches = fileType.is(MediaType.parse(type)
                                    .withoutParameters());
                            final boolean transform = !matches && !compatible
                                    && canTransform(fileTypeString, type);
                            compatible = compatible || matches || transform;
                            transformToType = transform ? type : null;
                        } catch (final IllegalArgumentException ex) {
                            // ignore error if supplied mime type is malformed
                        }
                    }
                    check(compatible, Status.ERROR_NOT_ACCEPTABLE, resourceID,
                            "Incompatible MIME type %s for file %s", fileType, fileName);
                }

                // Open a stream over file contents
                InputStream stream = Server.this.fileStore.read(fileName);
                check(stream != null, null, resourceID, "File %s missing for resource %s (!)",
                        fileName);

                if (transformToType != null) {
                    // Transformation required: do it and return a subset of metadata
                    final String ext = Iterables.getFirst(
                            Data.mimeTypeToExtensions(transformToType), "bin");
                    final String name = Objects.firstNonNull(
                            metadata.getUnique(NFO.FILE_NAME, String.class, null), "download")
                            + "." + ext;
                    stream = transform(fileTypeString, transformToType, stream);
                    final Representation representation = Representation.create(stream);
                    final Record meta = representation.getMetadata();
                    meta.setID(metadata.getID());
                    meta.set(NIE.MIME_TYPE, transformToType);
                    meta.set(NFO.FILE_NAME, name);
                    meta.set(NFO.FILE_LAST_MODIFIED, metadata.getUnique(NFO.FILE_LAST_MODIFIED));
                    return representation;

                } else {
                    // No transformation required: build and return the resulting representation
                    final Representation representation = Representation.create(stream);
                    representation.getMetadata().setID(metadata.getID());
                    for (final URI property : metadata.getProperties()) {
                        representation.getMetadata().set(property, metadata.get(property));
                    }
                    return representation;
                }

            } finally {
                // End the transaction (commit or rollback is irrelevant)
                transaction.end(true);
            }
        }

        private boolean canTransform(final String fromType, final String toType) {
            return toType.trim().toLowerCase().equals("text/html");
        }

        private InputStream transform(final String fromType, final String toType,
                final InputStream fromStream) throws IOException {
            // TODO inefficient + conversion to String may not work as charset is unknown
            final byte[] data = ByteStreams.toByteArray(fromStream);
            final String string = new String(data, Charsets.UTF_8);
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final OutputStreamWriter writer = new OutputStreamWriter(out, Charsets.UTF_8);
            writer.append("<html>\n");
            writer.append("<head>\n");
            writer.append("<meta http-equiv=\"Content-type\" "
                    + "content=\"text/html;charset=UTF-8\"/>\n");
            writer.append("</head>\n");
            writer.append("<body>\n");
            writer.append("<pre>");
            writer.append(HtmlEscapers.htmlEscaper().escape(string));
            writer.append("</pre>\n");
            writer.append("</body>\n");
            writer.append("</html>\n");
            writer.close();
            return new ByteArrayInputStream(out.toByteArray());
        }

        @Override
        protected Outcome doUpload(@Nullable final Long timeout, final URI resourceID,
                @Nullable final Representation representation) throws Throwable {

            // Keep track of the new file name and the status to return
            String fileName = null;
            Status status;

            // Start a read write datastore TX to update resource metadata
            final DataTransaction transaction = Server.this.dataStore.begin(false);

            try {
                // Retrieve the resource record and the old metadata; fail if it does not exist
                final Record resource = transaction.lookup(KS.RESOURCE,
                        ImmutableSet.of(resourceID), null).getUnique();
                if (resource == null) {
                    throw newException(Status.ERROR_DEPENDENCY_NOT_FOUND, resourceID,
                            "Specified resource does not exist");
                }

                // Retrieve old metadata
                final Record oldMetadata = resource.getUnique(KS.STORED_AS, Record.class);

                // Differentiate between delete and store representation
                if (representation == null) {
                    // In case of deletions, update the resource record dropping the file metadata
                    status = oldMetadata == null ? Status.OK_UNMODIFIED : Status.OK_DELETED;
                    resource.set(KS.STORED_AS, null);

                } else {
                    // Otherwise, assign file name and file type, considering supplied values
                    status = oldMetadata == null ? Status.OK_CREATED : Status.OK_MODIFIED;
                    final Record metadata = representation.getMetadata();
                    metadata.setID(Data.getValueFactory().createURI(resourceID + "_file"));
                    fileName = metadata.getUnique(NFO.FILE_NAME, String.class);
                    String fileType = metadata.getUnique(NIE.MIME_TYPE, String.class);
                    if (fileType != null) {
                        try {
                            MediaType.parse(fileType);
                        } catch (final IllegalArgumentException ex) {
                            fileType = null; // invalid MIME type, drop
                            metadata.set(NIE.MIME_TYPE, null);
                        }
                    }
                    fileName = generateFileName(resourceID, fileName, fileType);
                    fileType = fileType != null ? fileType : Data.extensionToMimeType(fileName);
                    metadata.set(NFO.FILE_NAME, fileName);
                    metadata.set(NIE.MIME_TYPE, fileType);

                    // Create new file using the assigned file name
                    final OutputStream stream = Server.this.fileStore.write(fileName);
                    try {
                        // Store the representation, counting written bytes and computing MD5
                        final CountingOutputStream cos = new CountingOutputStream(stream);
                        final HashingOutputStream hos = new HashingOutputStream(Hashing.md5(), cos);
                        representation.writeTo(hos);
                        hos.close();

                        // Update metadata attributes
                        final Record hash = Record.create();
                        hash.set(NFO.HASH_ALGORITHM, "MD5");
                        hash.set(NFO.HASH_VALUE, hos.hash().toString());
                        metadata.set(NFO.HAS_HASH, hash);
                        metadata.set(NFO.FILE_SIZE, cos.getCount());
                        if (metadata.isNull(NFO.FILE_LAST_MODIFIED)) {
                            metadata.set(NFO.FILE_LAST_MODIFIED, new Date());
                        }
                    } finally {
                        stream.close();
                    }

                    // Update the resource record
                    resource.set(KS.STORED_AS, Record.create(metadata, true));
                }

                // Update the resource record if necessary.
                if (status != Status.OK_UNMODIFIED) {
                    transaction.store(KS.RESOURCE, resource);
                }

                // Always delete the old file, if previously stored
                if (oldMetadata != null) {
                    deleteFileQuietly(oldMetadata.getUnique(NFO.FILE_NAME, String.class));
                }

                // Commit transaction
                transaction.end(true);

                // Compute and return outcome
                return newOutcome(status, resourceID, null);

            } catch (final Throwable ex) {
                // Rollback changes on failure
                deleteFileQuietly(fileName);
                transaction.end(false);
                throw ex;
            }
        }

        private String generateFileName(final URI resourceID,
                @Nullable final String suppliedFileName, @Nullable final String suppliedFileType) {

            // Start with default values for file name, extension and MIME type
            String fileName = "file";
            String fileExt = "bin"; // default ext for application/octet-stream

            // Revise file name, extension and MIME type from supplied fileName, if any
            if (suppliedFileName != null) {
                final String name = UrlEscapers.urlPathSegmentEscaper().escape(suppliedFileName);
                final int index = name.lastIndexOf('.');
                if (index > 0 && index < name.length() - 1) {
                    fileName = name.substring(0, index);
                    fileExt = name.substring(index + 1);
                }
            }

            // Revise file extension and/or MIME type based on supplied MIME type, if any
            if (suppliedFileType != null) {
                final List<String> mimeExtensions = Data.mimeTypeToExtensions(suppliedFileType);
                if (!mimeExtensions.isEmpty()) {
                    fileExt = mimeExtensions.get(0);
                }
            }

            // Revise file name based on resource ID, if possible
            final String uri = resourceID.stringValue();
            int start = 0;
            int end = uri.length();
            for (int index = 0; index < uri.length(); ++index) {
                final char ch = uri.charAt(index);
                if (ch == '/' || ch == ':') {
                    start = index + 1;
                } else if (ch == '.') {
                    end = index;
                } else if (ch == '#' || ch == '?') {
                    end = Math.min(end, index);
                    break;
                }
            }
            if (start < end) {
                fileName = uri.substring(start, end);
            }

            // Obtain the file version
            long fileVersion;
            final long ts = System.currentTimeMillis();
            synchronized (Server.class) {
                ++Server.fileVersionCounter;
                if (Server.fileVersionCounter < ts) {
                    Server.fileVersionCounter = ts;
                }
                fileVersion = Server.fileVersionCounter;
            }

            // Generate and return the filename
            return fileName + "." + Long.toString(fileVersion, 32) + "." + fileExt;
        }

        private void deleteFileQuietly(@Nullable final String fileName) {
            if (fileName != null) {
                try {
                    Server.this.fileStore.delete(fileName);
                } catch (final Throwable ex) {
                    LOGGER.error("Failed to delete file " + fileName
                            + " (will be garbage collected)", ex);
                }
            }
        }

        @Override
        protected long doCount(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids) throws Throwable {

            // If IDs have been supplied, we prefer to retrieve the records and apply the optional
            // condition locally (more efficient if few IDs are used)
            if (ids != null) {
                return doRetrieve(timeout, type, condition, ids, condition.getProperties(), null,
                        null).count();
            }

            // Otherwise, we resort to the count operation within a read-only datastore TX
            final DataTransaction transaction = Server.this.dataStore.begin(true);
            try {
                return transaction.count(type, condition);
            } finally {
                transaction.end(true); // commit or rollback irrelevant
            }
        }

        @Override
        protected Stream<Record> doRetrieve(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids,
                @Nullable final Set<URI> properties, @Nullable final Long offset,
                @Nullable final Long limit) throws Throwable {

            // Start a read-only datastore TX that will end when the resulting cursor is closed
            final DataTransaction tx = Server.this.dataStore.begin(true);

            Stream<Record> stream;
            if (ids == null) {
                // 1st approach: do a retrieve() if no ID was supplied
                stream = tx.retrieve(type, condition, properties);

            } else {
                // 2nd approach: do a lookup() and apply condition locally
                Set<URI> props = properties;
                if (props != null && condition != null
                        && !props.containsAll(condition.getProperties())) {
                    props = Sets.union(properties, condition.getProperties());
                }
                stream = tx.lookup(type, ids, props);
                if (condition != null) {
                    stream = stream.filter(condition.asPredicate(), 0);
                }
                if (props != properties) {
                    final URI[] array = properties.toArray(new URI[properties.size()]);
                    stream = stream.transform(new Function<Record, Record>() {

                        @Override
                        public Record apply(final Record record) {
                            record.retain(array);
                            return record;
                        }

                    }, 0);
                }
            }

            // Apply offset and limit directives
            if (offset != null || limit != null) {
                stream = stream.slice(Objects.firstNonNull(offset, 0L),
                        Objects.firstNonNull(limit, Long.MAX_VALUE));
            }

            // Attach the transaction to the cursor, so that it ends when the latter is closed
            return attach(tx, stream);
        }

        @Override
        protected void doCreate(@Nullable final Long timeout, final URI type,
                @Nullable final Stream<? extends Record> records,
                final Handler<? super Outcome> handler) throws Throwable {

            modify(new RecordUpdater() {

                @Override
                public Record computeNewRecord(final URI id, @Nullable final Record oldRecord,
                        @Nullable final Record suppliedRecord) throws Throwable {
                    assert suppliedRecord != null;
                    check(oldRecord == null, Status.ERROR_OBJECT_ALREADY_EXISTS, id, null);
                    return suppliedRecord;
                }

            }, type, null, records, handler);
        }

        @Override
        protected void doMerge(@Nullable final Long timeout, final URI type,
                @Nullable final Stream<? extends Record> records,
                @Nullable final Criteria criteria, final Handler<? super Outcome> handler)
                throws Throwable {

            modify(new RecordUpdater() {

                @Override
                public Record computeNewRecord(final URI id, @Nullable final Record oldRecord,
                        @Nullable final Record suppliedRecord) throws Throwable {
                    assert suppliedRecord != null;
                    if (criteria == null) {
                        return oldRecord; // NOP
                    } else {
                        final Record record = oldRecord == null ? Record.create(id, type)
                                : Record.create(oldRecord, true);
                        criteria.merge(record, suppliedRecord);
                        return record;
                    }
                }

            }, type, null, records, handler);
        }

        @Override
        protected void doUpdate(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids,
                @Nullable final Record record, @Nullable final Criteria criteria,
                final Handler<? super Outcome> handler) throws Throwable {

            modify(new RecordUpdater() {

                @Override
                public Record computeNewRecord(final URI id, @Nullable final Record oldRecord,
                        @Nullable final Record suppliedRecord) throws Throwable {
                    assert oldRecord != null;
                    assert suppliedRecord == null;
                    final Record newRecord = Record.create(oldRecord, true);
                    criteria.merge(newRecord, record);
                    return newRecord;
                }

            }, type, condition, ids == null ? null : Stream.create(ids), handler);
        }

        @Override
        protected void doDelete(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids,
                final Handler<? super Outcome> handler) throws Throwable {

            modify(new RecordUpdater() {

                @Override
                public Record computeNewRecord(final URI id, @Nullable final Record oldRecord,
                        @Nullable final Record suppliedRecord) throws Throwable {
                    assert oldRecord != null;
                    assert suppliedRecord == null;
                    return null;
                }

            }, type, condition, ids == null ? null : Stream.create(ids), handler);
        }

        private void modify(final RecordUpdater updater, final URI type,
                @Nullable final XPath condition, @Nullable final Stream<?> recordOrIDStream,
                final Handler<? super Outcome> handler) throws Throwable {

            // If no cursor was supplied, do a retrieve operation to obtain one
            final Stream<?> stream = recordOrIDStream != null ? recordOrIDStream : //
                    retrieveIDs(type, condition);

            try {
                // Process records in chunks, keeping track of chunk start index
                stream.chunk(Server.this.chunkSize).toHandler(new Handler<List<?>>() {

                    private final AtomicLong index = new AtomicLong(0L);

                    @Override
                    public void handle(@Nullable final List<?> chunk) throws Throwable {
                        final long startIndex = this.index.get();
                        if (chunk != null) {
                            // Attempt to process the chunk in a single transaction
                            final boolean success = modifyChunk(updater, type, condition, chunk,
                                    handler, this.index, false);

                            // On failure, process elementary 1-element chunks, notifying failures
                            if (!success) {
                                this.index.set(startIndex);
                                for (int i = 0; !Thread.interrupted() && i < chunk.size(); ++i) {
                                    final List<?> newChunk = ImmutableList.of(chunk.get(i));
                                    modifyChunk(updater, type, condition, newChunk, handler,
                                            this.index, true);
                                }
                            }
                        }
                    }

                });

                // Notify handler of completion
                handler.handle(null);

            } finally {
                // Ensure to close the cursor
                closeQuietly(stream);
            }
        }

        private boolean modifyChunk(final RecordUpdater updater, final URI type,
                @Nullable final XPath condition, final List<?> suppliedRecordsOrIDs,
                final Handler<? super Outcome> handler, final AtomicLong index,
                final boolean reportFailure) throws Throwable {

            // Extract IDs and allocate list for outcomes
            final long startIndex = index.get();
            final ValueFactory factory = Data.getValueFactory();
            final int size = suppliedRecordsOrIDs.size();
            final List<Outcome> outcomes = Lists.newArrayListWithCapacity(size);
            final List<URI> ids = Lists.newArrayListWithCapacity(size);
            final List<Record> suppliedRecords = Lists.newArrayListWithExpectedSize(size);
            for (final Object input : suppliedRecordsOrIDs) {
                if (input instanceof URI) {
                    ids.add((URI) input);
                    suppliedRecords.add(null);
                } else {
                    final Record record = (Record) input;
                    ids.add(record.getID());
                    suppliedRecords.add(record);
                }
            }

            // Start a read-write TX to process the chunk
            final DataTransaction tx = Server.this.dataStore.begin(false);

            try {
                // Retrieve old records for the IDs of this chunk
                final Stream<Record> stream = tx.lookup(type, ImmutableSet.copyOf(ids), null);
                final Map<URI, Record> oldRecords = stream.toMap(new Function<Record, URI>() {

                    @Override
                    public URI apply(final Record record) {
                        return record.getID();
                    }

                }, Functions.<Record>identity());

                // Process old/new record pairs (only those whose old record matches the
                // optional condition - this must be checked again as we work in new TX)
                for (int i = 0; !Thread.interrupted() && i < size; ++i) {
                    final URI id = ids.get(i);
                    final Record oldRecord = oldRecords.get(id);
                    final Record suppliedRecord = suppliedRecords.get(i);
                    if (id == null) {
                        assert suppliedRecord != null;
                        outcomes.add(newOutcome(Status.ERROR_INVALID_INPUT, null,
                                "Missing ID for record:\n" + suppliedRecord //
                                        .toString(Data.getNamespaceMap(), true)));

                    } else if (suppliedRecord != null || oldRecord != null
                            && (condition == null || condition.evalBoolean(oldRecord))) {
                        final URI oldInvocationID = getInvocationID();
                        setInvocationID(factory.createURI(oldInvocationID + "#"
                                + index.incrementAndGet()));
                        try {
                            outcomes.add(modifyRecord(updater, tx, id, oldRecord, suppliedRecord));
                        } catch (final OperationException ex) {
                            outcomes.add(ex.getOutcome());
                        } finally {
                            setInvocationID(oldInvocationID);
                        }
                    }
                }

                // Attempt commit
                tx.end(true);

                // Notify handlers and signal success
                for (final Outcome outcome : outcomes) {
                    handler.handle(outcome);
                }
                return true;

            } catch (final Throwable ex) {
                // Log exception
                LOGGER.error("Data processing error", ex);

                // Report failure to handler, if requested to do so
                if (reportFailure) {
                    for (int i = 0; i < ids.size(); ++i) {
                        index.set(startIndex);
                        handler.handle(Outcome.create(
                                Status.ERROR_UNEXPECTED,
                                factory.createURI(getInvocationID() + "#"
                                        + index.incrementAndGet()), ids.get(i), ex.getMessage()));
                    }
                }

                // Rollback TX and signal failure
                tx.end(false);
                return false;
            }
        }

        private Outcome modifyRecord(final RecordUpdater updater,
                final DataTransaction transaction, final URI recordID,
                @Nullable final Record oldRecord, @Nullable final Record suppliedRecord)
                throws Throwable {

            // Allocate three maps where to track the modifications that have to be done
            final Set<Record> recordsToStore = Sets.newHashSet();
            final Set<Record> recordsToDelete = Sets.newHashSet();

            // Preprocess supplied record
            if (suppliedRecord != null) {
                preprocess(suppliedRecord);
            }

            // Compute the new status of the target object; if not deleted, expand and validate it
            final Record newRecord = updater.computeNewRecord(recordID, oldRecord, suppliedRecord);
            if (newRecord != null) {
                expand(newRecord);
            }

            // Register the modification for the target object, determining the status on success
            Status status = Status.OK_UNMODIFIED;
            if (newRecord == null) {
                if (oldRecord != null) {
                    recordsToDelete.add(oldRecord);
                    status = Status.OK_DELETED;
                }
            } else {
                if (oldRecord == null) {
                    recordsToStore.add(newRecord);
                    status = Status.OK_CREATED;
                } else if (!oldRecord.hash().equals(newRecord.hash())) {
                    recordsToStore.add(newRecord);
                    status = Status.OK_MODIFIED;
                }
            }
            if (status == Status.OK_UNMODIFIED) {
                return newOutcome(status, recordID, null); // nothing to do here
            }

            // Extract related records before and after the modification to be performed
            final Map<URI, Record> nilMap = ImmutableMap.of();
            final Map<URI, Record> oldMap = oldRecord == null ? nilMap : extractRelated(oldRecord);
            final Map<URI, Record> newMap = newRecord == null ? nilMap : extractRelated(newRecord);

            // For each related record, determine if it has to be changed and how
            for (final URI id : Sets.union(oldMap.keySet(), newMap.keySet())) {

                // Compute what to removed (oldRel/oldProp.) and to add (newRel/newProp.)
                final Record oldRel = oldMap.get(id);
                final Record newRel = newMap.get(id);
                final URI type = Objects.firstNonNull(oldRel, newRel).getSystemType();
                if (oldRel != null && newRel != null) {
                    for (final URI property : oldRel.getProperties()) {
                        final List<URI> newValues = newRel.get(property, URI.class);
                        if (!newValues.isEmpty()) {
                            final List<URI> oldValues = oldRel.get(property, URI.class);
                            oldRel.remove(property, newValues);
                            newRel.remove(property, oldValues);
                        }
                    }
                }
                final List<URI> nilList = ImmutableList.of();
                final List<URI> oldProperties = oldRel == null ? nilList : oldRel.getProperties();
                final List<URI> newProperties = newRel == null ? nilList : newRel.getProperties();

                // If there are changes to apply, fetch the record, update it locally, expand and
                // validate it and register the required modification (either creation or update)
                if (!oldProperties.isEmpty() || !newProperties.isEmpty()) {
                    Record related = transaction.lookup(type, ImmutableSet.of(id), null)
                            .getUnique();
                    if (related == null) {
                        related = Record.create(id, type);
                    }
                    recordsToStore.add(related);
                    for (final URI property : oldProperties) {
                        assert oldRel != null;
                        if (!property.equals(RDF.TYPE)) {
                            related.remove(property, oldRel.get(property));
                        }
                    }
                    for (final URI property : newProperties) {
                        assert newRel != null;
                        if (!property.equals(RDF.TYPE)) {
                            related.add(property, newRel.get(property));
                        }
                    }
                    expand(related);
                }
            }

            // If new state for involved records is OK, apply registered modifications
            for (final Record record : recordsToStore) {
                transaction.store(record.getSystemType(), record);
            }
            for (final Record record : recordsToDelete) {
                transaction.delete(record.getSystemType(), record.getID());
            }

            // On success, return Status referred to target object
            return newOutcome(status, recordID, null);
        }

        private Stream<URI> retrieveIDs(final URI type, @Nullable final XPath condition)
                throws Throwable {

            // Allocate a memory buffer that will overflow to disk after a certain size
            final FileBackedOutputStream buffer = new FileBackedOutputStream(
                    Server.this.bufferSize);

            try {
                // Store the IDs of all matching records one per line in the buffer
                final Writer writer = new OutputStreamWriter(buffer, Charsets.UTF_8);
                final DataTransaction tx = Server.this.dataStore.begin(true);
                Stream<Record> cursor = null;
                try {
                    cursor = tx.retrieve(type, condition, ImmutableSet.<URI>of());
                    cursor.toHandler(new Handler<Record>() {

                        @Override
                        public void handle(final Record record) throws Throwable {
                            if (record != null) {
                                writer.write(record.getID().stringValue());
                                writer.write("\n");
                            }
                        }

                    });
                } finally {
                    closeQuietly(cursor);
                    tx.end(true); // does not matter
                    writer.flush();
                }

                // Return a cursor over buffered IDs
                final BufferedReader reader = buffer.asByteSource().asCharSource(Charsets.UTF_8)
                        .openBufferedStream();
                return Stream.create(new AbstractIterator<URI>() {

                    @Override
                    protected URI computeNext() {
                        try {
                            final String line = reader.readLine();
                            return line == null ? endOfData() : Data.getValueFactory().createURI(
                                    line);
                        } catch (final Throwable ex) {
                            throw Throwables.propagate(ex);
                        }
                    }

                }).onClose(buffer);

            } catch (final Throwable ex) {
                // Release the buffer and propagate
                buffer.close();
                throw ex;
            }
        }

        @Override
        protected Stream<Record> doMatch(@Nullable final Long timeout,
                final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
                final Map<URI, Set<URI>> properties) throws Throwable {
            // TODO
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <T> Stream<T> doSparql(@Nullable final Long timeout, final Class<T> type,
                final String expression, @Nullable final Set<URI> defaultGraphs,
                @Nullable final Set<URI> namedGraphs) throws Throwable {

            // Parse the query
            final ParsedQuery parsedQuery;
            try {
                parsedQuery = SparqlHelper.parse(expression, null);
            } catch (final Throwable ex) {
                throw newException(Status.ERROR_INVALID_INPUT, null, ex.getMessage(), ex);
            }

            // Override the query dataset, if provided in the operation parameters
            Dataset dataset = parsedQuery.getDataset();
            if (defaultGraphs != null || namedGraphs != null) {
                final DatasetImpl ds = new DatasetImpl();
                final Set<URI> emptyGraphs = ImmutableSet.of();
                for (final URI graph : Objects.firstNonNull(defaultGraphs, emptyGraphs)) {
                    ds.addDefaultGraph(graph);
                }
                for (final URI graph : Objects.firstNonNull(namedGraphs, emptyGraphs)) {
                    ds.addNamedGraph(graph);
                }
                dataset = ds;
            }

            // Operate inside a triple store transaction
            final TripleTransaction tx = Server.this.tripleStore.begin(true);
            try {
                // Start executing the query, obtaining a Sesame CloseableIteration object
                final TupleExpr expr = parsedQuery.getTupleExpr();
                final CloseableIteration<BindingSet, QueryEvaluationException> iteration;
                iteration = SparqlHelper.evaluate(tx, expr, dataset, null, timeout);

                // Wrap the iteration object dependings on the requested result
                if (type == BindingSet.class) {
                    return attach(tx, (Stream<T>) RDFUtil.toBindingsStream(iteration, parsedQuery
                            .getTupleExpr().getBindingNames()));
                } else if (type == Statement.class) {
                    return (Stream<T>) attach(tx, RDFUtil.toStatementStream(iteration));
                } else if (type == Boolean.class) {
                    try {
                        return (Stream<T>) attach(tx, Stream.create(iteration.hasNext()));
                    } finally {
                        iteration.close();
                    }
                } else {
                    throw new Error("Unexpected result type: " + type);
                }

            } catch (final Throwable ex) {
                tx.end(true); // commit or rollback does not matter
                throw ex;
            }
        }

        @Override
        protected void doClose() {
            evictClosedSessions();
            // TODO
        }

    }

    private void preprocess(final Record record) throws Throwable {

        // Ignore ks:storedAs possibly supplied by clients, as it is computed with file upload
        if (KS.RESOURCE.equals(record.getSystemType())) {
            record.set(KS.STORED_AS, null);
        }

        // TODO: add here filtering logic to be applied to records coming from the client
    }

    private void expand(final Record record) throws Throwable {

        // TODO: validation and inference can be triggered here (perhaps using a Schema object)
    }

    private Map<URI, Record> extractRelated(final Record record) throws Throwable {

        // TODO: this has to be done better using some Schema object

        final URI id = record.getID();
        final URI type = record.getSystemType();

        final Map<URI, Record> map = Maps.newHashMap();
        if (type.equals(KS.RESOURCE)) {
            for (final URI mentionID : record.get(KS.HAS_MENTION, URI.class)) {
                map.put(mentionID, Record.create(mentionID, KS.MENTION).add(KS.MENTION_OF, id));
            }

        } else if (type.equals(KS.MENTION)) {
            final URI resourceID = record.getUnique(KS.MENTION_OF, URI.class);
            if (resourceID != null) {
                map.put(resourceID, Record.create(resourceID, KS.RESOURCE).add(KS.HAS_MENTION, id));
            }

        } else {
            // TODO: handle entities, axioms and contexts
            throw new Error("Unexpected type: " + type);
        }

        return map;
    }

    private interface RecordUpdater {

        @Nullable
        Record computeNewRecord(final URI id, @Nullable final Record oldRecord,
                @Nullable final Record suppliedRecord) throws Throwable;

    }

    public static Builder builder(final FileStore fileStore, final DataStore dataStore,
            final TripleStore tripleStore) {
        return new Builder(fileStore, dataStore, tripleStore);
    }

    public static class Builder {

        private final FileStore fileStore;

        private final DataStore dataStore;

        private final TripleStore tripleStore;

        @Nullable
        private Integer chunkSize;

        @Nullable
        private Integer bufferSize;

        Builder(final FileStore fileStore, final DataStore dataStore, final TripleStore tripleStore) {
            this.fileStore = Preconditions.checkNotNull(fileStore);
            this.dataStore = Preconditions.checkNotNull(dataStore);
            this.tripleStore = Preconditions.checkNotNull(tripleStore);
        }

        public Builder chunkSize(@Nullable final Integer chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder bufferSize(@Nullable final Integer bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Server build() {
            return new Server(this);
        }

    }

}
