package eu.fbk.knowledgestore.internal.jaxrs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import com.google.common.reflect.TypeToken;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.Logging;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;
import eu.fbk.rdfpro.tql.TQL;

@Provider
@Consumes(MediaType.WILDCARD)
@Produces(MediaType.WILDCARD)
public class Serializer implements MessageBodyReader<Object>, MessageBodyWriter<Object> {

    // TODO: supported types depend on imported libraries

    private static final Logger LOGGER = LoggerFactory.getLogger(Serializer.class);

    @Override
    public boolean isReadable(final Class<?> type, final Type genericType,
            final Annotation[] annotations, final MediaType mediaType) {

        final boolean result = type.isAssignableFrom(Representation.class)
                || isAssignable(genericType, Protocol.STREAM_OF_RECORDS.getType())
                || isAssignable(genericType, Protocol.STREAM_OF_OUTCOMES.getType())
                || isAssignable(genericType, Protocol.STREAM_OF_STATEMENTS.getType())
                || isAssignable(genericType, Protocol.STREAM_OF_TUPLES.getType())
                || isAssignable(genericType, Protocol.STREAM_OF_BOOLEANS.getType());

        if (!result) {
            LOGGER.debug("Non deserializable stream: {} ({})", genericType, mediaType);
        }

        return result;
    }

    @Override
    public boolean isWriteable(final Class<?> type, final Type genericType,
            final Annotation[] annotations, final MediaType mediaType) {

        final boolean result = Representation.class.isAssignableFrom(type)
                || isAssignable(Protocol.STREAM_OF_RECORDS.getType(), genericType)
                || isAssignable(Protocol.STREAM_OF_OUTCOMES.getType(), genericType)
                || isAssignable(Protocol.STREAM_OF_STATEMENTS.getType(), genericType)
                || isAssignable(Protocol.STREAM_OF_TUPLES.getType(), genericType)
                || isAssignable(Protocol.STREAM_OF_BOOLEANS.getType(), genericType);

        if (!result) {
            LOGGER.debug("Non serializable stream: {} ({})", genericType, mediaType);
        }

        return result;
    }

    @Override
    public long getSize(final Object object, final Class<?> type, final Type genericType,
            final Annotation[] annotations, final MediaType mediaType) {
        throw new UnsupportedOperationException(); // JAX-RS promises never to call this method
    }

    @SuppressWarnings("resource")
    @Override
    public Object readFrom(final Class<Object> type, final Type genericType,
            final Annotation[] annotations, final MediaType mediaType,
            final MultivaluedMap<String, String> headers, final InputStream input)
            throws IOException, WebApplicationException {

        final String mimeType = mediaType.getType() + "/" + mediaType.getSubtype();

        final CountingInputStream in = new CountingInputStream(input);
        final boolean chunked = "true".equalsIgnoreCase(headers.getFirst(Protocol.HEADER_CHUNKED));
        final long ts = System.currentTimeMillis();

        try {
            if (type.isAssignableFrom(Representation.class)) {
                final InputStream stream = interceptClose(in, ts);
                final Representation representation = Representation.create(stream);
                readMetadata(representation.getMetadata(), headers);
                return representation;

            } else if (isAssignable(genericType, Protocol.STREAM_OF_RECORDS.getType())) {
                final RDFFormat format = formatFor(mimeType);
                final AtomicLong numStatements = new AtomicLong();
                final AtomicLong numRecords = new AtomicLong();
                Stream<Statement> statements = RDFUtil.readRDF(in, format, null, null, false);
                statements = statements.track(numStatements, null);
                Stream<Record> records = Record.decode(statements, null, chunked);
                records = records.track(numRecords, null);
                interceptClose(records, in, ts, numRecords, "record(s)", numStatements,
                        "statement(s)");
                return records;

            } else if (isAssignable(genericType, Protocol.STREAM_OF_OUTCOMES.getType())) {
                final RDFFormat format = formatFor(mimeType);
                final AtomicLong numStatements = new AtomicLong();
                final AtomicLong numOutcomes = new AtomicLong();
                Stream<Statement> statements = RDFUtil.readRDF(in, format, null, null, false);
                statements = statements.track(numStatements, null);
                Stream<Outcome> outcomes = Outcome.decode(statements, chunked);
                outcomes = outcomes.track(numOutcomes, null);
                interceptClose(outcomes, in, ts, numOutcomes, "outcome(s)", numStatements,
                        "statement(s)");
                return outcomes;

            } else if (isAssignable(genericType, Protocol.STREAM_OF_STATEMENTS.getType())) {
                final RDFFormat format = formatFor(mimeType);
                final AtomicLong numStatements = new AtomicLong();
                Stream<Statement> statements = RDFUtil.readRDF(in, format, null, null, false);
                statements = statements.track(numStatements, null);
                interceptClose(statements, in, ts, numStatements, "statement(s)");
                return statements;

            } else if (isAssignable(genericType, Protocol.STREAM_OF_TUPLES.getType())) {
                final TupleQueryResultFormat format;
                format = TupleQueryResultFormat.forMIMEType(mimeType);
                final AtomicLong numTuples = new AtomicLong();
                Stream<BindingSet> tuples = RDFUtil.readSparqlTuples(format, in);
                tuples = tuples.track(numTuples, null);
                interceptClose(tuples, in, ts, numTuples, "tuple(s)");
                return tuples;

            } else if (isAssignable(genericType, Protocol.STREAM_OF_BOOLEANS.getType())) {
                final BooleanQueryResultFormat format;
                format = BooleanQueryResultFormat.forMIMEType(mimeType);
                final boolean result = RDFUtil.readSparqlBoolean(format, in);
                final Stream<Boolean> stream = Stream.create(result);
                interceptClose(stream, in, ts, 1, "boolean");
                return stream;
            }
        } catch (final Throwable ex) {
            Util.closeQuietly(in); // done even if advised against it
            Throwables.propagateIfPossible(ex, IOException.class);
            throw Throwables.propagate(ex);
        }

        throw new IllegalArgumentException("Cannot deserialize " + genericType + " from "
                + mimeType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeTo(final Object object, final Class<?> type, final Type genericType,
            final Annotation[] annotations, final MediaType mediaType,
            final MultivaluedMap<String, Object> headers, final OutputStream output)
            throws IOException, WebApplicationException {

        final String mimeType = mediaType.getType() + "/" + mediaType.getSubtype();

        final Map<String, String> namespaces = Data.getNamespaceMap();
        final CountingOutputStream out = new CountingOutputStream(output);
        final long ts = System.currentTimeMillis();

        try {
            if (Representation.class.isAssignableFrom(type)) {
                final Representation representation = (Representation) object;
                writeMetadata(representation.getMetadata(), headers);
                representation.writeTo(out);
                logWrite(ts, out);

            } else if (isAssignable(Protocol.STREAM_OF_RECORDS.getType(), genericType)) {
                headers.putSingle(Protocol.HEADER_CHUNKED, "true");
                final String mime = setupType(mimeType, Protocol.MIME_TYPES_RDF, headers);
                final RDFFormat format = formatFor(mime);
                final AtomicLong recordCounter = new AtomicLong();
                Stream<? extends Record> records = (Stream<? extends Record>) object;
                records = records.track(recordCounter, null);
                final Stream<Statement> stmt = Record.encode(records, null);
                final long count = RDFUtil.writeRDF(out, format, namespaces, null, stmt);
                logWrite(ts, out, recordCounter.get(), "record(s)", count, "statement(s)");

            } else if (isAssignable(Protocol.STREAM_OF_OUTCOMES.getType(), genericType)) {
                headers.putSingle(Protocol.HEADER_CHUNKED, "true");
                final String mime = setupType(mimeType, Protocol.MIME_TYPES_RDF, headers);
                final RDFFormat format = formatFor(mime);
                final AtomicLong outcomeCounter = new AtomicLong();
                Stream<? extends Outcome> outcomes = (Stream<? extends Outcome>) object;
                outcomes = outcomes.track(outcomeCounter, null);
                final Stream<Statement> stmt = Outcome.encode(outcomes);
                final long count = RDFUtil.writeRDF(out, format, namespaces, null, stmt);
                logWrite(ts, out, outcomeCounter.get(), "outcome(s)", count, "statement(s)");

            } else if (isAssignable(Protocol.STREAM_OF_STATEMENTS.getType(), genericType)) {
                final String mime = setupType(mimeType, Protocol.MIME_TYPES_RDF, headers);
                final RDFFormat format = formatFor(mime);
                final Stream<? extends Statement> stmt = (Stream<? extends Statement>) object;
                final long count = RDFUtil.writeRDF(out, format, namespaces, null, stmt);
                logWrite(ts, out, count, "statement(s)");

            } else if (isAssignable(Protocol.STREAM_OF_TUPLES.getType(), genericType)) {
                final String mime = setupType(mimeType, Protocol.MIME_TYPES_SPARQL_TUPLE, headers);
                final TupleQueryResultFormat format = TupleQueryResultFormat.forMIMEType(mime);
                final Stream<? extends BindingSet> tuples = (Stream<? extends BindingSet>) object;
                final long count = RDFUtil.writeSparqlTuples(format, out, tuples);
                logWrite(ts, out, count, "tuple(s)");

            } else if (isAssignable(Protocol.STREAM_OF_BOOLEANS.getType(), genericType)) {
                final String mime = setupType(mimeType, Protocol.MIME_TYPES_SPARQL_BOOLEAN,
                        headers);
                final BooleanQueryResultFormat format = BooleanQueryResultFormat.forMIMEType(mime);
                final boolean bool = ((Stream<? extends Boolean>) object).getUnique();
                RDFUtil.writeSparqlBoolean(format, out, bool);
                logWrite(ts, out, 1, "boolean");

            } else {
                throw new IllegalArgumentException("Cannot serialize " + genericType + " to "
                        + mediaType);
            }

        } finally {
            Util.closeQuietly(object);
            Util.closeQuietly(out); // done even if asked not to do so
        }
    }

    @Nullable
    private static void readMetadata(final Record metadata,
            final MultivaluedMap<String, String> headers) {

        // Read Content-Type header
        final String mime = headers.getFirst(HttpHeaders.CONTENT_TYPE);
        metadata.set(NIE.MIME_TYPE, mime != null ? mime : MediaType.APPLICATION_OCTET_STREAM);

        // Read Content-MD5 header, if available
        final String md5 = headers.getFirst("Content-MD5");
        if (md5 != null) {
            final Record hash = Record.create();
            hash.set(NFO.HASH_ALGORITHM, "MD5");
            hash.set(NFO.HASH_VALUE, md5);
            metadata.set(NFO.HAS_HASH, hash);
        }

        // Read Content-Language header, if possible
        final String language = headers.getFirst(HttpHeaders.CONTENT_LANGUAGE);
        try {
            metadata.set(DCTERMS.LANGUAGE, Data.languageCodeToURI(language));
        } catch (final Throwable ex) {
            LOGGER.warn("Invalid {}: {}", HttpHeaders.CONTENT_LANGUAGE, language);
        }

        // Read custom X-KS-Meta header
        final String encodedMeta = headers.getFirst(Protocol.HEADER_META);
        if (encodedMeta != null) {
            final InputStream in = new ByteArrayInputStream(encodedMeta.getBytes(Charsets.UTF_8));
            final Stream<Statement> statements = RDFUtil.readRDF(in, RDFFormat.TURTLE,
                    Data.getNamespaceMap(), null, true);
            final Record record = Record.decode(statements,
                    ImmutableSet.<URI>of(KS.REPRESENTATION), true).getUnique();
            metadata.setID(record.getID());
            for (final URI property : record.getProperties()) {
                metadata.set(property, record.get(property));
            }
        }
    }

    @Nullable
    private static void writeMetadata(final Record metadata,
            final MultivaluedMap<String, Object> headers) {

        // Write Content-Type header
        headers.putSingle(HttpHeaders.CONTENT_TYPE, metadata.getUnique(NIE.MIME_TYPE,
                String.class, MediaType.APPLICATION_OCTET_STREAM));

        // Write Content-MD5 header, if possible
        final Record hash = metadata.getUnique(NFO.HAS_HASH, Record.class, null);
        final String md5 = hash == null ? null : !"MD5".equals(hash.getUnique(NFO.HASH_ALGORITHM,
                String.class, null)) ? null //
                : hash.getUnique(NFO.HASH_VALUE, String.class, null);
        headers.putSingle("Content-MD5", md5);

        // Write Content-Language header, if possible
        String language = metadata.getUnique(NIE.LANGUAGE, String.class, null);
        if (language == null) {
            final URI languageURI = metadata.getUnique(DCTERMS.LANGUAGE, URI.class, null);
            try {
                language = Data.languageURIToCode(languageURI);
            } catch (final Throwable ex) {
                LOGGER.warn("Invalid language URI: ", languageURI);
            }
        }
        headers.putSingle(HttpHeaders.CONTENT_LANGUAGE, language);

        // Write custom X-KS-Meta header
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Stream<Statement> statements = Record.encode(Stream.create(metadata),
                ImmutableSet.<URI>of(KS.REPRESENTATION));
        RDFUtil.writeRDF(out, RDFFormat.TURTLE, Data.getNamespaceMap(), null, statements);
        final String string = new String(out.toByteArray(), Charsets.UTF_8);
        final StringBuilder builder = new StringBuilder();
        String separator = "";
        for (final String line : Splitter.on('\n').trimResults().omitEmptyStrings().split(string)) {
            if (!line.toLowerCase().startsWith("@prefix")) {
                builder.append(separator).append(line);
                separator = " ";
            }
        }
        headers.putSingle(Protocol.HEADER_META, builder.toString());
    }

    private static boolean isAssignable(final Type lhs, final Type rhs) {
        return TypeToken.of(lhs).isAssignableFrom(rhs);
    }

    private static String setupType(final String jaxrsType, final String supportedTypes,
            final MultivaluedMap<String, Object> headers) {

        if (jaxrsType != null) {
            return jaxrsType;
        }

        final int index = supportedTypes.indexOf(',');
        final String mediaType = index < 0 ? supportedTypes : supportedTypes.substring(0, index);

        headers.putSingle("Content-Type", mediaType);
        headers.remove("ETag"); // to stay on the safe side

        return mediaType;
    }

    private static void interceptClose(final Stream<?> stream, final CountingInputStream in,
            final long startTime, final Object... args) {
        final Map<String, String> mdc = Logging.getMDC();
        stream.onClose(new Runnable() {

            @Override
            public void run() {
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    logRead(in, startTime, args);
                    // closing the stream should not be done, but GZIPFilter seems not to detect
                    // EOF and does not release the underlying stream, causing the connection not
                    // to be released
                    Util.closeQuietly(in);
                } finally {
                    Logging.setMDC(oldMdc);
                }
            }

        });
    }

    private static InputStream interceptClose(final CountingInputStream stream,
            final long startTime, final Object... args) {
        final Map<String, String> mdc = Logging.getMDC();
        return new FilterInputStream(stream) {

            private boolean closed;

            @Override
            public void close() throws IOException {
                if (this.closed) {
                    return;
                }
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    logRead(stream, startTime, args);
                    // closing the stream should not be done, but GZIPFilter seems not to detect
                    // EOF and does not release the underlying stream, causing the connection not
                    // to be released
                    Util.closeQuietly(this.in);
                } finally {
                    this.closed = true;
                    Logging.setMDC(oldMdc);
                    super.close();
                }
            }

        };
    }

    private static void logRead(final CountingInputStream in, final long startTime,
            final Object... args) {
        if (LOGGER.isDebugEnabled()) {
            boolean eof = false;
            try {
                eof = in.read() == -1;
            } catch (final Throwable ex) {
                // ignore
            }
            final long elapsed = System.currentTimeMillis() - startTime;
            final StringBuilder builder = new StringBuilder();
            builder.append("Http: read complete, ");
            for (int i = 0; i < args.length; i += 2) {
                builder.append(args[i]).append(" ").append(args[i + 1]).append(", ");
            }
            builder.append(in.getCount()).append(" byte(s), ");
            if (eof) {
                builder.append("EOF, ");
            }
            builder.append(elapsed).append(" ms");
            LOGGER.debug(builder.toString());
        }
    }

    private static void logWrite(final long startTime, final CountingOutputStream stream,
            final Object... args) {
        if (LOGGER.isDebugEnabled()) {
            final long elapsed = System.currentTimeMillis() - startTime;
            final StringBuilder builder = new StringBuilder();
            builder.append("Http: write complete, ");
            for (int i = 0; i < args.length; i += 2) {
                builder.append(args[i]).append(" ").append(args[i + 1]).append(", ");
            }
            builder.append(stream.getCount()).append(" byte(s), ");
            builder.append(elapsed).append(" ms");
            LOGGER.debug(builder.toString());
        }
    }

    private static RDFFormat formatFor(final String mimeType) {
        final RDFFormat format = RDFFormat.forMIMEType(mimeType);
        if (format == null) {
            throw new IllegalArgumentException("No RDF format for MIME type '" + mimeType + "'");
        }
        return format;
    }

    static {
        RDFFormat.register(TQL.FORMAT);
    }

}
