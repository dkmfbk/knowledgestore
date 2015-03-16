package eu.fbk.knowledgestore.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.*;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

// NOTE: supports only serialization and deserialization of Record, URI, BNode, Literal,
// Statement objects. For records, it is possible to specify which properties to serialize /
// deserialize.

// TODO: add ideas from smaz/jsmaz to dictionary-compress short strings / uris
// <https://github.com/icedrake/jsmaz> (30-50% string reduction achievable)

public final class SerializerAvro {

    private final Dictionary<URI> dictionary;

    private final ValueFactory factory;

    private final DatatypeFactory datatypeFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializerAvro.class);

    public SerializerAvro() {
        this((Dictionary<URI>) null);
    }

    public SerializerAvro(@Nullable final Dictionary<URI> dictionary) {
        this.dictionary = dictionary;
        this.factory = Data.getValueFactory();
        this.datatypeFactory = Data.getDatatypeFactory();
    }

    public SerializerAvro(String fileName) throws IOException {
        this.dictionary = new Dictionary<URI>(URI.class, fileName);
        this.factory = Data.getValueFactory();
        this.datatypeFactory = Data.getDatatypeFactory();
    }

    public Dictionary<URI> getDictionary() {
        return this.dictionary;
    }

    public byte[] compressURI(final URI uri) {
        Preconditions.checkNotNull(uri);
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final Encoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
            final DatumWriter<Object> writer = new GenericDatumWriter<Object>(
                    Schemas.COMPRESSED_IDENTIFIER);
            this.dictionary.keyFor(uri); // ensure a compressed version of URI is available
            final Object generic = encodeIdentifier(uri);
            writer.write(generic, encoder);
            return stream.toByteArray();

        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public URI expandURI(final byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        try {
            final InputStream stream = new ByteArrayInputStream(bytes);
            final Decoder decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
            final DatumReader<Object> reader = new GenericDatumReader<Object>(
                    Schemas.COMPRESSED_IDENTIFIER);
            final Object generic = reader.read(null, decoder);
            return (URI) decodeNode(generic);

        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public byte[] toBytes(final Object object) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            this.toStream(stream, object);
            return stream.toByteArray();
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public byte[] toBytes(final Record object, @Nullable final Set<URI> propertiesToSerialize) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            this.toStream(stream, object, propertiesToSerialize);
            return stream.toByteArray();
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public Object fromBytes(final byte[] bytes) {
        try {
            return this.fromStream(new ByteArrayInputStream(bytes));
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public Record fromBytes(final byte[] bytes, final @Nullable Set<URI> propertiesToDeserialize) {
        try {
            return this.fromStream(new ByteArrayInputStream(bytes), propertiesToDeserialize);
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public void toStream(final OutputStream stream, final Object object) throws IOException {
        final Object generic = encodeNode(object);
        final Encoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
        final DatumWriter<Object> writer = new GenericDatumWriter<Object>(Schemas.NODE);
        writer.write(generic, encoder);
        encoder.flush();
    }

    public void toStream(final OutputStream stream, final Record object,
            @Nullable final Set<URI> propertiesToSerialize) throws IOException {
        final Object generic = encodeRecord(object, propertiesToSerialize);
        final Encoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
        final DatumWriter<Object> writer = new GenericDatumWriter<Object>(Schemas.NODE);
        writer.write(generic, encoder);
        encoder.flush();
    }

    public Object fromStream(final InputStream stream) throws IOException {
        final Decoder decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
        final DatumReader<Object> reader = new GenericDatumReader<Object>(Schemas.NODE);
        final Object generic = reader.read(null, decoder);
        return decodeNode(generic);
    }

    public Record fromStream(final InputStream stream,
            @Nullable final Set<URI> propertiesToDeserialize) throws IOException {
        final Decoder decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
        final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                Schemas.NODE);
        final GenericRecord generic = reader.read(null, decoder);
        return decodeRecord(generic, propertiesToDeserialize);
    }

    private List<Object> decodeNodes(final Object generic) {
        if (generic instanceof Iterable<?>) {
            final Iterable<?> iterable = (Iterable<?>) generic;
            final int size = Iterables.size(iterable);
            final List<Object> nodes = Lists.<Object>newArrayListWithCapacity(size);
            for (final Object element : iterable) {
                nodes.add(decodeNode(element));
            }
            return nodes;
        }
        Preconditions.checkNotNull(generic);
        return ImmutableList.of(decodeNode(generic));
    }

    private Object decodeNode(final Object generic) {
        if (generic instanceof GenericRecord) {
            final GenericRecord record = (GenericRecord) generic;
            final Schema schema = record.getSchema();
            if (schema.equals(Schemas.RECORD)) {
                return decodeRecord(record, null);
            } else if (schema.equals(Schemas.PLAIN_IDENTIFIER)
                    || schema.equals(Schemas.COMPRESSED_IDENTIFIER)) {
                return decodeIdentifier(record);
            } else if (schema.equals(Schemas.STATEMENT)) {
                return decodeStatement(record);
            }
        }
        return decodeLiteral(generic);
    }

    @SuppressWarnings("unchecked")
    private Record decodeRecord(final GenericRecord generic,
            @Nullable final Set<URI> propertiesToDecode) {
        final Record record = Record.create();
        final GenericRecord encodedID = (GenericRecord) generic.get(0);
        if (encodedID != null) {
            record.setID((URI) decodeIdentifier(encodedID));
        }
        for (final GenericRecord prop : (Iterable<GenericRecord>) generic.get(1)) {
            final URI property = (URI) decodeIdentifier((GenericRecord) prop.get(0));
            final List<Object> values = decodeNodes(prop.get(1));
            if (propertiesToDecode == null || propertiesToDecode.contains(property)) {
                record.set(property, values);
            }
        }
        return record;
    }

    private Value decodeValue(final Object generic) {
        if (generic instanceof GenericRecord) {
            final GenericRecord record = (GenericRecord) generic;
            final Schema schema = record.getSchema();
            if (schema.equals(Schemas.COMPRESSED_IDENTIFIER)
                    || schema.equals(Schemas.PLAIN_IDENTIFIER)) {
                return decodeIdentifier(record);
            }
        }
        return decodeLiteral(generic);
    }

    private Resource decodeIdentifier(final GenericRecord record) {
        final Schema schema = record.getSchema();
        if (schema.equals(Schemas.COMPRESSED_IDENTIFIER)) {
            try {
                return this.dictionary.objectFor((Integer) record.get(0));
            } catch (final IOException ex) {
                throw new IllegalStateException("Cannot access dictionary: " + ex.getMessage(), ex);
            }
        } else if (schema.equals(Schemas.PLAIN_IDENTIFIER)) {
            final String string = record.get(0).toString();
            if (string.startsWith("_:")) {
                return this.factory.createBNode(string.substring(2));
            } else {
                return this.factory.createURI(string);
            }
        }
        throw new IllegalArgumentException("Unsupported encoded identifier: " + record);
    }

    private Literal decodeLiteral(final Object generic) {
        if (generic instanceof GenericRecord) {
            final GenericRecord record = (GenericRecord) generic;
            final Schema schema = record.getSchema();
            if (schema.equals(Schemas.STRING_LANG)) {
                final String label = record.get(0).toString(); // Utf8 class used
                final Object language = record.get(1);
                return this.factory.createLiteral(label, language.toString());
            } else if (schema.equals(Schemas.SHORT)) {
                return this.factory.createLiteral(((Integer) record.get(0)).shortValue());
            } else if (schema.equals(Schemas.BYTE)) {
                return this.factory.createLiteral(((Integer) record.get(0)).byteValue());
            } else if (schema.equals(Schemas.BIGINTEGER)) {
                return this.factory.createLiteral(record.get(0).toString(), XMLSchema.INTEGER);
            } else if (schema.equals(Schemas.BIGDECIMAL)) {
                return this.factory.createLiteral(record.get(0).toString(), XMLSchema.DECIMAL);
            } else if (schema.equals(Schemas.CALENDAR)) {
                final int tz = (Integer) record.get(0);
                final GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTimeInMillis((Long) record.get(1));
                calendar.setTimeZone(TimeZone.getTimeZone(String.format("GMT%s%02d:%02d",
                        tz >= 0 ? "+" : "-", Math.abs(tz) / 60, Math.abs(tz) % 60)));
                return this.factory.createLiteral(this.datatypeFactory
                        .newXMLGregorianCalendar(calendar));
            }
        } else if (generic instanceof CharSequence) {
            return this.factory.createLiteral(generic.toString()); // Utf8 class used
        } else if (generic instanceof Boolean) {
            return this.factory.createLiteral((Boolean) generic);
        } else if (generic instanceof Long) {
            return this.factory.createLiteral((Long) generic);
        } else if (generic instanceof Integer) {
            return this.factory.createLiteral((Integer) generic);
        } else if (generic instanceof Double) {
            return this.factory.createLiteral((Double) generic);
        } else if (generic instanceof Float) {
            return this.factory.createLiteral((Float) generic);
        }
        Preconditions.checkNotNull(generic);
        throw new IllegalArgumentException("Unsupported generic data: " + generic);
    }

    private Statement decodeStatement(final GenericRecord record) {
        final Resource subj = decodeIdentifier((GenericRecord) record.get(0));
        final URI pred = (URI) decodeIdentifier((GenericRecord) record.get(1));
        final Value obj = decodeValue(record.get(2));
        final Resource ctx = decodeIdentifier((GenericRecord) record.get(3));
        if (ctx == null) {
            return this.factory.createStatement(subj, pred, obj);
        } else {
            return this.factory.createStatement(subj, pred, obj, ctx);
        }
    }

    private Object encodeNodes(final Iterable<? extends Object> nodes) {
        final int size = Iterables.size(nodes);
        if (size == 1) {
            return encodeNode(Iterables.get(nodes, 0));
        }
        final List<Object> list = Lists.<Object>newArrayListWithCapacity(size);
        for (final Object node : nodes) {
            list.add(encodeNode(node));
        }
        return list;
    }

    private Object encodeNode(final Object node) {
        if (node instanceof Record) {
            return encodeRecord((Record) node, null);
        } else if (node instanceof Literal) {
            return encodeLiteral((Literal) node);
        } else if (node instanceof Resource) {
            return encodeIdentifier((Resource) node);
        } else if (node instanceof Statement) {
            return encodeStatement((Statement) node);
        }
        Preconditions.checkNotNull(node);
        throw new IllegalArgumentException("Unsupported node: " + node);
    }

    private Object encodeRecord(final Record record, @Nullable final Set<URI> propertiesToEncode) {
        final URI id = record.getID();
        final Object encodedID = id == null ? null : encodeIdentifier(id);
        final List<Object> props = Lists.newArrayList();
        for (final URI property : record.getProperties()) {
            if (propertiesToEncode == null || propertiesToEncode.contains(property)) {
                ensureInDictionary(property);
                final List<? extends Object> nodes = record.get(property);
                if (property.equals(RDF.TYPE)) {
                    for (final Object value : nodes) {
                        if (value instanceof URI) {
                            ensureInDictionary((URI) value);
                        }
                    }
                }
                final GenericData.Record prop = new GenericData.Record(Schemas.PROPERTY);
                prop.put("propertyURI", encodeIdentifier(property));
                prop.put("propertyValue", encodeNodes(nodes));
                props.add(prop);
            }
        }
        return SerializerAvro.newGenericRecord(Schemas.RECORD, encodedID, props);
    }

    private Object encodeValue(final Value value) {
        if (value instanceof Literal) {
            return encodeLiteral((Literal) value);
        } else if (value instanceof Resource) {
            return encodeIdentifier((Resource) value);
        } else {
            throw new IllegalArgumentException("Unsupported value: " + value);
        }
    }

    private Object encodeIdentifier(final Resource identifier) {
        if (identifier instanceof URI) {
            try {
                final Integer key = this.dictionary.keyFor((URI) identifier, false);
                if (key != null) {
                    return SerializerAvro.newGenericRecord(Schemas.COMPRESSED_IDENTIFIER, key);
                }
            } catch (final IOException ex) {
                throw new IllegalStateException("Cannot access dictionary: " + ex.getMessage(), ex);
            }
        }
        final String id = identifier instanceof BNode ? "_:" + ((BNode) identifier).getID()
                : identifier.stringValue();
        return SerializerAvro.newGenericRecord(Schemas.PLAIN_IDENTIFIER, id);
    }

    private Object encodeLiteral(final Literal literal) {
        final URI datatype = literal.getDatatype();
        if (datatype == null || datatype.equals(XMLSchema.STRING)) {
            final String language = literal.getLanguage();
            if (language == null) {
                return literal.getLabel();
            } else {
                return SerializerAvro.newGenericRecord(Schemas.STRING_LANG, literal.getLabel(),
                        language);
            }
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return literal.booleanValue();
        } else if (datatype.equals(XMLSchema.LONG)) {
            return literal.longValue();
        } else if (datatype.equals(XMLSchema.INT)) {
            return literal.intValue();
        } else if (datatype.equals(XMLSchema.DOUBLE)) {
            return literal.doubleValue();
        } else if (datatype.equals(XMLSchema.FLOAT)) {
            return literal.floatValue();
        } else if (datatype.equals(XMLSchema.SHORT)) {
            return SerializerAvro.newGenericRecord(Schemas.SHORT, literal.intValue());
        } else if (datatype.equals(XMLSchema.BYTE)) {
            return SerializerAvro.newGenericRecord(Schemas.BYTE, literal.intValue());
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            return SerializerAvro.newGenericRecord(Schemas.BIGINTEGER, literal.stringValue());
        } else if (datatype.equals(XMLSchema.DECIMAL)) {
            return SerializerAvro.newGenericRecord(Schemas.BIGDECIMAL, literal.stringValue());
        } else if (datatype.equals(XMLSchema.DATETIME)) {
            final XMLGregorianCalendar calendar = literal.calendarValue();
            return SerializerAvro.newGenericRecord(Schemas.CALENDAR, calendar.getTimezone(),
                    calendar.toGregorianCalendar().getTimeInMillis());
        }
        throw new IllegalArgumentException("Unsupported literal: " + literal);
    }

    private Object encodeStatement(final Statement statement) {
        return SerializerAvro.newGenericRecord(Schemas.STATEMENT,
                encodeIdentifier(statement.getSubject()),
                encodeIdentifier(statement.getPredicate()), //
                encodeValue(statement.getObject()), //
                encodeIdentifier(statement.getContext()));
    }

    private URI ensureInDictionary(final URI uri) {
        try {
            this.dictionary.keyFor(uri);
            return uri;
        } catch (final IOException ex) {
            throw new IllegalStateException("Cannot access dictionary: " + ex.getMessage(), ex);
        }
    }

    private static GenericData.Record newGenericRecord(final Schema schema,
            final Object... fieldValues) {

        final GenericData.Record record = new GenericData.Record(schema);
        for (int i = 0; i < fieldValues.length; ++i) {
            record.put(i, fieldValues[i]);
        }
        return record;
    }

    private static final class Schemas {

        /** The namespace for KS-specific AVRO schemas. */
        public static final String NAMESPACE = "eu.fbk.knowledgestore";

        /** AVRO schema for NULL. */
        public static final Schema NULL = Schema.create(Schema.Type.NULL);

        /** AVRO schema for boolean literals. */
        public static final Schema BOOLEAN = Schema.create(Schema.Type.BOOLEAN);

        /** AVRO schema for string literals. */
        public static final Schema STRING = Schema.create(Schema.Type.STRING);

        /** AVRO schema for string literals with a language. */
        public static final Schema STRING_LANG = Schema.createRecord("stringlang", null,
                Schemas.NAMESPACE, false);

        /** AVRO schema for long literals. */
        public static final Schema LONG = Schema.create(Schema.Type.LONG);

        /** AVRO schema for int literals. */
        public static final Schema INT = Schema.create(Schema.Type.INT);

        /** AVRO schema for short literals. */
        public static final Schema SHORT = Schema.createRecord("short", null, Schemas.NAMESPACE,
                false);

        /** AVRO schema for byte literals. */
        public static final Schema BYTE = Schema.createRecord("byte", null, Schemas.NAMESPACE,
                false);

        /** AVRO schema for double literals. */
        public static final Schema DOUBLE = Schema.create(Schema.Type.DOUBLE);

        /** AVRO schema for float literals. */
        public static final Schema FLOAT = Schema.create(Schema.Type.FLOAT);

        /** AVRO schema for big integer literals. */
        public static final Schema BIGINTEGER = Schema.createRecord("biginteger", null,
                Schemas.NAMESPACE, false);

        /** AVRO schema for big decimal literals. */
        public static final Schema BIGDECIMAL = Schema.createRecord("bigdecimal", null,
                Schemas.NAMESPACE, false);

        /** AVRO schema for non-compressed IDs (URIs, BNodes). */
        public static final Schema PLAIN_IDENTIFIER = Schema //
                .createRecord("plainidentifier", null, Schemas.NAMESPACE, false);

        /** AVRO schema for compressed ID (URIs, BNodes). */
        public static final Schema COMPRESSED_IDENTIFIER = Schema //
                .createRecord("compressedidentifier", null, Schemas.NAMESPACE, false);

        /** AVRO schema for any ID (URIs, BNodes). */
        public static final Schema IDENTIFIER = Schema.createUnion(ImmutableList.<Schema>of(
                PLAIN_IDENTIFIER, COMPRESSED_IDENTIFIER));

        /** AVRO schema for calendar literals. */
        public static final Schema CALENDAR = Schema.createRecord("calendar", null,
                Schemas.NAMESPACE, false);

        /** AVRO schema for RDF statements. */
        public static final Schema STATEMENT = Schema.createRecord("statement", null,
                Schemas.NAMESPACE, false);

        /** AVRO schema for record nodes ({@code Record}). */
        public static final Schema RECORD = Schema.createRecord("struct", null, Schemas.NAMESPACE,
                false);

        /** AVRO schema for generic data model nodes. */
        public static final Schema NODE = Schema.createUnion(ImmutableList.<Schema>of(
                Schemas.BOOLEAN, Schemas.STRING, Schemas.STRING_LANG, Schemas.LONG, Schemas.INT,
                Schemas.SHORT, Schemas.BYTE, Schemas.DOUBLE, Schemas.FLOAT, Schemas.BIGINTEGER,
                Schemas.BIGDECIMAL, Schemas.PLAIN_IDENTIFIER, Schemas.COMPRESSED_IDENTIFIER,
                Schemas.CALENDAR, Schemas.STATEMENT, Schemas.RECORD));

        /** AVRO schema for lists of nodes. */
        public static final Schema LIST = Schema.createArray(Schemas.NODE);

        /** AVRO schema for properties of a record node. */
        public static final Schema PROPERTY = Schema.createRecord("property", null,
                Schemas.NAMESPACE, false);

        private Schemas() {
        }

        static {
            Schemas.STRING_LANG.setFields(ImmutableList.<Schema.Field>of(new Schema.Field("label",
                    Schemas.STRING, null, null), new Schema.Field("language", Schemas.STRING,
                    null, null)));
            Schemas.SHORT.setFields(ImmutableList.<Schema.Field>of(new Schema.Field("short",
                    Schemas.INT, null, null)));
            Schemas.BYTE.setFields(ImmutableList.<Schema.Field>of(new Schema.Field("byte",
                    Schemas.INT, null, null)));
            Schemas.BIGINTEGER.setFields(ImmutableList.<Schema.Field>of(new Schema.Field(
                    "biginteger", Schemas.STRING, null, null)));
            Schemas.BIGDECIMAL.setFields(ImmutableList.<Schema.Field>of(new Schema.Field(
                    "bigdecimal", Schemas.STRING, null, null)));
            Schemas.PLAIN_IDENTIFIER.setFields(ImmutableList.<Schema.Field>of(new Schema.Field(
                    "identifier", Schemas.STRING, null, null)));
            Schemas.COMPRESSED_IDENTIFIER.setFields(ImmutableList
                    .<Schema.Field>of(new Schema.Field("identifier", Schemas.INT, null, null)));
            Schemas.CALENDAR.setFields(ImmutableList.<Schema.Field>of(new Schema.Field("timezone",
                    Schemas.INT, null, null), new Schema.Field("timestamp", Schemas.LONG, null,
                    null)));

            Schemas.STATEMENT.setFields(ImmutableList.<Schema.Field>of(
                    new Schema.Field("subject", Schemas.IDENTIFIER, null, null),
                    new Schema.Field("predicate", Schemas.IDENTIFIER, null, null),
                    new Schema.Field("object", Schema.createUnion(ImmutableList.<Schema>of(
                            Schemas.BOOLEAN, Schemas.STRING, Schemas.STRING_LANG, Schemas.LONG,
                            Schemas.INT, Schemas.SHORT, Schemas.BYTE, Schemas.DOUBLE,
                            Schemas.FLOAT, Schemas.BIGINTEGER, Schemas.BIGDECIMAL,
                            Schemas.CALENDAR, Schemas.PLAIN_IDENTIFIER,
                            Schemas.COMPRESSED_IDENTIFIER)), null, null), //
                    new Schema.Field("context", Schemas.IDENTIFIER, null, null)));

            Schemas.PROPERTY.setFields(ImmutableList.<Schema.Field>of(
                    new Schema.Field("propertyURI", Schemas.COMPRESSED_IDENTIFIER, null, null),
                    new Schema.Field("propertyValue", Schema.createUnion(ImmutableList.<Schema>of(
                            Schemas.BOOLEAN, Schemas.STRING, Schemas.STRING_LANG, Schemas.LONG,
                            Schemas.INT, Schemas.SHORT, Schemas.BYTE, Schemas.DOUBLE,
                            Schemas.FLOAT, Schemas.BIGINTEGER, Schemas.BIGDECIMAL,
                            Schemas.CALENDAR, Schemas.PLAIN_IDENTIFIER,
                            Schemas.COMPRESSED_IDENTIFIER, Schemas.STATEMENT, Schemas.RECORD,
                            Schemas.LIST)), null, null)));

            Schemas.RECORD.setFields(ImmutableList.<Schema.Field>of(
                    new Schema.Field("id",
                            Schema.createUnion(ImmutableList.<Schema>of(Schemas.NULL,
                                    Schemas.PLAIN_IDENTIFIER, Schemas.COMPRESSED_IDENTIFIER)),
                            null, null), //
                    new Schema.Field("properties", Schema.createArray(Schemas.PROPERTY), null,
                            null)));
        }

    }

}
