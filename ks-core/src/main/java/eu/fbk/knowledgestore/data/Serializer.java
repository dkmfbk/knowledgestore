package eu.fbk.knowledgestore.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.XMLSchema;

// NOTE: supports only serialization and deserialization of Record, URI, BNode, Literal,
// Statement objects. For records, it is possible to specify which properties to serialize /
// deserialize.

public final class Serializer {

    private static final Set<String> KB_PREFIXES = ImmutableSet.of("dbpedia", "yago", "gn",
            "geonames", "lgdo", "lgv");

    private static final String LANG_NS = "lang:";

    private static final int TYPE_NULL = 0x00;

    private static final int TYPE_LIST = 0x10;

    private static final int TYPE_RECORD = 0x20;

    private static final int TYPE_LIT_STRING = 0x40;

    private static final int TYPE_LIT_STRING_LANG = 0x80;

    private static final int TYPE_LIT_TRUE = 0x01;

    private static final int TYPE_LIT_FALSE = 0x02;

    private static final int TYPE_LIT_LONG = 0x03;

    private static final int TYPE_LIT_INT = 0x04;

    private static final int TYPE_LIT_SHORT = 0x05;

    private static final int TYPE_LIT_BYTE = 0x06;

    private static final int TYPE_LIT_DOUBLE = 0x07;

    private static final int TYPE_LIT_FLOAT = 0x08;

    private static final int TYPE_LIT_BIG_INTEGER = 0x09;

    private static final int TYPE_LIT_BIG_DECIMAL = 0x0A;

    private static final int TYPE_LIT_DATETIME = 0x0B;

    private static final int TYPE_BNODE = 0x30;

    private static final int TYPE_URI_PLAIN = 0xC0;

    private static final int TYPE_URI_COMPRESSED = 0x0C;

    private static final int TYPE_STATEMENT = 0x0D;

    // Number serialization

    // bits len hi mask layout
    // 07 01 0x00 0x7F 0 7
    // 14 02 0x80 0x3F 10 6 8
    // 21 03 0xC0 0x1F 110 5 8 8
    // 28 04 0xE0 0x0F 1110 4 8 8 8
    // 35 05 0xF0 0x07 11110 3 8 8 8 8
    // 42 06 0xF8 0x03 111110 2 8 8 8 8 8
    // 49 07 0xFC 0x01 1111110 1 8 8 8 8 8 8
    // 56 08 0xFE 0x00 11111110 8 8 8 8 8 8 8
    // 64 09 0xFF 0x00 11111111 8 8 8 8 8 8 8 8

    private final boolean compress;

    @Nullable
    private final Dictionary<URI> dictionary;

    private final ValueFactory factory;

    public Serializer() {
        this(false, null, null);
    }

    public Serializer(final boolean compress, @Nullable final Dictionary<URI> dictionary,
            @Nullable final ValueFactory factory) {
        this.compress = compress;
        this.dictionary = dictionary;
        this.factory = MoreObjects.firstNonNull(factory, Data.getValueFactory());
    }

    public byte[] toBytes(final Object object) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            toStream(stream, object);
            return stream.toByteArray();
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public Object fromBytes(final byte[] bytes) {
        try {
            return fromStream(new ByteArrayInputStream(bytes));
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public void toStream(final OutputStream stream, final Object object) throws IOException {
        if (this.compress) {
            final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
            final DeflaterOutputStream compressStream = new DeflaterOutputStream(stream, deflater);
            writeObject(compressStream, object);
            compressStream.finish();
        } else {
            writeObject(stream, object);
        }
    }

    public Object fromStream(final InputStream stream) throws IOException {
        if (this.compress) {
            final Inflater inflater = new Inflater(true);
            final InflaterInputStream compressStream = new InflaterInputStream(stream, inflater);
            return readObject(compressStream);
        } else {
            return readObject(stream);
        }
    }

    private void writeObject(final OutputStream stream, final Object object) throws IOException {

        if (object == null) {
            writeHeader(stream, TYPE_NULL, 0);

        } else if (object instanceof Iterable<?>) {
            final Iterable<?> iterable = (Iterable<?>) object;
            final int size = Iterables.size(iterable);
            writeHeader(stream, TYPE_LIST, size);
            for (final Object element : iterable) {
                writeObject(stream, element);
            }

        } else if (object instanceof Record) {
            final Record record = (Record) object;
            writeHeader(stream, TYPE_RECORD, record.getProperties().size());
            writeObject(stream, record.getID());
            for (final URI property : record.getProperties()) {
                writeCompressedURI(stream, property);
                final List<? extends Object> nodes = record.get(property);
                writeObject(stream, nodes.size() == 1 ? nodes.get(0) : nodes);
            }

        } else if (object instanceof Literal) {
            final Literal literal = (Literal) object;
            final URI datatype = literal.getDatatype();
            if (datatype == null || datatype.equals(XMLSchema.STRING)) {
                final String language = literal.getLanguage();
                final byte[] label = encodeString(literal.getLabel());
                if (language == null) {
                    writeHeader(stream, TYPE_LIT_STRING, label.length);
                } else {
                    writeHeader(stream, TYPE_LIT_STRING_LANG, label.length);
                    final URI langURI = this.factory.createURI("lang:" + language);
                    writeCompressedURI(stream, langURI);
                }
                stream.write(label);
            } else if (datatype.equals(XMLSchema.BOOLEAN)) {
                writeHeader(stream, literal.booleanValue() ? TYPE_LIT_TRUE : TYPE_LIT_FALSE, 0);
            } else if (datatype.equals(XMLSchema.LONG)) {
                writeHeader(stream, TYPE_LIT_LONG, 0);
                writeNumber(stream, literal.longValue());
            } else if (datatype.equals(XMLSchema.INT)) {
                writeHeader(stream, TYPE_LIT_INT, 0);
                writeNumber(stream, literal.longValue());
            } else if (datatype.equals(XMLSchema.DOUBLE)) {
                writeHeader(stream, TYPE_LIT_DOUBLE, 0);
                stream.write(Longs.toByteArray(Double.doubleToLongBits(literal.doubleValue())));
            } else if (datatype.equals(XMLSchema.FLOAT)) {
                writeHeader(stream, TYPE_LIT_FLOAT, 0);
                stream.write(Ints.toByteArray(Float.floatToIntBits(literal.floatValue())));
            } else if (datatype.equals(XMLSchema.SHORT)) {
                writeHeader(stream, TYPE_LIT_SHORT, 0);
                writeNumber(stream, literal.longValue());
            } else if (datatype.equals(XMLSchema.BYTE)) {
                writeHeader(stream, TYPE_LIT_BYTE, 0);
                writeNumber(stream, literal.longValue());
            } else if (datatype.equals(XMLSchema.INTEGER)) {
                writeHeader(stream, TYPE_LIT_BIG_INTEGER, 0);
                final byte[] bytes = literal.integerValue().toByteArray();
                writeNumber(stream, bytes.length);
                stream.write(bytes);
            } else if (datatype.equals(XMLSchema.DECIMAL)) {
                writeHeader(stream, TYPE_LIT_BIG_DECIMAL, 0);
                final byte[] bytes = encodeString(literal.decimalValue().toString());
                writeNumber(stream, bytes.length);
                stream.write(bytes);
            } else if (datatype.equals(XMLSchema.DATETIME)) {
                writeHeader(stream, TYPE_LIT_DATETIME, 0);
                final XMLGregorianCalendar calendar = literal.calendarValue();
                writeNumber(stream, calendar.getTimezone());
                writeNumber(stream, calendar.toGregorianCalendar().getTimeInMillis());
            } else {
                throw new UnsupportedOperationException("Don't know how to serialize: " + literal);
            }

        } else if (object instanceof BNode) {
            final byte[] id = encodeString(((BNode) object).getID());
            writeHeader(stream, TYPE_BNODE, id.length);
            stream.write(id);

        } else if (object instanceof URI) {
            final URI uri = (URI) object;
            if (isVocabTerm(uri)) {
                writeHeader(stream, TYPE_URI_COMPRESSED, 0);
                writeCompressedURI(stream, uri);
            } else {
                final byte[] string = encodeString(uri.stringValue());
                writeHeader(stream, TYPE_URI_PLAIN, string.length);
                stream.write(string);
            }

        } else if (object instanceof Statement) {
            final Statement statement = (Statement) object;
            writeHeader(stream, TYPE_STATEMENT, 0);
            writeObject(stream, statement.getSubject());
            writeObject(stream, statement.getPredicate());
            writeObject(stream, statement.getObject());
            writeObject(stream, statement.getContext());

        } else {
            throw new UnsupportedOperationException("Don't know how to serialize "
                    + object.getClass());
        }
    }

    private void writeHeader(final OutputStream stream, final int type, final int number)
            throws IOException {
        if ((type & 0xC0) != 0 && number <= 62) {
            stream.write(type | number + 1);
        } else if ((type & 0x30) != 0 && number <= 14) {
            stream.write(type | number + 1);
        } else if ((type & 0xF0) != 0) {
            stream.write(type);
            writeNumber(stream, number);
        } else {
            stream.write(type);
        }
    }

    private void writeCompressedURI(final OutputStream stream, final URI uri) throws IOException {
        if (this.dictionary != null) {
            final int key = this.dictionary.keyFor(uri, true);
            writeNumber(stream, key);
        } else {
            final String ns = uri.getNamespace();
            if (LANG_NS.equals(ns)) {
                final byte[] utf8 = encodeString(uri.getLocalName());
                writeNumber(stream, utf8.length << 2 | 1);
                stream.write(utf8);
            } else {
                final String prefix = Data.namespaceToPrefix(uri.getNamespace(),
                        Data.getNamespaceMap());
                if (prefix != null) {
                    final byte[] utf8 = encodeString(prefix + ":" + uri.getLocalName());
                    writeNumber(stream, utf8.length << 2 | 3);
                    stream.write(utf8);
                } else {
                    final byte[] utf8 = encodeString(uri.stringValue());
                    writeNumber(stream, utf8.length << 1);
                    stream.write(utf8);
                }
            }
        }
    }

    private void writeNumber(final OutputStream stream, final long num) throws IOException {
        if (num < 0L || num > 0xFFFFFFFFFFFFFFL /* 56 bit */) {
            writeNumberHelper(stream, 9, 0xFF, num);
        } else if (num <= 0x7FL /* 7 bit */) {
            writeNumberHelper(stream, 1, 0x00, num);
        } else if (num <= 0x3FFFL /* 14 bit */) {
            writeNumberHelper(stream, 2, 0x80, num);
        } else if (num <= 0x1FFFFFL /* 21 bit */) {
            writeNumberHelper(stream, 3, 0xC0, num);
        } else if (num <= 0xFFFFFFFL /* 28 bit */) {
            writeNumberHelper(stream, 4, 0xE0, num);
        } else if (num <= 0x7FFFFFFFFL /* 35 bit */) {
            writeNumberHelper(stream, 5, 0xF0, num);
        } else if (num <= 0x3FFFFFFFFFFL /* 42 bit */) {
            writeNumberHelper(stream, 6, 0xF8, num);
        } else if (num <= 0x1FFFFFFFFFFFFL /* 49 bit */) {
            writeNumberHelper(stream, 7, 0xFC, num);
        } else {
            writeNumberHelper(stream, 8, 0xFE, num);
        }
    }

    private void writeNumberHelper(final OutputStream stream, final int len, final int mask,
            final long num) throws IOException {
        stream.write(mask | (int) (num >>> (len - 1) * 8));
        for (int i = len - 2; i >= 0; --i) {
            stream.write((int) (num >>> i * 8 & 0xFF));
        }
    }

    private Object readObject(final InputStream stream) throws IOException {

        // Read header: type and optional number used later for parsing
        int type = stream.read();
        if (type < 0) {
            throw new EOFException();
        }
        int num = 0;
        if ((type & 0xC0) != 0) {
            final int n = type & 0x3F;
            num = n > 0 ? n - 1 : (int) readNumber(stream);
            type = type & 0xC0;
        } else if ((type & 0x30) != 0) {
            final int n = type & 0x0F;
            num = n > 0 ? n - 1 : (int) readNumber(stream);
            type = type & 0x30;
        }

        // Read the remainder based on parsed type
        switch (type) {
        case TYPE_NULL:
            return null;

        case TYPE_LIST:
            final List<Object> list = Lists.newArrayListWithCapacity(num);
            for (int i = 0; i < num; ++i) {
                list.add(readObject(stream));
            }
            return list;

        case TYPE_RECORD:
            final Record record = Record.create();
            record.setID((URI) readObject(stream));
            for (int i = 0; i < num; ++i) {
                final URI property = readCompressedURI(stream);
                final Object value = readObject(stream);
                record.set(property, value);
            }
            return record;

        case TYPE_BNODE:
            final String bnodeID = decodeString(readBytes(stream, num));
            return this.factory.createBNode(bnodeID);

        case TYPE_URI_COMPRESSED:
            return readCompressedURI(stream);

        case TYPE_URI_PLAIN:
            final String uriString = decodeString(readBytes(stream, num));
            return this.factory.createURI(uriString);

        case TYPE_LIT_STRING:
            final String plainLabel = decodeString(readBytes(stream, num));
            return this.factory.createLiteral(plainLabel);

        case TYPE_LIT_STRING_LANG:
            final String lang = readCompressedURI(stream).getLocalName();
            final String label = decodeString(readBytes(stream, num));
            return this.factory.createLiteral(label, lang);

        case TYPE_LIT_TRUE:
            return this.factory.createLiteral(true);

        case TYPE_LIT_FALSE:
            return this.factory.createLiteral(false);

        case TYPE_LIT_LONG:
            final long longVal = readNumber(stream);
            return this.factory.createLiteral(longVal);

        case TYPE_LIT_INT:
            final int intVal = (int) readNumber(stream);
            return this.factory.createLiteral(intVal);

        case TYPE_LIT_SHORT:
            final short shortVal = (short) readNumber(stream);
            return this.factory.createLiteral(shortVal);

        case TYPE_LIT_BYTE:
            final byte byteVal = (byte) readNumber(stream);
            return this.factory.createLiteral(byteVal);

        case TYPE_LIT_DOUBLE:
            final byte[] doubleBytes = readBytes(stream, 8);
            final double doubleVal = Double.longBitsToDouble(Longs.fromByteArray(doubleBytes));
            return this.factory.createLiteral(doubleVal);

        case TYPE_LIT_FLOAT:
            final byte[] floatBytes = readBytes(stream, 4);
            final float floatVal = Float.intBitsToFloat(Ints.fromByteArray(floatBytes));
            return this.factory.createLiteral(floatVal);

        case TYPE_LIT_BIG_INTEGER:
            final int bigintLen = (int) readNumber(stream);
            final String bigintVal = new BigInteger(readBytes(stream, bigintLen)).toString();
            return this.factory.createLiteral(bigintVal, XMLSchema.INTEGER);

        case TYPE_LIT_BIG_DECIMAL:
            final int bigdecLen = (int) readNumber(stream);
            final String bigdecVal = decodeString(readBytes(stream, bigdecLen));
            return this.factory.createLiteral(bigdecVal, XMLSchema.DECIMAL);

        case TYPE_LIT_DATETIME:
            final int tz = (int) readNumber(stream);
            final long millis = readNumber(stream);
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTimeInMillis(millis);
            calendar.setTimeZone(TimeZone.getTimeZone(String.format("GMT%s%02d:%02d",
                    tz >= 0 ? "+" : "-", Math.abs(tz) / 60, Math.abs(tz) % 60)));
            return this.factory.createLiteral(Data.getDatatypeFactory().newXMLGregorianCalendar(
                    calendar));

        case TYPE_STATEMENT:
            final Resource subj = (Resource) readObject(stream);
            final URI pred = (URI) readObject(stream);
            final Value obj = (Value) readObject(stream);
            final Resource ctx = (Resource) readObject(stream);
            return ctx == null ? this.factory.createStatement(subj, pred, obj) : this.factory
                    .createStatement(subj, pred, obj, ctx);

        default:
            throw new UnsupportedOperationException("Don't know how to deserialize type " + type);
        }
    }

    private byte[] readBytes(final InputStream stream, final int length) throws IOException {
        final byte[] bytes = new byte[length];
        ByteStreams.readFully(stream, bytes);
        return bytes;
    }

    private URI readCompressedURI(final InputStream stream) throws IOException {
        if (this.dictionary != null) {
            final int key = (int) readNumber(stream);
            return this.dictionary.objectFor(key);
        } else {
            final int header = (int) readNumber(stream);
            if ((header & 0x1) == 0) {
                final String string = decodeString(readBytes(stream, header >> 1));
                return this.factory.createURI(string);
            } else {
                final String string = decodeString(readBytes(stream, header >> 2));
                return (header & 0x3) == 1 ? this.factory.createURI(LANG_NS, string) //
                        : (URI) Data.parseValue(string, Data.getNamespaceMap());
            }
        }
    }

    private long readNumber(final InputStream stream) throws IOException {
        final int b = stream.read();
        if (b < 0) {
            throw new EOFException();
        }
        if (b <= 0x00 + 0x7F) {
            return readNumberHelper(stream, 1, b & 0x7F);
        } else if (b <= 0x80 + 0x3F) {
            return readNumberHelper(stream, 2, b & 0x3F);
        } else if (b <= 0xC0 + 0x1F) {
            return readNumberHelper(stream, 3, b & 0x1F);
        } else if (b <= 0xE0 + 0x0F) {
            return readNumberHelper(stream, 4, b & 0x0F);
        } else if (b <= 0xF0 + 0x07) {
            return readNumberHelper(stream, 5, b & 0x07);
        } else if (b <= 0xF8 + 0x03) {
            return readNumberHelper(stream, 6, b & 0x03);
        } else if (b <= 0xFC + 0x01) {
            return readNumberHelper(stream, 7, b & 0x01);
        } else if (b <= 0xFE + 0x01) {
            return readNumberHelper(stream, 8, b & 0x00);
        } else {
            return readNumberHelper(stream, 9, b & 0x00);
        }
    }

    private long readNumberHelper(final InputStream stream, final int len, final int start)
            throws IOException {
        long num = start;
        for (int i = 1; i < len; ++i) {
            final int c = stream.read();
            if (c < 0) {
                throw new EOFException();
            }
            num = num << 8 | c & 0xFF;
        }
        return num;
    }

    private byte[] encodeString(final String string) {
        // return string.getBytes(Charsets.UTF_8);
        return Smaz.compress(string);
    }

    private String decodeString(final byte[] bytes) {
        // return new String(bytes, Charsets.UTF_8);
        return Smaz.decompress(bytes);
    }

    private static boolean isVocabTerm(final URI uri) {
        final String prefix = Data.namespaceToPrefix(uri.getNamespace(), Data.getNamespaceMap());
        return prefix != null && !KB_PREFIXES.contains(prefix);
    }

}
