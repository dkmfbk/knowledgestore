package eu.fbk.knowledgestore.datastore.hbase.utils;

import com.google.common.collect.ImmutableList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * Definition of AVRO schemas for nodes of the data model.
 */
public final class AvroSchemas {

    /** The namespace for KS-specific AVRO schemas. */
    public static final String NAMESPACE = "eu.fbk.knowledgestore";

    /** AVRO schema for NULL. */
    public static final Schema NULL = Schema.create(Type.NULL);

    /** AVRO schema for boolean literals. */
    public static final Schema BOOLEAN = Schema.create(Type.BOOLEAN);

    /** AVRO schema for string literals. */
    public static final Schema STRING = Schema.create(Type.STRING);

    /** AVRO schema for string literals with a language. */
    public static final Schema STRING_LANG = Schema.createRecord("stringlang", null,
            AvroSchemas.NAMESPACE, false);

    /** AVRO schema for long literals. */
    public static final Schema LONG = Schema.create(Type.LONG);

    /** AVRO schema for int literals. */
    public static final Schema INT = Schema.create(Type.INT);

    /** AVRO schema for short literals. */
    public static final Schema SHORT = Schema.createRecord("short", null, AvroSchemas.NAMESPACE,
            false);

    /** AVRO schema for byte literals. */
    public static final Schema BYTE = Schema.createRecord("byte", null, AvroSchemas.NAMESPACE,
            false);

    /** AVRO schema for double literals. */
    public static final Schema DOUBLE = Schema.create(Type.DOUBLE);

    /** AVRO schema for float literals. */
    public static final Schema FLOAT = Schema.create(Type.FLOAT);

    /** AVRO schema for big integer literals. */
    public static final Schema BIGINTEGER = Schema.createRecord("biginteger", null,
            AvroSchemas.NAMESPACE, false);

    /** AVRO schema for big decimal literals. */
    public static final Schema BIGDECIMAL = Schema.createRecord("bigdecimal", null,
            AvroSchemas.NAMESPACE, false);

    /** AVRO schema for non-compressed IDs (URIs, BNodes). */
    public static final Schema PLAIN_IDENTIFIER = Schema //
            .createRecord("plainidentifier", null, AvroSchemas.NAMESPACE, false);

    /** AVRO schema for compressed ID (URIs, BNodes). */
    public static final Schema COMPRESSED_IDENTIFIER = Schema //
            .createRecord("compressedidentifier", null, AvroSchemas.NAMESPACE, false);

    /** AVRO schema for any ID (URIs, BNodes). */
    public static final Schema IDENTIFIER = Schema.createUnion(ImmutableList.<Schema>of(
            PLAIN_IDENTIFIER, COMPRESSED_IDENTIFIER));

    /** AVRO schema for calendar literals. */
    public static final Schema CALENDAR = Schema.createRecord("calendar", null,
            AvroSchemas.NAMESPACE, false);

    /** AVRO schema for RDF statements. */
    public static final Schema STATEMENT = Schema.createRecord("statement", null,
            AvroSchemas.NAMESPACE, false);

    /** AVRO schema for record nodes ({@code Record}). */
    public static final Schema RECORD = Schema.createRecord("struct", null, AvroSchemas.NAMESPACE,
            false);

    /** AVRO schema for generic data model nodes. */
    public static final Schema NODE = Schema.createUnion(ImmutableList.<Schema>of(
            AvroSchemas.BOOLEAN, AvroSchemas.STRING, AvroSchemas.STRING_LANG, AvroSchemas.LONG,
            AvroSchemas.INT, AvroSchemas.SHORT, AvroSchemas.BYTE, AvroSchemas.DOUBLE,
            AvroSchemas.FLOAT, AvroSchemas.BIGINTEGER, AvroSchemas.BIGDECIMAL,
            AvroSchemas.PLAIN_IDENTIFIER, AvroSchemas.COMPRESSED_IDENTIFIER, AvroSchemas.CALENDAR,
            AvroSchemas.STATEMENT, AvroSchemas.RECORD));

    /** AVRO schema for lists of nodes. */
    public static final Schema LIST = Schema.createArray(AvroSchemas.NODE);

    /** AVRO schema for properties of a record node. */
    public static final Schema PROPERTY = Schema.createRecord("property", null,
            AvroSchemas.NAMESPACE, false);

    private AvroSchemas() {
    }

    static {
        AvroSchemas.STRING_LANG.setFields(ImmutableList.<Field>of(new Field("label",
                AvroSchemas.STRING, null, null), new Field("language", AvroSchemas.STRING, null,
                null)));
        AvroSchemas.SHORT.setFields(ImmutableList.<Field>of(new Field("short", AvroSchemas.INT,
                null, null)));
        AvroSchemas.BYTE.setFields(ImmutableList.<Field>of(new Field("byte", AvroSchemas.INT,
                null, null)));
        AvroSchemas.BIGINTEGER.setFields(ImmutableList.<Field>of(new Field("biginteger",
                AvroSchemas.STRING, null, null)));
        AvroSchemas.BIGDECIMAL.setFields(ImmutableList.<Field>of(new Field("bigdecimal",
                AvroSchemas.STRING, null, null)));
        AvroSchemas.PLAIN_IDENTIFIER.setFields(ImmutableList.<Field>of(new Field("identifier",
                AvroSchemas.STRING, null, null)));
        AvroSchemas.COMPRESSED_IDENTIFIER.setFields(ImmutableList.<Field>of(new Field(
                "identifier", AvroSchemas.INT, null, null)));
        AvroSchemas.CALENDAR.setFields(ImmutableList
                .<Field>of(new Field("timezone", AvroSchemas.INT, null, null), new Field(
                        "timestamp", AvroSchemas.LONG, null, null)));

        AvroSchemas.STATEMENT.setFields(ImmutableList.<Field>of(
                new Field("subject", AvroSchemas.IDENTIFIER, null, null),
                new Field("predicate", AvroSchemas.IDENTIFIER, null, null),
                new Field("object", Schema.createUnion(ImmutableList.<Schema>of(
                        AvroSchemas.BOOLEAN, AvroSchemas.STRING, AvroSchemas.STRING_LANG,
                        AvroSchemas.LONG, AvroSchemas.INT, AvroSchemas.SHORT, AvroSchemas.BYTE,
                        AvroSchemas.DOUBLE, AvroSchemas.FLOAT, AvroSchemas.BIGINTEGER,
                        AvroSchemas.BIGDECIMAL, AvroSchemas.CALENDAR,
                        AvroSchemas.PLAIN_IDENTIFIER, AvroSchemas.COMPRESSED_IDENTIFIER)), null,
                        null), //
                new Field("context", AvroSchemas.IDENTIFIER, null, null)));

        AvroSchemas.PROPERTY
                .setFields(ImmutableList.<Field>of(
                        new Field("propertyURI", AvroSchemas.COMPRESSED_IDENTIFIER, null, null),
                        new Field("propertyValue", Schema.createUnion(ImmutableList.<Schema>of(
                                AvroSchemas.BOOLEAN, AvroSchemas.STRING, AvroSchemas.STRING_LANG,
                                AvroSchemas.LONG, AvroSchemas.INT, AvroSchemas.SHORT,
                                AvroSchemas.BYTE, AvroSchemas.DOUBLE, AvroSchemas.FLOAT,
                                AvroSchemas.BIGINTEGER, AvroSchemas.BIGDECIMAL,
                                AvroSchemas.CALENDAR, AvroSchemas.PLAIN_IDENTIFIER,
                                AvroSchemas.COMPRESSED_IDENTIFIER, AvroSchemas.STATEMENT,
                                AvroSchemas.RECORD, AvroSchemas.LIST)), null, null)));

        AvroSchemas.RECORD.setFields(ImmutableList.<Field>of(
                new Field("id", Schema.createUnion(ImmutableList.<Schema>of(AvroSchemas.NULL,
                        AvroSchemas.PLAIN_IDENTIFIER, AvroSchemas.COMPRESSED_IDENTIFIER)), null,
                        null), //
                new Field("properties", Schema.createArray(AvroSchemas.PROPERTY), null, null)));
    }

}
