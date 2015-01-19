package eu.fbk.knowledgestore.datastore.hbase.utils;

import java.io.ByteArrayOutputStream;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import eu.fbk.knowledgestore.data.Record;

/**
 * Class used for testing Avro schemas
 */
public class AvroSchemasTest {

    @Test
    public void test() throws Throwable {
        System.out.println(AvroSchemas.RECORD.toString());

        final Record resource = HBaseTestUtils.getMockResource();
        //final Object generic = AvroSerialization.toGenericData(resource);
        final Object generic = null;

        final ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream binaryStream = new ByteArrayOutputStream();
        final Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(AvroSchemas.RECORD,
                jsonStream);
        final Encoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(binaryStream, null);
        final DatumWriter<Object> writer = new GenericDatumWriter<Object>(AvroSchemas.RECORD);
        writer.write(generic, jsonEncoder);
        writer.write(generic, binaryEncoder);
        binaryEncoder.flush();
        jsonEncoder.flush();

        final byte[] bytes = binaryStream.toByteArray();
        final String json = new String(jsonStream.toByteArray(), Charsets.UTF_8);
        System.out.println(bytes.length + " bytes: " + BaseEncoding.base16().encode(bytes));
        System.out.println("JSON:\n" + json);
    }

}
