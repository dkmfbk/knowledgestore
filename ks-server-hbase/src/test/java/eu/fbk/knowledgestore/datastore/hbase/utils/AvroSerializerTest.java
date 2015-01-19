package eu.fbk.knowledgestore.datastore.hbase.utils;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import com.google.common.io.BaseEncoding;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.runtime.Dictionary;

public class AvroSerializerTest {

    @Test
    public void testLanguage() throws Throwable {

        final AvroSerializer serializer = new AvroSerializer();

        Literal l1 = ValueFactoryImpl.getInstance().createLiteral("hello", "en");

        byte[] bytes = serializer.toBytes(l1);

        Literal l2 = (Literal) serializer.fromBytes(bytes);

        Assert.assertEquals(l1, l2);
    }

    @Test
    public void test() throws Throwable {

        final Dictionary<URI> dictionary = new Dictionary<URI>(URI.class, new Path(System.getProperty("java.io.tmpdir") + "/uris.dic").toString());

        final AvroSerializer serializer = new AvroSerializer(dictionary);

        final Record resource = HBaseTestUtils.getMockResource();

        final byte[] bytes = serializer.toBytes(resource);

        System.out.println(bytes.length + " bytes: " + BaseEncoding.base16().encode(bytes));

        final Record resource2 = (Record) serializer.fromBytes(bytes);

        final byte[] bytes2 = serializer.toBytes(resource2);

        Assert.assertEquals(resource, resource2);
        Assert.assertTrue(Arrays.equals(bytes, bytes2));

        System.out.println(resource2.toString(Data.getNamespaceMap(), true));

        final URI uri = new URIImpl(
                "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileSize");
        final byte[] uriBytes = serializer.compressURI(uri);
        System.out.println("URI bytes: " + BaseEncoding.base16().encode(uriBytes));

        final URI uri2 = serializer.expandURI(uriBytes);

        Assert.assertEquals(uri, uri2);

        final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DeflaterOutputStream dos = new DeflaterOutputStream(bos, deflater);
        dos.write(bytes);
        dos.close();
        System.out.println(bos.toByteArray().length + " compressed bytes: "
                + BaseEncoding.base16().encode(bos.toByteArray()));
    }

}
