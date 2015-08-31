package eu.fbk.knowledgestore.data;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

import com.google.common.io.BaseEncoding;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

public class SerializerTest {

    @Test
    public void testLanguage() throws Throwable {
        final Serializer serializer = new Serializer();
        final Literal l1 = ValueFactoryImpl.getInstance().createLiteral("hello", "en");
        final byte[] bytes = serializer.toBytes(l1);
        final Literal l2 = (Literal) serializer.fromBytes(bytes);
        Assert.assertEquals(l1, l2);
    }

    // JAVA SERIALIZATION
    // 2432

    // OLD SERIALIZER
    // dict: 104

    // NEW SERIALIZER
    // baseline: 209
    // comp: 173
    // dict: 89
    // comp + dict: 85

    @Test
    public void test() throws Throwable {

        final Dictionary<URI> dictionary = Dictionary.createLocalDictionary(URI.class, new File(
                System.getProperty("java.io.tmpdir") + "/uris.dic"));
        // final Serializer serializer = new Serializer(dictionary);
        final Serializer serializer = new Serializer(true, dictionary, null);

        final Record resource = getMockResource();
        final byte[] bytes = serializer.toBytes(resource);
        System.out.println(serializeJava(resource).length + " bytes java");
        System.out.println(bytes.length + " bytes: " + BaseEncoding.base16().encode(bytes));
        final Record resource2 = (Record) serializer.fromBytes(bytes);
        System.out.println(resource2.toString(Data.getNamespaceMap(), true));
        Assert.assertEquals(resource, resource2);
        final byte[] bytes2 = serializer.toBytes(resource2);
        Assert.assertTrue(Arrays.equals(bytes, bytes2));
    }

    private static Record getMockResource() {
        final GregorianCalendar calendar = new GregorianCalendar();
        calendar.set(2013, 9, 23);

        final Record rep = Record.create();
        rep.setID(new URIImpl("ks:r15_rep"));
        rep.set(RDF.TYPE, KS.REPRESENTATION);
        rep.set(NFO.FILE_NAME, "r15.txt");
        rep.set(NFO.FILE_SIZE, 1533L);
        rep.set(NFO.FILE_CREATED, new Date());
        rep.set(NIE.MIME_TYPE, "text/plain");

        final Record resource = Record.create();
        resource.setID(new URIImpl("ks:r15"));
        resource.set(RDF.TYPE, KS.RESOURCE);
        resource.set(DCTERMS.TITLE, "This is the news title");
        resource.set(DCTERMS.ISSUED, calendar);
        resource.set(NIE.IS_STORED_AS, rep);
        return resource;
    }

    private static byte[] serializeJava(final Object object) throws Throwable {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(object);
        return bos.toByteArray();
    }

}
