package eu.fbk.knowledgestore;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

public class XPathTest {

    @Ignore
    @Test
    public void test2() {

        final Record record = Record.create(new URIImpl("ex:id"));
        record.set(new URIImpl(KS.NAMESPACE + "pippo"), "pioppo");
        System.out.println(record.toString(Data.getNamespaceMap(), true));

        final XPath xpath = XPath.parse("ks:pippo = 'pioppo'");
        System.out.println(xpath.evalBoolean(record));
    }

    @Test
    public void test() {

        final XPath xpath2 = XPath.parse("dct:created >= dateTime('2012-01-01') " //
                + "and dct:created < dateTime('2012-01-31') " //
                + "and dct:source = \\ks:test ");
        final Map<URI, Set<Object>> map2 = new HashMap<>();
        xpath2.decompose(map2);
        System.out.println(map2);

        final XPath xpath0 = XPath.parse("rdfs:comment = 'comment # 0'");
        System.out.println(xpath0);

        final Record resource = getMockResource();
        // final XPath xpath = XPath.create("with dct: <" + DCTERMS.NAMESPACE + "> : "
        // + "./nie:isStoredAs[/dct:creator = 'John']/dct:issued >= 1007 "
        // + "and dct:title = 'This is the news title'" + " and rdf:type = \\ks:Resource",
        // ImmutableBiMap.of("nie", NIE.NAMESPACE, "rdf", RDF.NAMESPACE));
        System.out.println(XPath.constant(5, 3, new URIImpl("test:a"), "bla"));
        System.out.println(XPath.parse("uri(sequence('test:a', 'test:b', 13))").eval(resource));
        System.out.println(XPath.compose("and", XPath.constant(false),
                XPath.parse(ImmutableMap.of("dct", "sesame:nil:"), "dct:title = 'test'")));
        XPath xpath = XPath.parse(ImmutableMap.of("ks", "sesame:nil:"),
                "\\ks:Resource = uri(sequence('sesame:nil:Resource', 'fd'))");
        System.out.println(xpath);
        // System.out.println(xpath.eval(resource));
        System.out.println(xpath.getHead());
        System.out.println(xpath.getBody());
        System.out.println(xpath.getNamespaces());
        System.out.println(xpath.getProperties());

        xpath = XPath.parse(ImmutableBiMap.of("nie", NIE.NAMESPACE, "rdf", RDF.NAMESPACE),
                "with dct: <" + DCTERMS.NAMESPACE + "> : /nie:isStoredAs >= 1008"
                        + "and ./nie:isStoredAs[/dct:creator = 'John']/dct:issued >= 1007 "
                        + "and dct:title = 'This is the news title'"
                        + " and rdf:type = \\ks:Resource");
        final Map<URI, Set<Object>> map = Maps.newHashMap();
        final XPath remaining = xpath.decompose(map);
        System.out.println(xpath.evalBoolean(resource));
        System.out.println("*** remaining: " + remaining);
        System.out.println("*** map: " + map);
        // final long ts = System.nanoTime();
        // for (int i = 0; i < 100000; ++i) {
        // xpath.evalBoolean(resource);
        // }
        // System.out.println((System.nanoTime() - ts) / 100000);
        //
        // System.out.println(condition);
        // System.out.println(condition.apply(resource));
    }

    public static Record getMockResource() {
        final GregorianCalendar calendar = new GregorianCalendar();
        calendar.set(2013, 9, 23);

        final Record rep = Record.create();
        rep.setID(new URIImpl("ks:r15_rep"));
        rep.set(RDF.TYPE, KS.REPRESENTATION);
        rep.set(NFO.FILE_NAME, "r15.txt");
        rep.set(NFO.FILE_SIZE, 1533L);
        rep.set(NFO.FILE_CREATED, new Date());
        rep.set(NIE.MIME_TYPE, "text/plain");
        rep.set(DCTERMS.CREATOR, "John", "Steve", "Mark");
        rep.set(DCTERMS.ISSUED, 1000, 1005, 1007);

        final Record resource = Record.create();
        resource.setID(new URIImpl("ks:r15"));
        resource.set(RDF.TYPE, KS.RESOURCE);
        resource.set(DCTERMS.TITLE, "This is the news title");
        resource.set(DCTERMS.ISSUED, calendar);
        resource.set(NIE.IS_STORED_AS, rep);
        return resource;
    }

}
