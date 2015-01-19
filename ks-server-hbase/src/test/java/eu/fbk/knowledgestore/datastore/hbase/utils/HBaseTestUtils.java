package eu.fbk.knowledgestore.datastore.hbase.utils;

import java.util.Date;
import java.util.GregorianCalendar;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

/**
 * Class used to perform different actions needed for testing.
 */
public class HBaseTestUtils {

    /**
     * Creates a mock resource.
     * 
     * @return Resource created with fake data.
     */
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

        final Record resource = Record.create();
        resource.setID(new URIImpl("ks:r15"));
        resource.set(RDF.TYPE, KS.RESOURCE);
        resource.set(DCTERMS.TITLE, "This is the news title");
        resource.set(DCTERMS.ISSUED, calendar);
        resource.set(NIE.IS_STORED_AS, rep);
        return resource;
    }
}
