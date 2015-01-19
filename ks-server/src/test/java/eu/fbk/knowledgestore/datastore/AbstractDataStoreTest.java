package eu.fbk.knowledgestore.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Abstract class for defining data store tests.
 */
public abstract class AbstractDataStoreTest {

    /** Data store to be used. */
    private DataStore dataStore;

    protected abstract DataStore createDataStore();

    /**
     * @return the dataStore
     */
    protected final DataStore getDataStore() {
        return this.dataStore;
    }

    @Before
    public void setUp() throws IOException {
        this.dataStore = createDataStore();
        this.dataStore.init();
    }

    @After
    public void tearDown() throws IOException {
        this.dataStore.close();
    }

    @Test
    public void testDataModifyResources() throws Throwable {
        try {
            final List<Record> records = createRecords(3, KS.RESOURCE);
            final DataTransaction dataTran = this.dataStore.begin(false);
            dataTran.store(KS.RESOURCE, records.get(0));
            dataTran.store(KS.RESOURCE, records.get(1));
            dataTran.delete(KS.RESOURCE, records.get(2).getID());
            dataTran.end(true);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDataLookup() throws Throwable {
        Stream<Record> stream = null;
        final DataTransaction dt = this.dataStore.begin(true);
        try {
            stream = dt.lookup(KS.RESOURCE, new HashSet<URI>(createURIs(3)), null);
            Assert.assertNotNull(stream);
            Assert.assertTrue(stream.iterator().hasNext());
        } finally {
            if (stream != null) {
                stream.close();
            }
            dt.end(true);
        }
    }

    /**
     * Creates a list of URIs.
     * 
     * @param number
     *            of URIs to be created
     * @return list of URIs created
     */
    protected final List<URI> createURIs(final int number) {
        final List<URI> uris = new ArrayList<URI>();
        for (int cont = 0; cont < number; cont++) {
            final URI idTmp = new URIImpl("http://example.org/" + cont);
            uris.add(idTmp);
        }
        return uris;
    }

    /**
     * Method that creates a list of URI as identifies.
     * 
     * @param number
     *            of URIs to be created
     * @param type
     *            the type of record
     * @return list of URIs created
     */
    protected final List<Record> createRecords(final int number, final URI type) {
        final List<Record> records = new ArrayList<Record>();
        final List<URI> uris = createURIs(number);
        for (int cont = 0; cont < number; cont++) {
            final Record rTmp = Record.create();
            rTmp.set(RDF.TYPE, type);
            rTmp.setID(uris.get(cont));
            records.add(rTmp);
        }
        return records;
    }

}
