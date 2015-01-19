package eu.fbk.knowledgestore.datastore.hbase;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_ZOOKEEPER_CLIENT_PORT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_ZOOKEEPER_QUORUM;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.URIDICT_RELATIVEPATH_PROP;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.AbstractDataStoreTest;
import eu.fbk.knowledgestore.datastore.DataStore;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.runtime.Files;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Class for testing the HBase data store
 */
public class HBaseDataStoreTest extends AbstractDataStoreTest{

    @Override
    protected DataStore createDataStore() {
        final FileSystem fileSystem = Files.getRawLocalFileSystem();
        final Properties properties = new Properties();
        properties.setProperty(URIDICT_RELATIVEPATH_PROP, "uri" + System.nanoTime() + ".dic");
        properties.setProperty(HBASE_ZOOKEEPER_QUORUM, "192.168.0.8");
        properties.setProperty(HBASE_ZOOKEEPER_CLIENT_PORT, "2181");
        return new HBaseDataStore(fileSystem, properties);
    }

    @Test
    public void testRetrieve() {
        HBaseDataStore ds = (HBaseDataStore) new HBaseDataStoreTest().createDataStore();
        try {
            ds.init();
            List<Record> records = createRecords(3, KS.RESOURCE);
            DataTransaction dataTran = ds.begin(false);
            dataTran.store(KS.RESOURCE, records.get(0));
            dataTran.store(KS.RESOURCE,  records.get(1));
            dataTran.delete(KS.RESOURCE, records.get(2).getID());
            XPath condition = XPath.parse("'example.org'"); // TODO: I don't thing this will work
            //condition.decompose(propertyRanges);
            Stream<Record> cur = dataTran.retrieve(KS.RESOURCE, condition, null);
            try {
                for (Record r : cur) {
                    System.out.println(r);
                }
            } finally {
                cur.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CancellationException ex) {
            // not our case: ignore
        }
    }

}
