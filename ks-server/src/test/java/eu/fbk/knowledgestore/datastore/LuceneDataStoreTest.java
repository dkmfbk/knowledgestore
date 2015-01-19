package eu.fbk.knowledgestore.datastore;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Created with IntelliJ IDEA.
 * User: alessio
 * Date: 09/09/14
 * Time: 11:24
 * To change this template use File | Settings | File Templates.
 */

public class LuceneDataStoreTest extends AbstractDataStoreTest {
	@Override
	protected DataStore createDataStore() {
		LuceneDataStore s = new LuceneDataStore("/Users/alessio/lucene-ks", null);
		return s;
	}

	@Test
	public void testRetrieve() {
		DataStore ds = this.createDataStore();
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
