package eu.fbk.knowledgestore.datastore.utils;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.datastore.LuceneDataStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by alessio on 07/11/14.
 */

public class LuceneIndexReader {

	static Logger logger = LoggerFactory.getLogger(LuceneIndexReader.class);

	public static void main(String[] args) {
		String indexPath = args[0];
		String url = args[1];

		try {
			IndexReader reader = IndexReader.open(FSDirectory.open(new File(indexPath)), false);

			Term s = new Term(LuceneDataStore.LuceneTransaction.KEY_NAME, url);
			TermDocs termDocs = reader.termDocs(s);

			if (termDocs.next()) {
				Document document = reader.document(termDocs.doc());
//				Record r = LuceneDataStore.unserializeRecord(document.getBinaryValue(LuceneDataStore.LuceneTransaction.VALUE_NAME));
//				System.out.println(r.toString(null, true));
			}
			else {
				logger.info("URI {} not found", url);
			}

			reader.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}
