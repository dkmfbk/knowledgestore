package eu.fbk.knowledgestore.datastore;

import com.google.common.collect.Iterables;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.runtime.*;
import eu.fbk.knowledgestore.vocabulary.KS;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.FSDirectory;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
*
* TODO
* - Optimize Lucene in background
* - Sometimes the UI says that the IndexReader is closed
* - Manage read only mode
* - Introduce ReentrantReadWriteLock
*   http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
*   http://stackoverflow.com/questions/18354339/reentrantreadwritelock-whats-the-difference-between-readlock-and-writelock
*
* */

/**
 * Created with IntelliJ IDEA.
 * User: alessio
 * Date: 08/09/14
 * Time: 17:57
 * To change this template use File | Settings | File Templates.
 */

public class LuceneDataStore implements DataStore {

	private String mentionsFolder;
	private String resourcesFolder;
	private HashMap<URI, IndexReader> readers = new HashMap<>();
	private HashMap<URI, IndexWriter> writers = new HashMap<>();
	private HashMap<URI, AtomicInteger> writingOperations = new HashMap<>();

	private Serializer serializer;

	private final int MAX_LUCENE_SEGMENTS = 100;

	private static final HashMap<URI, Integer> OPTIMIZATION_THRESHOLD = new HashMap<>();

	static {
		OPTIMIZATION_THRESHOLD.put(KS.RESOURCE, 1000);
		OPTIMIZATION_THRESHOLD.put(KS.MENTION, 10000);
	}

	static Logger logger = LoggerFactory.getLogger(LuceneDataStore.class);

	public LuceneDataStore(String folder, @Nullable Serializer serializer) {
		this.mentionsFolder = folder + File.separator + "mentions";
		this.resourcesFolder = folder + File.separator + "resources";
		this.serializer = serializer;
	}

	public static byte[] serializeRecord(Record record, @Nullable Serializer serializer) throws IOException {
		if (serializer == null) {
			ObjectOutput out = null;
			byte[] returnBytes;

			try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
				out = new ObjectOutputStream(bos);
				out.writeObject(record);
				returnBytes = bos.toByteArray();
			} finally {
				if (out != null) {
					out.close();
				}
			}

			return returnBytes;
		}
		else {
			return serializer.toBytes(record);
		}
	}

	public static Record unserializeRecord(byte[] bytes, @Nullable Serializer serializer) throws IOException {
		if (serializer == null) {
			ObjectInput in = null;
			Record returnRecord;

			try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
				in = new ObjectInputStream(bis);
				try {
					returnRecord = (Record) in.readObject();
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			return returnRecord;
		}
		else {
			return (Record) serializer.fromBytes(bytes);
		}
	}

	public class LuceneTransaction implements DataTransaction {

		boolean readOnly;

		public static final String KEY_NAME = "key";
		public static final String VALUE_NAME = "value";

		public LuceneTransaction(boolean readOnly) throws IOException {
			this.readOnly = readOnly;

			// todo
//			readOnly = false;
		}

		private void optimize(URI type) throws IOException {
			if (!readers.get(type).isOptimized()) {

				synchronized (writingOperations.get(type)) {
					if (writingOperations.get(type).intValue() > OPTIMIZATION_THRESHOLD.get(type)) {
						logger.info("Optimizing index {}", type.toString());
						writers.get(type).optimize(MAX_LUCENE_SEGMENTS);
						writingOperations.get(type).set(0);
					}
				}

			}

			readers.get(type).close();
			readers.put(type, writers.get(type).getReader());
		}

		@Override
		public Stream<Record> lookup(URI type, Set<? extends URI> ids, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {

			optimize(type);

			List<Record> returns = new ArrayList<>();

			for (URI id : ids) {
				String uri;
				try {
					uri = id.toString();
				} catch (NullPointerException e) {
					throw new IOException(e);
				}

				logger.debug("Selecting {}", uri);

				Term s = new Term(KEY_NAME, uri);
				TermDocs termDocs = readers.get(type).termDocs(s);

				if (termDocs.next()) {
					Document doc = readers.get(type).document(termDocs.doc());
					Record r = unserializeRecord(doc.getBinaryValue(VALUE_NAME), serializer);
					if (properties != null && !properties.isEmpty()) {
						r.retain(Iterables.toArray(properties, URI.class));
					}
					returns.add(r);
				}
			}

			return Stream.create(returns);
		}

		@Override
		public Stream<Record> retrieve(URI type, @Nullable XPath condition, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {

			optimize(type);

			List<Record> returns = new ArrayList<>();

			for (int i = 0; i < readers.get(type).numDocs(); i++) {

				Document doc = readers.get(type).document(i);
				Record r = unserializeRecord(doc.getBinaryValue(VALUE_NAME), serializer);
				if (condition != null && !condition.evalBoolean(r)) {
					continue;
				}

				if (properties != null) {
					r.retain(Iterables.toArray(properties, URI.class));
				}
				returns.add(r);
			}

			return Stream.create(returns);
		}

		@Override
		public long count(URI type, @Nullable XPath condition) throws IOException, IllegalArgumentException, IllegalStateException {
			optimize(type);
			if (condition == null) {
				return readers.get(type).numDocs();
			}
			else {
				Stream<Record> stream = retrieve(type, condition, null);
				return stream.count();
			}
		}

		@Override
		public Stream<Record> match(Map<URI, XPath> conditions, Map<URI, Set<URI>> ids, Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
			return null;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public void store(URI type, Record record) throws IOException, IllegalStateException {
			writingOperations.get(type).incrementAndGet();
			IndexWriter indexWriter = writers.get(type);

			String uri;
			try {
				uri = record.getID().toString();
			} catch (NullPointerException e) {
				throw new IOException(e);
			}

			logger.debug(String.format("Inserting %s", uri));
			Document doc = new Document();
			doc.add(new Field(KEY_NAME, uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.add(new Field(VALUE_NAME, serializeRecord(record, serializer), Field.Store.YES));

			// use "update" instead of "add" to avoid duplicates
			indexWriter.updateDocument(new Term(KEY_NAME, uri), doc);
		}

		@Override
		public void delete(URI type, URI id) throws IOException, IllegalStateException {
			writingOperations.get(type).incrementAndGet();
			Term s = new Term(KEY_NAME, id.toString());
			writers.get(type).deleteDocuments(s);
			writers.get(type).commit();
			optimize(type);
		}

		@Override
		public void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException {
			// Nothing to do here
		}
	}

	@Override
	public DataTransaction begin(boolean readOnly) throws DataCorruptedException, IOException, IllegalStateException {

		LuceneTransaction ret = new LuceneTransaction(readOnly);

		return ret;
	}

	@Override
	public void init() throws IOException, IllegalStateException {
		Files.createDirectories(Paths.get(mentionsFolder));
		Files.createDirectories(Paths.get(resourcesFolder));

		writingOperations.put(KS.RESOURCE, new AtomicInteger(0));
		writingOperations.put(KS.MENTION, new AtomicInteger(0));

		try {
			writers.put(KS.RESOURCE, new IndexWriter(FSDirectory.open(new File(resourcesFolder)), new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED));
			writers.put(KS.MENTION, new IndexWriter(FSDirectory.open(new File(mentionsFolder)), new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED));

//			writers.get(KS.RESOURCE).setUseCompoundFile(true);
//			writers.get(KS.MENTION).setUseCompoundFile(true);

			writers.get(KS.RESOURCE).optimize(MAX_LUCENE_SEGMENTS);
			writers.get(KS.MENTION).optimize(MAX_LUCENE_SEGMENTS);

			readers.put(KS.RESOURCE, writers.get(KS.RESOURCE).getReader());
			readers.put(KS.MENTION, writers.get(KS.MENTION).getReader());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public void close() {
		try {
			readers.get(KS.RESOURCE).close();
			readers.get(KS.MENTION).close();

			writers.get(KS.RESOURCE).close();
			writers.get(KS.MENTION).close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	private void resetOperationCount(URI type) {
		writingOperations.get(type).set(0);
	}
}
