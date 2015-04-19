package eu.fbk.knowledgestore.filestore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Stream;

/*
*
* TODO
* - Pay attention to the list, when a file is deleted
*   (the LuceneIterator will stop when it'll reach a hole)
* - The zip seek is not optimized because the Hadoop API
*   can only read file from start to end
* - Optimize Lucene in background
* - Introduce ReentrantReadWriteLock to avoid IndexReader
*   closed error
* - Add parameter for number of files
*
* */

/**
 * A {@code FileStore} implementation based on the Hadoop API optimized for huge number of files.
 * <p>
 * An {@code HadoopFileStore} stores its files in an Hadoop {@link org.apache.hadoop.fs.FileSystem}, under a certain,
 * configurable root path; the filesystem can be any of the filesystems supported by the Hadoop
 * API, including the local (raw) filesystem and the distributed HDFS filesystem.
 * </p>
 * <p>
 * Files are stored in a a two-level directory structure, where first level directories reflect
 * the MIME types of stored files, and second level directories are buckets of files whose name is
 * obtained by hashing the filename; buckets are used in order to equally split a large number of
 * files in several subdirectories, overcoming possible filesystem limitations in terms of maximum
 * number of files storable in a directory.
 * </p>
 */
public class HadoopMultiFileStore implements FileStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(HadoopMultiFileStore.class);

	private static final String DEFAULT_PATH = "files";
	private static final String DEFAULT_LUCENE_PATH = "./lucene-index";
	private static final String SMALL_FILES_PATH = "_small";

	private final String FILENAME_FIELD_NAME = "filename";
	private final String ZIP_FIELD_NAME = "zipfilename";

	private final FileSystem fileSystem;
	private final int MAX_LUCENE_SEGMENTS = 100;

	private final Path rootPath;
	private final Path smallFilesPath;
	private final File luceneFolder;

	private int MAX_NUM_SMALL_FILES = 10;
	private Set<String> filesInWritingMode = Collections.synchronizedSet(new HashSet<String>());
	final private AtomicBoolean isWritingBigFile = new AtomicBoolean(false);

	private IndexReader luceneReader;
	private IndexWriter luceneWriter;

	/**
	 * Creates a new {@code HadoopFileStore} storing files in the {@code FileSystem} and under the
	 * {@code rootPath} specified.
	 *
	 * @param fileSystem the file system, not null
	 * @param path       the root path where to store files, possibly relative to the filesystem working
	 *                   directory; if null, the default root path {@code files} will be used
	 */
	public HadoopMultiFileStore(final FileSystem fileSystem, @Nullable final String lucenePath, @Nullable final String path, @Nullable Integer numSmallFile) {
		if (numSmallFile != null) {
			MAX_NUM_SMALL_FILES = numSmallFile;
		}
		this.fileSystem = Preconditions.checkNotNull(fileSystem);
		this.rootPath = new Path(MoreObjects.firstNonNull(path, DEFAULT_PATH)).makeQualified(this.fileSystem); // resolve wrt workdir
		this.luceneFolder = new File(MoreObjects.firstNonNull(lucenePath, DEFAULT_LUCENE_PATH));
		this.smallFilesPath = new Path(this.rootPath.toString() + File.separator + SMALL_FILES_PATH).makeQualified(this.fileSystem);
		LOGGER.info("{} configured, paths={};{}", getClass().getSimpleName(), this.rootPath, this.luceneFolder);
	}

	public InputStream readGenericFile(Path path) throws IOException {
		try {
//			fileSystem.setVerifyChecksum(false);
			final InputStream stream = this.fileSystem.open(path);
			LOGGER.debug("Reading file {}", path.getName());
			return stream;
		} catch (final IOException ex) {
			if (!this.fileSystem.exists(path)) {
				throw new FileMissingException(path.getName(), "Cannot read non-existing file");
			}
			throw ex;
		}

	}

	@Override
	public void init() throws IOException {
		if (!this.fileSystem.exists(this.rootPath)) {
			LOGGER.debug("Created root folder");
			this.fileSystem.mkdirs(this.rootPath);
		}
		if (!this.fileSystem.exists(this.smallFilesPath)) {
			LOGGER.debug("Created small files folder");
			this.fileSystem.mkdirs(this.smallFilesPath);
		}
		if (!this.luceneFolder.exists()) {
			LOGGER.debug("Created lucene folder");
			if (!this.luceneFolder.mkdirs()) {
				throw new IOException(String.format("Unable to create dir %s", this.luceneFolder.toString()));
			}
		}

		luceneWriter = new IndexWriter(FSDirectory.open(this.luceneFolder), new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
		luceneReader = luceneWriter.getReader();
//		luceneReader = IndexReader.open(FSDirectory.open(this.luceneFolder), true);
	}

	private void deleteFromBigFile(final String fileName, final Path zipFile) throws IOException {

		LOGGER.debug("Deleting zip file {}", zipFile.getName());
		ZipInputStream zipInputStream = new ZipInputStream(readGenericFile(zipFile));
		ZipEntry entry;
		byte[] buffer = new byte[2048];
		HashSet<String> listOfFiles = new HashSet<>();

		while ((entry = zipInputStream.getNextEntry()) != null) {
			String entryName = entry.getName();

			if (entryName.equals(fileName)) {
				continue;
			}

			final Path path = getSmallPath(entryName);
			filesInWritingMode.add(entryName);
			listOfFiles.add(entryName);

			OutputStream stream = this.fileSystem.create(path, false);
			stream = new InterceptCloseOutputStream(stream, entryName);
			int len;
			while ((len = zipInputStream.read(buffer)) > 0) {
				stream.write(buffer, 0, len);
			}
			stream.close();
		}

		zipInputStream.close();
		this.fileSystem.delete(zipFile, false);

		Term s = new Term(ZIP_FIELD_NAME, zipFile.getName());
		luceneWriter.deleteDocuments(s);
		luceneWriter.commit();
		luceneReader.close();
		luceneReader = luceneWriter.getReader();

		filesInWritingMode.removeAll(listOfFiles);

		LOGGER.debug("Finishing deletion of zip file {}", zipFile.getName());

		checkSmallFilesAndMerge();
	}

	private InputStream loadFromBigFile(final String fileName, final Path zipFile) throws IOException {
		ZipInputStream stream = new ZipInputStream(readGenericFile(zipFile));
		ZipEntry entry;
		while ((entry = stream.getNextEntry()) != null) {
			String entryName = entry.getName();
			if (entryName.equals(fileName)) {
				return stream;
			}
		}

		throw new FileMissingException(fileName, "The file does not exist");
	}

	@Override
	public InputStream read(final String fileName) throws IOException {

		optimizeOnDemand();

		// Search for small file
		final Path path = getSmallPath(fileName);
		if (this.fileSystem.exists(path)) {
			LOGGER.debug("It is a small file");
			return readGenericFile(path);
		}

		// Seek it in the gzipped files
		else {
			Term s = new Term(FILENAME_FIELD_NAME, fileName);
			TermDocs termDocs = luceneReader.termDocs(s);

			if (termDocs.next()) {
				Document doc = luceneReader.document(termDocs.doc());
				String zipFile = doc.get(ZIP_FIELD_NAME);
				Path inputFile = getFolderFromBigFile(zipFile);

				LOGGER.debug("The zip file is {}", inputFile.getName());
				return loadFromBigFile(fileName, inputFile);
			}

			throw new FileMissingException(fileName, "The file does not exist");
		}
	}

	@Override
	public OutputStream write(final String fileName) throws IOException {
//		return new IOUtils.NullOutputStream();

		final Path path = getSmallPath(fileName);

		// Check existence in small folder
		if (this.fileSystem.exists(path)) {
			throw new FileExistsException(path.getName(), "Cannot overwrite file");
		}

		// Check existence in Lucene index
		Term s = new Term(FILENAME_FIELD_NAME, fileName);
		TermDocs termDocs = luceneReader.termDocs(s);
		if (termDocs.next()) {
			throw new FileExistsException(path.getName(), "Cannot overwrite file");
		}

		checkSmallFilesAndMerge();

		// Write small file
		LOGGER.debug("Creating file {}", path.getName());
		final OutputStream stream = this.fileSystem.create(path, false);
		filesInWritingMode.add(fileName);
		return new InterceptCloseOutputStream(stream, fileName);
	}

	@Override
	public void delete(final String fileName) throws FileMissingException, IOException {
		final Path path = getSmallPath(fileName);
		if (this.fileSystem.exists(path)) {
			// It is a small file
			this.fileSystem.delete(path, false);
			LOGGER.debug("Deleted file {}", path.getName());
		}
		else {
			Term s = new Term(FILENAME_FIELD_NAME, fileName);
			TermDocs termDocs = luceneReader.termDocs(s);

			if (termDocs.next()) {
				Document doc = luceneReader.document(termDocs.doc());
				String zipFile = doc.get(ZIP_FIELD_NAME);
				Path inputFile = getFolderFromBigFile(zipFile);

				LOGGER.debug("The zip file is {}", inputFile.getName());
				deleteFromBigFile(fileName, inputFile);
				return;
			}

			throw new FileMissingException(fileName, "The file does not exist");
		}
	}

	private synchronized void optimizeOnDemand() throws IOException {
		if (!luceneReader.isOptimized()) {
			LOGGER.debug("Optimizing index");
			luceneWriter.optimize(MAX_LUCENE_SEGMENTS);
			luceneReader.close();
			luceneReader = luceneWriter.getReader();
		}
		else {
			LOGGER.debug("Index is optimized");
		}
	}

	@Override
	public Stream<String> list() throws IOException {
//		return Stream.create(new LuceneIterator(luceneReader));
		optimizeOnDemand();
		return Stream.concat(Stream.create(new LuceneIterator(luceneReader)), Stream.create(new HadoopIterator(this.smallFilesPath, this.fileSystem)));
	}

	@Override
	public void close() {
		// Nothing to do here. FileSystems are cached and closed by Hadoop at shutdown.

		while (isWritingBigFile.get()) {
			try {
				Thread.sleep(100);
			} catch (Exception e) {
				// ignored
			}
		}

		try {
			luceneReader.close();
		} catch (Exception e) {
			LOGGER.warn("Unable to close Lucene reader");
		}

		try {
			luceneWriter.optimize();
		} catch (Exception e) {
			LOGGER.warn("Unable to optimize Lucene writer");
		}

		try {
			luceneWriter.close();
		} catch (Exception e) {
			LOGGER.warn("Unable to close Lucene writer");
		}

	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	private Path getFolderFromBigFile(String fileName) {
		final String bucketDirectory = fileName.substring(0, 2);
		return new Path(this.rootPath, bucketDirectory + File.separator + fileName);
	}

	private void checkSmallFilesAndMerge() throws IOException {
		boolean mustMerge = false;
		FileStatus[] files;
		LOGGER.debug("Checking small files");

		synchronized (isWritingBigFile) {
			if (!isWritingBigFile.get()) {
				files = fileSystem.listStatus(smallFilesPath);
				if (files == null) {
					return;
				}
				LOGGER.debug("Number of files: {}", files.length);
				if (files.length > MAX_NUM_SMALL_FILES) {
					mustMerge = true;
					isWritingBigFile.set(true);
				}
			}
			else {
				return;
			}
		}

		if (mustMerge) {
			LOGGER.debug("More than {} small files, building zip", MAX_NUM_SMALL_FILES);
			LOGGER.debug(filesInWritingMode.toString());

			FileStatus[] list = new FileStatus[MAX_NUM_SMALL_FILES];

			StringBuffer stringBuffer = new StringBuffer();

			try {
				int i = 0;
				for (FileStatus fs : files) {
					if (fs.isDir()) {
						continue;
					}
					if (filesInWritingMode.contains(fs.getPath().getName())) {
						continue;
					}
					if (++i > MAX_NUM_SMALL_FILES) {
						break;
					}

					LOGGER.debug("{} - {}", i - 1, fs.getPath().getName());
					stringBuffer.append(fs.toString());
					list[i - 1] = fs;
				}

				if (list[list.length - 1] == null) {
					throw new IOException("Not enough files, skipping");
				}

				final String fileName = Data.hash(stringBuffer.toString());
				Path outputFile = getFolderFromBigFile(fileName);
//				new SaveBigFile(fileSystem, list, outputFile).run();
				Data.getExecutor().schedule(new SaveBigFile(fileSystem, list, outputFile), 0, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				isWritingBigFile.set(false);
				LOGGER.debug(e.getMessage());
			}
		}
	}

	private Path getSmallPath(final String fileName) {
//        final String typeDirectory = Objects.firstNonNull(Data.extensionToMimeType(fileName),
//                "application/octet-stream").replace('/', '_');
//        final String bucketDirectory = Data.hash(fileName).substring(0, 2);
		return new Path(this.smallFilesPath, fileName);
	}

	private int numSmallFiles() throws IOException {
		return fileSystem.listStatus(smallFilesPath).length;
	}


	// Classes

	private class SaveBigFile implements Runnable {

		private FileSystem fileSystem;
		private FileStatus[] files;
		private Path outputFile;

		private SaveBigFile(FileSystem fileSystem, FileStatus[] files, Path outputFile) {
			this.fileSystem = fileSystem;
			this.files = files;
			this.outputFile = outputFile;
		}

		@Override
		public void run() {

			// Make the zip file

			ZipOutputStream out;
			try {
				LOGGER.debug("Opening ZIP file {}", outputFile.getName());
				out = new ZipOutputStream(new BufferedOutputStream(fileSystem.create(outputFile)));
			} catch (Exception e) {
				LOGGER.debug("Unable to open ZIP file");
				isWritingBigFile.set(false);
				return;
			}

			byte[] data = new byte[1024];
			int count;

			for (FileStatus f : files) {
				try {
					BufferedInputStream in = new BufferedInputStream(fileSystem.open(f.getPath()));

					out.putNextEntry(new ZipEntry(f.getPath().getName()));
					while ((count = in.read(data, 0, 1000)) != -1) {
						out.write(data, 0, count);
					}
					in.close();

				} catch (Exception e) {
					// ignore
					LOGGER.warn("Unable to write to ZIP file {}", outputFile.getName(), e);
				}
			}

			try {
				LOGGER.debug("Closing ZIP file {}", outputFile.getName());
				out.flush();
				out.close();
			} catch (Exception e) {
				// ignore
				LOGGER.warn("Unable to close ZIP file {}", outputFile.getName(), e);
			}


			// Update index

			for (FileStatus f : files) {
				LOGGER.debug("Adding file {} to index", f.getPath().getName());
				Document doc = new Document();
				doc.add(new Field(FILENAME_FIELD_NAME, f.getPath().getName(), Field.Store.YES, Field.Index.NOT_ANALYZED));
				doc.add(new Field(ZIP_FIELD_NAME, outputFile.getName(), Field.Store.YES, Field.Index.NOT_ANALYZED));
				try {
					// use "update" instead of "add" to avoid duplicates
					luceneWriter.updateDocument(new Term(FILENAME_FIELD_NAME, f.getPath().getName()), doc);
				} catch (Exception e) {
					LOGGER.warn("Error in writing file {} to Lucene index", f.getPath().getName(), e);
				}
			}

			try {
				luceneWriter.commit();
			} catch (Exception e) {
				// ignored
			}

//			try {
//				LOGGER.debug("Optimizing index");
//				luceneWriter.optimize();
//				synchronized (luceneReader) {
//					luceneReader = luceneWriter.getReader();
//				}
//			} catch (Exception e) {
//				LOGGER.warn("Error in optimizing Lucene index {}", e.getMessage());
//			}


			// Delete small files

			for (FileStatus f : files) {
				try {
					LOGGER.debug("Deleting file {}", f.getPath().getName());
					fileSystem.delete(f.getPath(), false);
				} catch (Exception e) {
					// ignore
					LOGGER.warn("Unable to delete file {}", f.getPath().getName());
				}
			}

			isWritingBigFile.set(false);
			try {
				checkSmallFilesAndMerge();
			} catch (Exception e) {
				// ignored
			}
		}
	}

	private class InterceptCloseOutputStream extends FilterOutputStream {

		private String fileName;

		private InterceptCloseOutputStream(OutputStream out, String fileName) {
			super(out);
			this.fileName = fileName;
		}

		@Override
		public void close() throws IOException {
			try {
				LOGGER.debug("Closing {}", fileName);
				super.close();
			} finally {
				filesInWritingMode.remove(this.fileName);
			}
		}
	}

	private class LuceneIterator implements Iterable<String> {

		private IndexReader reader;

		private LuceneIterator(IndexReader reader) {
			this.reader = reader;
			LOGGER.debug("Reader loaded! Num files: {}", this.reader.numDocs());
		}

		@Override
		public Iterator<String> iterator() {
			return new Iterator<String>() {

				private int currentIndex = 0;

				@Override
				public boolean hasNext() {
					try {
						Document document = reader.document(currentIndex);
						if (document != null) {
							return true;
						}
					} catch (Exception e) {
						return false;
					}
					return false;
				}

				@Override
				public String next() {
					try {
						Document document = reader.document(currentIndex++);
						if (document != null) {
							return document.get(FILENAME_FIELD_NAME);
						}
					} catch (Exception e) {
						return null;
					}

					return null;
				}

				@Override
				public void remove() {
					// empty
				}
			};
		}
	}

	private class HadoopIterator extends AbstractIterator<String> {

		private FileStatus[] files;
		private int fileIndex = 0;

		HadoopIterator(Path path, FileSystem fileSystem) throws IOException {
			this.files = fileSystem.listStatus(path);
//			this.typeDirectories = HadoopMultiFileStore.this.fileSystem.listStatus(path);
//			this.bucketDirectories = new FileStatus[]{};
//			this.files = new FileStatus[]{};
		}

		@Override
		protected String computeNext() {
			try {
				while (true) {
					if (this.fileIndex < this.files.length) {
						final FileStatus file = this.files[this.fileIndex++];
						if (!file.isDir()) {
							return file.getPath().getName();
						}
					}

					return endOfData();

//					else if (this.bucketIndex < this.bucketDirectories.length) {
//						final FileStatus bucketDirectory;
//						bucketDirectory = this.bucketDirectories[this.bucketIndex++];
//						if (bucketDirectory.isDir()) {
//							this.files = this.fileSystem
//									.listStatus(bucketDirectory.getPath());
//							this.fileIndex = 0;
//						}
//					}
//					else if (this.typeIndex < this.typeDirectories.length) {
//						final FileStatus typeDirectory;
//						typeDirectory = this.typeDirectories[this.typeIndex++];
//						if (typeDirectory.isDir()) {
//							this.bucketDirectories = this.fileSystem
//									.listStatus(typeDirectory.getPath());
//							this.bucketIndex = 0;
//						}
//					}
//					else {
//						return endOfData();
//					}
				}
			} catch (final Throwable ex) {
				throw Throwables.propagate(ex);
			}
		}

	}

}
