package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.io.ByteStreams;

import org.junit.*;

import eu.fbk.knowledgestore.data.Stream;

public abstract class AbstractFileStoreTest {

    /** Buffer size for reading data. */
    public static final int BUFFER_SIZE = 1024;

    protected static final String FILENAME_MISSING = "non-existing-file.txt";

    protected static final String FILENAME_1 = "file1.txt";

    protected static final byte[] CONTENT_1 = "content1".getBytes();

    private FileStore fileStore;

    protected abstract FileStore createFileStore();

    protected final FileStore getFileStore() {
        return this.fileStore;
    }

    @Before
    public void setUp() throws IOException {
        this.fileStore = createFileStore();
        this.fileStore.init();
    }

    @After
    public void tearDown() throws IOException {
        this.fileStore.close();
    }

    @Test
    public void testOneFile() throws IOException {
        Assert.assertTrue(this.fileStore.list().count() == 0);

        try {
            this.fileStore.read(AbstractFileStoreTest.FILENAME_MISSING);
            Assert.fail();
        } catch (final FileMissingException ex) {
            // ok
        }

        final OutputStream out = this.fileStore.write(AbstractFileStoreTest.FILENAME_1);
        out.write(AbstractFileStoreTest.CONTENT_1);
        out.close();

        try {
            this.fileStore.write(AbstractFileStoreTest.FILENAME_1);
            Assert.fail();
        } catch (final FileExistsException ex) {
            // ok
        }

		System.out.println(fileStore.list().getUnique());
		Assert.assertTrue(this.fileStore.list().getUnique()
                .equals(AbstractFileStoreTest.FILENAME_1));

        final InputStream in = this.fileStore.read(AbstractFileStoreTest.FILENAME_1);
        final byte[] bytes = ByteStreams.toByteArray(in);
        in.close();
        Assert.assertArrayEquals(AbstractFileStoreTest.CONTENT_1, bytes);

        this.fileStore.delete(AbstractFileStoreTest.FILENAME_1);

        try {
            this.fileStore.delete(AbstractFileStoreTest.FILENAME_1);
            Assert.fail();
        } catch (final FileMissingException ex) {
            // ok
        }

        try {
            this.fileStore.read(AbstractFileStoreTest.FILENAME_1);
            Assert.fail();
        } catch (final FileMissingException ex) {
            // ok
        }

        Assert.assertTrue(this.fileStore.list().count() == 0);
    }

    @Test
    public void deleteFile() {
        try {
            // writing the file
            OutputStream outStream;
            outStream = getFileStore().write(AbstractFileStoreTest.FILENAME_1);
            Assert.assertNotNull(outStream);
            outStream.write(AbstractFileStoreTest.CONTENT_1);
            outStream.close();
            // deleting the file
            getFileStore().delete(AbstractFileStoreTest.FILENAME_1);
        } catch (final FileExistsException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void readFile() {
        try {
            // writing the file
            final OutputStream outStream = getFileStore().write(AbstractFileStoreTest.FILENAME_1);
            Assert.assertNotNull(outStream);
            outStream.write(AbstractFileStoreTest.CONTENT_1);
            outStream.close();
            // reading what has been written
            final InputStream inStream = getFileStore().read(AbstractFileStoreTest.FILENAME_1);
            Assert.assertNotNull(inStream);
            final byte[] b = new byte[AbstractFileStoreTest.BUFFER_SIZE];
            final int off = 0;
            final int len = AbstractFileStoreTest.BUFFER_SIZE;
            int length = inStream.read(b, off, len);
            Assert.assertEquals(new String(b, 0, length).getBytes().length, AbstractFileStoreTest.CONTENT_1.length);
        } catch (final FileExistsException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void writeFile() {
        try {
            final OutputStream outStream = getFileStore().write(AbstractFileStoreTest.FILENAME_1);
            Assert.assertNotNull(outStream);
            outStream.write(AbstractFileStoreTest.CONTENT_1);
            outStream.close();
        } catch (final FileExistsException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void listFiles() throws IOException {
        final Stream<String> files = getFileStore().list();
        try {
            Assert.assertNotNull(files);
            Assert.assertEquals(files.count(), 0);
        } finally {
            files.close();
        }
    }

}
