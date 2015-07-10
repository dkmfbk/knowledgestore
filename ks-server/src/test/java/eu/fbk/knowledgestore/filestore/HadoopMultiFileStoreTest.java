package eu.fbk.knowledgestore.filestore;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import eu.fbk.knowledgestore.runtime.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class HadoopMultiFileStoreTest {

    public static final int BUFFER_SIZE = 1024;

    protected static final String FILENAME_MISSING = "non-existing-file.txt";

    protected static final String FILENAME_1 = "file1.txt";

    protected static final byte[] CONTENT_1 = "content1".getBytes();

    private FileStore fileStore;

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

    //

    private final int NUM_FILES = 25;
    private final String CONTENT = "myContent";

    protected FileStore createFileStore() {

        String thisRandomID = UUID.randomUUID().toString();

        System.out.println(System.getProperty("java.io.tmpdir"));
        System.out.println(thisRandomID);

        return new HadoopMultiFileStore(Files.getRawLocalFileSystem(),
                System.getProperty("java.io.tmpdir") + "/ks-lucene-" + thisRandomID,
                System.getProperty("java.io.tmpdir") + "/ks-" + thisRandomID, 10, null);
    }

    @Test
    public void writeMoreFiles() {
        try {
            for (int i = 1; i <= NUM_FILES; i++) {
                final OutputStream outStream = getFileStore().write("myFile" + i);
                Assert.assertNotNull(outStream);
                outStream.write((CONTENT + i).getBytes());
                outStream.close();
                try {
                    Thread.sleep(10);
                } catch (Exception e) {
                    // ignored
                }
            }

            InputStream f9 = getFileStore().read("myFile9");
            System.out.println(new String(ByteStreams.toByteArray(f9), Charsets.UTF_8));
            f9.close();
            InputStream f1 = getFileStore().read("myFile1");
            System.out.println(new String(ByteStreams.toByteArray(f1), Charsets.UTF_8));
            f1.close();

            getFileStore().delete("myFile1");
            getFileStore().delete("myFile9");

            System.out.println("List files:");
            for (String s : getFileStore().list()) {
                System.out.println(s);
            }
        } catch (final FileExistsException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

}
