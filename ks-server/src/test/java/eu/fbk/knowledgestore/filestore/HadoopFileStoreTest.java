package eu.fbk.knowledgestore.filestore;

import java.util.UUID;

import eu.fbk.knowledgestore.filestore.FileStore;
import eu.fbk.knowledgestore.filestore.HadoopFileStore;
import eu.fbk.knowledgestore.runtime.Files;

public class HadoopFileStoreTest extends AbstractFileStoreTest {

    @Override
    protected FileStore createFileStore() {
        return new HadoopFileStore(Files.getRawLocalFileSystem(),
                System.getProperty("java.io.tmpdir") + "/ks-" + UUID.randomUUID().toString());
    }

}
