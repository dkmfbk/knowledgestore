package eu.fbk.knowledgestore.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.collect.ForwardingObject;

import eu.fbk.knowledgestore.data.Stream;

/**
 * A {@code FileStore} that forwards all its method calls to another {@code FileStore}.
 * <p>
 * This class provides a starting point for implementing the decorator pattern on top of the
 * {@code FileStore} interface. Subclasses must implement method {@link #delegate()} and override
 * the methods of {@code FileStore} they want to decorate.
 * </p>
 */
public abstract class ForwardingFileStore extends ForwardingObject implements FileStore {

	@Override
	protected abstract FileStore delegate();

	@Override
	public void init() throws IOException {
		delegate().init();
	}

	@Override
	public InputStream read(final String filename) throws FileMissingException, IOException {
		return delegate().read(filename);
	}

	@Override
	public OutputStream write(final String filename) throws FileExistsException, IOException {
		return delegate().write(filename);
	}

	@Override
	public void delete(final String filename) throws FileMissingException, IOException {
		delegate().delete(filename);
	}

	@Override
	public Stream<String> list() throws IOException {
		return delegate().list();
	}

	@Override
	public void close() {
		delegate().close();
	}

}
