package eu.fbk.knowledgestore.runtime;

import java.io.Closeable;
import java.io.IOException;

/**
 * A KnowledgeStore internal component.
 * <p>
 * This interface defines the basic API and lifecycle of a generic KnowledgeStore internal
 * component, and is specialized for specific types of components.
 * </p>
 * <p>
 * The <i>lifecycle</i> of a {@code Component} is the following:
 * <ul>
 * <li>The {@code Component} instance is created starting with a number of key/value properties
 * and using one of the mechanisms supported by {@link Factory} (i.e., constructor, static factory
 * method, builder pattern). The {@code Component} instance configures itself based on the
 * supplied properties. In case the configuration is incorrect, an exception will be thrown;
 * otherwise, the component is now configured, but still inactive, meaning that no activity is
 * being carried out by the component, no persistent data is being modified and no resource that
 * needs to be later freed is being allocated.</li>
 * <li>Method {@link #init()} is called to initialize the {@code Component} instance and make it
 * operational; differently from the constructor, method {@code init()} is allowed to allocate any
 * resource, to modify persisted data and to start any task as necessary for the component to
 * perform its tasks.</li>
 * <li>Methods in the {@code Component} interface are called by external code (e.g., the
 * frontend). Whether these methods can be called by a thread at a time (meaning the component can
 * be thread-unsafe) or concurrently by multiple threads (meaning the component must be
 * thread-safe) depends on the particular type of component, as documented in itsJavadoc.</li>
 * <li>Method {@link #close()} is called to dispose the {@code Component} object, allowing it to
 * free allocated resources (e.g., close files and network connections) in an orderly way. Note
 * that method {@code close()} can be called at any time after the component has been instantiated
 * (even before initialization, in case external failure requires the component to be immediately
 * disposed). In particular, it is possible for the {@code close()} method to be called while a
 * component method is being invoked by another thread.</li>
 * </ul>
 * </p>
 * <p>
 * Components are expected to access external resources (e.g., storage) or to communicate to
 * external processes, hence methods in this interface and its specializations may throw
 * {@link IOException}s. As a special kind of <tt>IOException</tt>, they may throw a
 * {@link DataCorruptedException} in case a data corruption situation is detected, possibly
 * triggering some external recovery procedure.
 * </p>
 */
public interface Component extends Closeable {

    /**
     * Initializes the {@code Component} with the supplied {@code Runtime} object. This method is
     * called after the instantiation of a {@code Component} and before any other instance method
     * is called. It provides a {@code Runtime} that can be used to access runtime services such
     * as locking, serialization and filesystem access. The {@code Component} is allowed to
     * perform any initialization operation that is necessary in order to become functional; on
     * failure, these operations may result in a {@link IOException} being thrown.
     * 
     * @throws IOException
     *             in case initialization fails
     * @throws IllegalStateException
     *             in case the component has already been initialized or closed
     */
    void init() throws IOException, IllegalStateException;

    /**
     * Closes this {@code Component} object. If the component has been initialized, closing a it
     * causes any allocated resource to be freed and any operation or transaction ongoing within
     * the component being aborted; in case the component has not been initialized yet, or
     * {@code close()} has already being called, calling this method has no effect. Note that the
     * operation affects only the local {@code Component} object and not any remote service this
     * object may rely on to implement its functionalities; in particular, such a remote service
     * is not shutdown by the operation, so that it can be accessed by other {@code Component}
     * instances possibly running in other (virtual) machines. Similarly, closing a
     * {@code Component} object has no impact on stored data, that continues to be persisted and
     * will be accessed unchanged (provided no external modification occurs) the next time a
     * similarly configured {@code Component} is created.
     */
    @Override
    void close();

}
