package eu.fbk.knowledgestore.data;

import java.util.Iterator;

import javax.annotation.Nullable;

/**
 * A handler accepting a sequence of elements, one at a time.
 * <p>
 * Implementations of this interface are accepted by methods that performs an internal, push-style
 * iteration by forwarding a sequence of elements, one at a time, to a supplied Handler; to this
 * respect, this interface complements the {@link Iterator} which supports an external, pull-style
 * iteration.
 * </p>
 * <p>
 * A Handler implements a single method {@link #handle(Object)} that is called for each object of
 * the sequence, which MUST not be null; at the end of the sequence, the method is called a last
 * time passing null as sentinel value. This interface does not specify how to interrupt the
 * iteration and how to deal with exceptions, which depend on (and are documented as part of) the
 * specific method accepting a Handler. In particular:
 * </p>
 * <ul>
 * <li>interruption of iteration MAY be implemented using the standard mechanism of thread
 * interruption (see {@link Thread#interrupt()}), which may be triggered inside the
 * {@code handle()} method and will cause, eventually, the end of the iteration;</li>
 * <li>exceptions thrown by the Handler MAY cause the iteration to stop immediately, without the
 * end of sequence being propagated.</li>
 * </ul>
 * <p>
 * Implementations of this interface are not expected to be thread safe. It is a responsibility of
 * the caller of {@link #handle(Object)} to never invoke this method multiple times concurrently.
 * </p>
 * 
 * @param <T>
 *            the type of element
 */
public interface Handler<T> {

    /**
     * Callback method called for each non-null element of the sequence, and with a null value at
     * the end of the sequence.
     * 
     * @param element
     *            the element
     * @throws Throwable
     *             on failure
     */
    void handle(@Nullable T element) throws Throwable;

}
