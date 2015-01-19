package eu.fbk.knowledgestore.data;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.Futures;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

import eu.fbk.knowledgestore.internal.Util;

// Metadata can be made available by some Stream implementations or can be attached to a
// Stream via

/**
 * A stream of typed elements that can be consumed at most once.
 * <p>
 * A Stream returns a sequence of elements (e.g., the results of a retrieval API operation) one at
 * a time, after which it is no more usable, allowing manipulation and consumption of elements it
 * in a number of way.
 * </p>
 * <p>
 * A Stream can be created:
 * </p>
 * <ul>
 * <li>using one of the static {@code create()} factory methods starting from an
 * {@link #create(Object...) array}, an {@link #create(Iterable) Iterable}, an
 * {@link #create(Iterator) Iterator}, a Sesame {@link #create(Iteration) Iteration} or an
 * {@link #create(Enumeration) Enumeration} of elements;</li>
 * <li>concatenating zero or more Streams or Iterables, via {@link #concat(Iterable)} or
 * {@link #concat(Iterable...)};</li>
 * <li>subclassing the Stream class and overriding at least one of protected methods
 * {@link #doIterator()} and {@link #doToHandler(Handler)} (the default implementation of each of
 * them is based on the implementation of the other one).</li>
 * </ul>
 * <p>
 * While the first method supports the usual <i>external iteration</i> / pull-like pattern based
 * on {@code Iterator}s, the second supports an <i>internal iteration</i> / push-like pattern
 * where iterated elements are forwarded to an {@code Handler} and control of the iteration is
 * maintained by the {@code Stream}. Although implementing external iteration should be preferred
 * (as more flexible and immediately convertible to internal iteration), there are cases where
 * only internal iteration is feasible (e.g., because more simple to implement, or because the
 * ultimate source of the iterated elements is a third party library that supports only an
 * internal iteration / push like approach, such as Sesame parsers). In these cases, conversion
 * from internal iteration to external iteration is automatically performed by the {@code Stream}
 * using a background thread and an element queue.
 * </p>
 * <p>
 * Two main families of operations are offered: <i>intermediate operations</i> and <i>terminal
 * operations</i> (as in JDK 8 streams).
 * </p>
 * <p>
 * Intermediate operations</i> return new Stream wrappers that lazily manipulate elements of a
 * source Stream while they are traversed, or otherwise enrich the source Stream, and can be
 * chained in a fluent style. These operations are:
 * <ul>
 * <li>element filtering, via {@link #filter(Predicate) sequential} or
 * {@link #filter(Predicate, int) parallel} {@code filter()} methods;</li>
 * <li>element transformation, via {@link #transform(Function) sequential} or
 * {@link #transform(Function, int) parallel} {@code transform()} methods;</li>
 * <li>duplicate removal, via {@link #distinct()};</li>
 * <li>element slicing, via {@link #slice(long, long)};</li>
 * <li>element chunking, via {@link #chunk(int)};</li>
 * <li>iteration timeout, via {@link #timeout(long)}.</li>
 * </ul>
 * </p>
 * <p>
 * Terminal operations consume the elements of the stream. At most a terminal operation can be
 * invoked on a Stream, reflecting the fact that elements can be accessed only once. Before
 * invoking it, the Stream is {@link #isAvailable() available}; after invoking it, the Stream is
 * <i>consumed</i> and no other intermediate or terminal operation can be called on it. Terminal
 * operations are:
 * <ul>
 * <li>element counting, via {@link #count()};</li>
 * <li>external, pull-style iteration, via {@link #iterator()};</li>
 * <li>internal, push-style iteration, via {@link #toHandler(Handler)};</li>
 * <li>array construction, via {@link #toArray(Class)};</li>
 * <li>immutable List construction, via {@link #toList()};</li>
 * <li>immutable Set construction, via {@link #toSet()};</li>
 * <li>immutable sorted List construction, via {@link #toSortedList(Comparator)};</li>
 * <li>immutable SortedSet construction, via {@link #toSortedSet(Comparator)};</li>
 * <li>Map {@link #toMap(Function, Function, Map) population} or immutable Map
 * {@link #toMap(Function, Function) construction}, via {@code toMap()} methods;</li>
 * <li>Multimap {@link #toMultimap(Function, Function, Multimap) population} or immutable Multimap
 * {@link #toMultimap(Function, Function) construction}, via {@code toMultimap()} methods;</li>
 * <li>unique element extraction, via {@link #getUnique()} and {@link #getUnique(Object)};</li>
 * </ul>
 * </p>
 * <p>
 * Streams are thread safe. They implement the {@link Iterable} interface, hence can be used in
 * enhanced {@code for} loops, and the {@code Closeable} interface, as they may wrap underlying
 * resources (e.g., network connections) that need to be closed. Closing a Stream causes the call
 * of method {@link #doClose()}, which can be overridden by subclasses to perform additional
 * cleanup. Note that closing a Stream will cause any pending terminal operation to be interrupted
 * (on a best effort basis), resulting in an exception being thrown by that operation. Also,
 * completion of a terminal operation automatically causes the Stream to be closed (as it cannot
 * be used in any other way).
 * </p>
 * 
 * @param <T>
 *            the type of element returned by the Stream
 */
public abstract class Stream<T> implements Iterable<T>, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Stream.class);

    private static final Object EOF = new Object();

    final State state;

    /**
     * Constructor for use by non-delegating sub-classes.
     */
    protected Stream() {
        this(new State());
    }

    Stream(final State state) {
        this.state = state;
        state.closeObjects.add(this);
    }

    /**
     * Creates a new Stream for the supplied elements. Use this method without arguments to
     * produce an <i>empty</i> Stream, or with a single argument to produce a <i>singleton</i>
     * Stream.
     * 
     * @param elements
     *            the elements to be returned by the Stream, not null
     * @param <T>
     *            the type of elements
     * @return a Stream over the supplied elements
     */
    public static <T> Stream<T> create(@SuppressWarnings("unchecked") final T... elements) {
        if (elements.length == 0) {
            return new EmptyStream<T>();
        } else if (elements.length == 1) {
            return new SingletonStream<T>(elements[0]);
        } else {
            return new IteratorStream<T>(Iterators.forArray(elements));
        }
    }

    /**
     * Creates a new Stream over the elements of the supplied {@code Iterable}. If the supplied
     * {@code Iterable} is a {@code Stream}, it will be returned unchanged. If the supplied
     * {@code Iterable} implements {@code Closeable}, method {@link Closeable#close()} will be
     * called when the {@code Stream} is closed.
     * 
     * @param iterable
     *            an {@code Iterable} of non-null elements, possibly empty but not null
     * @param <T>
     *            the type of element
     * @return a {@code Stream} over the elements in the {@code Iterable}
     */
    @SuppressWarnings("unchecked")
    public static <T> Stream<T> create(final Iterable<? extends T> iterable) {
        if (iterable instanceof Stream) {
            return (Stream<T>) iterable;
        } else if (iterable instanceof ImmutableCollection<?>
                && ((ImmutableCollection<? extends T>) iterable).isEmpty()) {
            return new EmptyStream<T>();
        } else {
            return new IterableStream<T>(iterable);
        }
    }

    /**
     * Creates a new Stream over the elements returned by the supplied Iterator.
     * 
     * @param iterator
     *            an Iterator returning non-null elements
     * @param <T>
     *            the type of elements
     * @return a Stream over the elements returned by the supplied Iterator
     */
    public static <T> Stream<T> create(final Iterator<? extends T> iterator) {
        if (iterator.hasNext()) {
            return new IteratorStream<T>(iterator);
        } else {
            return new EmptyStream<T>();
        }
    }

    /**
     * Creates a new Stream over the elements returned by the supplied Sesame Iteration.
     * 
     * @param iteration
     *            the Iteration
     * @param <T>
     *            the type of elements
     * @return a Stream over the elements returned by the supplied Iteration
     */
    public static <T> Stream<T> create(final Iteration<? extends T, ?> iteration) {
        return new IterationStream<T>(iteration);
    }

    /**
     * Creates a new Stream over the elements returned by the supplied Enumeration.
     * 
     * @param enumeration
     *            an Enumeration of non-null elements
     * @param <T>
     *            the type of elements
     * @return a Stream over the elements returned by the supplied Enumeration
     */
    public static <T> Stream<T> create(final Enumeration<? extends T> enumeration) {
        if (enumeration.hasMoreElements()) {
            return new IteratorStream<T>(Iterators.forEnumeration(enumeration));
        } else {
            return new EmptyStream<T>();
        }
    }

    /**
     * Returns a Stream concatenating zero or more Iterables. If an input Iterable is a Stream, it
     * is closed as soon as exhausted or as iteration completes.
     * 
     * @param iterables
     *            an Iterable with the Iterables or Streams to concatenate
     * @param <T>
     *            the type of elements
     * @return the resulting concatenated Stream
     */
    public static <T> Stream<T> concat(final Iterable<? extends Iterable<? extends T>> iterables) {
        return new ConcatStream<Iterable<? extends T>, T>(create(iterables));
    }

    /**
     * Returns a Stream concatenating zero or more Iterables. If an input Iterable is a Stream, it
     * is closed as soon as exhausted or as iteration completes.
     * 
     * @param iterables
     *            a vararg array with the Iterables or Streams to concatenate
     * @param <T>
     *            the type of elements
     * @return the resulting concatenated Stream
     */
    public static <T> Stream<T> concat(
            @SuppressWarnings("unchecked") final Iterable<? extends T>... iterables) {
        return new ConcatStream<Iterable<? extends T>, T>(create(iterables));
    }

    /**
     * Intermediate operation returning a Stream with only the elements of this Stream that
     * satisfy the specified predicate. If {@code parallelism > 1}, up to {@code parallelism}
     * evaluations of the predicate are simultaneously performed in background threads for greater
     * throughput.
     * 
     * @param predicate
     *            the predicate, never called with a null input
     * @param parallelism
     *            the parallelism degree, i.e., the maximum number of predicate evaluations that
     *            can be performed in parallel (if <= 1 no parallel evaluation will be performed)
     * @return a Stream over the elements satisfying the predicate
     */
    public final Stream<T> filter(final Predicate<? super T> predicate, final int parallelism) {
        synchronized (this.state) {
            checkState();
            return new FilterStream<T>(this, parallelism, predicate);
        }
    }

    /**
     * Intermediate operation returning a Stream with the elements obtained from the ones of this
     * Stream by applying the specified transformation function. Method
     * {@link Function#apply(Object)} is called to transform each input element into an output
     * element. If {@code parallelism > 1}, up to {@code parallelism} evaluations of the function
     * are simultaneously performed in background threads for greater throughput. Note that the
     * function is never called with a null input; in case it returns a null output, it is ignored
     * and iteration moves to the next element (this feature can be used to combine filtering with
     * transformation).
     * 
     * @param function
     *            the function, not null
     * @param parallelism
     *            the parallelism degree, i.e., the maximum number of function evaluations that
     *            can be performed in parallel (if <= 1 no parallel evaluation will be performed)
     * @param <R>
     *            the type of transformed elements
     * @return a Stream over the transformed elements
     */
    public final <R> Stream<R> transform(final Function<? super T, ? extends R> function,
            final int parallelism) {
        synchronized (this.state) {
            checkState();
            return new TransformElementStream<T, R>(this, parallelism, function);
        }
    }

    /**
     * Intermediate operation returning a Stream with the elements obtained by applying the
     * supplied transformation functions to the {@code Iterator} and {@code Handler} returned /
     * accepted by this Stream. At least a function must be supplied. If both of them are
     * supplied, make sure that they perform the same transformation.
     * 
     * @param iteratorFunction
     *            the transformation function responsible to adapt the {@code Iterator} returned
     *            by this Stream
     * @param handlerFunction
     *            the transformation function responsible to adapt the {@code Handler} accepted by
     *            this Stream
     * @param <R>
     *            the type of transformed elements
     * @return a Stream over the transformed sequence of elements
     */
    public final <R> Stream<R> transform(
            @Nullable final Function<Iterator<T>, Iterator<R>> iteratorFunction,
            @Nullable final Function<Handler<R>, Handler<T>> handlerFunction) {
        synchronized (this.state) {
            checkState();
            return new TransformSequenceStream<T, R>(this, iteratorFunction, handlerFunction);
        }
    }

    /**
     * Intermediate operation returning a Stream with the elements obtained by applying an
     * optional <i>navigation path</i> and conversion to a certain type to the elements of this
     * Stream. The path is a sequence of keys ({@code String}s, {@code URI}s, generic objects)
     * that are applied to {@code Record}, {@code BindingSet}, {@code Map} and {@code Multimap}
     * elements to extract child elements in a recursive fashion. Starting from an element
     * returned by this stream, the result of this navigation process is a list of (sub-)child
     * elements that are converted to the requested type (via {@link Data#convert(Object, Class)})
     * and concatenated in the resulting stream; {@code Iterable}s, {@code Iterator}s and arrays
     * found during the navigation are exploded and their elements individually considered. The
     * {@code lenient} parameters controls whether conversion errors should be ignored or result
     * in an exception being thrown by the returned Stream.
     * 
     * @param type
     *            the class resulting elements should be converted to
     * @param lenient
     *            true if conversion errors should be ignored
     * @param path
     *            a vararg array of zero or more keys that recursively select the elements to
     *            return
     * @param <R>
     *            the type of resulting elements
     * @return a Stream over the elements obtained applying the navigation path and the conversion
     *         specified
     */
    public final <R> Stream<R> transform(final Class<R> type, final boolean lenient,
            final Object... path) {
        synchronized (this.state) {
            checkState();
            return concat(new TransformPathStream<T, R>(this, type, lenient, path));
        }
    }

    /**
     * Intermediate operation returning a Stream with only the distinct elements of this Stream.
     * Duplicates are removed lazily during the iteration. Note that duplicate removal requires to
     * keep track of the elements seen, so an amount of memory proportional to the number of
     * elements in the Stream is required.
     * 
     * @return a Stream over de-duplicated elements
     */
    public final Stream<T> distinct() {
        synchronized (this.state) {
            checkState();
            return this instanceof DistinctStream<?> ? this : new DistinctStream<T>(this);
        }
    }

    /**
     * Intermediate operation returning a Stream with max {@code limit} elements with index
     * starting at {@code offset} taken from this Stream. After those elements are returned, the
     * wrapped Stream is automatically closed.
     * 
     * @param offset
     *            the offset where to start returning elements from, not negative
     * @param limit
     *            the maximum number of elements to return (starting from offset), not negative
     * @return a Stream wrapping this Stream and limiting the number of returned elements
     */
    public final Stream<T> slice(final long offset, final long limit) {
        synchronized (this.state) {
            checkState();
            return new SliceStream<T>(this, offset, limit);
        }
    }

    /**
     * Intermediate operation returning a Stream of elements chunks of the specified size obtained
     * from this Stream (the last chunk may be smaller).
     * 
     * @param chunkSize
     *            the chunk size, positive
     * @return a Stream wrapping this Stream and returning chunks of elements
     */
    public final Stream<List<T>> chunk(final int chunkSize) {
        synchronized (this.state) {
            checkState();
            return new ChunkStream<T>(this, chunkSize);
        }
    }

    /**
     * Intermediate operation returning a Stream that returns the elements of this {@code Stream}
     * and tracks the number of elements returned so far. As tracking the number elements has a
     * small cost (due to {@code Iterator} and {@code Handler} wrapping, this feature must be
     * explicitly requested and is not offered as part of the default feature set of
     * {@code Stream}.
     * 
     * @param counter
     *            the variable where to hold the number of returned elements, possibly null
     * @param eof
     *            the variable where to store whether end of sequence has been reached, possibly
     *            null
     * @return a {@code Stream} tracking the number of returned elements
     */
    public final Stream<T> track(@Nullable final AtomicLong counter,
            @Nullable final AtomicBoolean eof) {
        synchronized (this.state) {
            checkState();
            return new TrackStream<T>(this, counter, eof);
        }
    }

    /**
     * Terminal operation returning the number of elements in this Stream. Note that only few
     * elements are materialized at any time, so it is safe to use this method with arbitrarily
     * large Streams.
     * 
     * @return the number of elements in this Stream
     */
    public final long count() {
        final AtomicLong result = new AtomicLong();
        toHandler(new Handler<T>() {

            private long count;

            @Override
            public void handle(final T element) {
                if (element != null) {
                    ++this.count;
                } else {
                    result.set(this.count);
                }

            }

        });
        return result.get();
    }

    /**
     * Terminal operation returning an Iterator over the elements of this Stream. {@inheritDoc}
     */
    @Override
    public final Iterator<T> iterator() {
        synchronized (this.state) {
            checkState();
            this.state.available = false;
            final Iterator<T> iterator;
            try {
                iterator = new CheckedIterator<T>(doIterator(), this);
            } catch (final Throwable ex) {
                throw Throwables.propagate(ex);
            }
            this.state.activeIterator = iterator;
            return iterator;
        }
    }

    /**
     * Terminal operations that forwards all the elements of this Stream to the Handler specified.
     * No explicit mechanism is provided for interrupting the Iteration, although many Streams may
     * react to the standard {@link Thread#interrupt()} signal to stop iteration.
     * 
     * @param handler
     *            the Handler where to forward elements
     */
    public final void toHandler(final Handler<? super T> handler) {
        Preconditions.checkNotNull(handler);
        synchronized (this.state) {
            checkState();
            this.state.available = false;
            this.state.toHandlerThread = Thread.currentThread();
        }
        try {
            try {
                doToHandler(handler);
            } catch (final Throwable ex) {
                Throwables.propagate(ex);
            }
        } finally {
            synchronized (this.state) {
                if (this.state.closed) {
                    checkState(); // fail in case stream was closed while iteration was active
                }
                this.state.toHandlerThread = null; // to avoid interruption
                Thread.interrupted(); // clear interruption status
                close(); // if not closed, will close the stream now
            }
        }
    }

    /**
     * Terminal operation returning an array of the specified type with all the elements of this
     * Stream. Call this method only if there is enough memory to hold the resulting array.
     * 
     * @param elementClass
     *            the type of element to be stored in the array (necessary for the array creation)
     * @return the resulting array
     */
    public final T[] toArray(final Class<T> elementClass) {
        return Iterables.toArray(toCollection(Lists.<T>newArrayListWithCapacity(256)),
                elementClass);
    }

    /**
     * Terminal operation returning an immutable List with all the elements of this Stream. Call
     * this method only if there is enough memory to hold the resulting List.
     * 
     * @return the resulting immutable List
     */
    public final List<T> toList() {
        return ImmutableList.copyOf(toCollection(Lists.<T>newArrayListWithCapacity(256)));
    }

    /**
     * Terminal operation returning an immutable Set with all the elements of this Stream. Call
     * this method only if there is enough memory to hold the resulting Set. Note that duplicate
     * elements are automatically removed from the resulting Set.
     * 
     * @return the resulting immutable Set
     */
    public final Set<T> toSet() {
        return ImmutableSet.copyOf(toCollection(Lists.<T>newArrayListWithCapacity(256)));
    }

    /**
     * Terminal operation returning an immutable List with all the elements of this Stream, sorted
     * using the supplied Comparator. Use {@link Ordering#natural()} to sort Comparable elements
     * based on {@link Comparable#compareTo(Object)} order. Call this method only if there is
     * enough memory to hold the resulting List.
     * 
     * @param comparator
     *            the Comparator to sort elements, not null
     * @return the resulting immutable sorted list
     */
    public final List<T> toSortedList(final Comparator<? super T> comparator) {
        return Ordering.from(comparator).immutableSortedCopy(
                toCollection(Lists.<T>newArrayListWithCapacity(256)));
    }

    /**
     * Terminal operation returning an immutable List with all the elements of this Stream, sorted
     * based on a sort key obtained by applying an optional <i>navigation path</i> and conversion
     * to a specified type. The path is a sequence of keys ({@code String}s, {@code URI}s, generic
     * objects) that are applied to {@code Record}, {@code BindingSet}, {@code Map} and
     * {@code Multimap} elements to extract child elements in a recursive fashion. Starting from
     * an element returned by this stream, the result of this navigation process is either null or
     * a {@code Comparable} key object that is converted to the requested type (via
     * {@link Data#convert(Object, Class)}) and used for comparing the element with other element.
     * The {@code lenient} parameters controls whether conversion errors should be ignored or
     * result in an exception being thrown.
     * 
     * @param type
     *            the class of the sort key
     * @param lenient
     *            true if conversion errors should be ignored
     * @param path
     *            a vararg array of zero or more keys that recursively select the sort key
     * @return the resulting immutable sorted list
     */
    public final List<T> toSortedList(final Class<? extends Comparable<?>> type,
            final boolean lenient, final Object... path) {
        return Ordering.from(new PathComparator(type, lenient, path)).immutableSortedCopy(
                toCollection(Lists.<T>newArrayListWithCapacity(256)));
    }

    /**
     * Terminal operation returning an immutable SortedSet with all the elements of this Stream,
     * sorted using the supplied {@code Comparator}. Use {@link Ordering#natural()} to sort
     * Comparable elements based on {@link Comparable#compareTo(Object)} order. Call this method
     * only if there is enough memory to hold the resulting SortedSet.
     * 
     * @param comparator
     *            the {@code Comparator} to sort elements, not null
     * @return the resulting immutable SortedSet
     */
    public final SortedSet<T> toSortedSet(final Comparator<? super T> comparator) {
        return ImmutableSortedSet.copyOf(comparator,
                toCollection(Lists.<T>newArrayListWithCapacity(256)));
    }

    /**
     * Terminal operation storing all the elements of this Stream in the supplied Collection. Call
     * this method only if the target Collection can hold all the remaining elements.
     * 
     * @param collection
     *            the Collection where to store elements, not null
     * @param <C>
     *            the type of Collection
     * @return the supplied Collection
     */
    public final <C extends Collection<? super T>> C toCollection(final C collection) {
        Preconditions.checkNotNull(collection);
        toHandler(new Handler<T>() {

            @Override
            public void handle(final T element) {
                if (element != null) {
                    collection.add(element);
                }
            }
        });
        return collection;
    }

    /**
     * Terminal operation returning an immutable Map indexing the elements of this Stream as
     * {@code <key, value>} pairs computed using the supplied Functions. The supplied key and
     * value Functions are called for each element, producing the keys and values to store in the
     * Map. If a null key or value are produced, the element is discarded. If multiple values are
     * mapped to the same key, only the most recently computed one will be stored. Use
     * {@link Functions#identity()} in case no transformation is required to extract the key
     * and/or the value.
     * 
     * @param keyFunction
     *            the key function
     * @param valueFunction
     *            the value function
     * @param <K>
     *            the type of key
     * @param <V>
     *            the type of value
     * @return an immutable Map with the computed {@code <key, value>} pairs
     */
    public final <K, V> Map<K, V> toMap(final Function<? super T, ? extends K> keyFunction,
            final Function<? super T, ? extends V> valueFunction) {
        return ImmutableMap
                .copyOf(toMap(keyFunction, valueFunction, Maps.<K, V>newLinkedHashMap()));
    }

    /**
     * Terminal operation storing the elements of this Stream in the supplied map as
     * {@code <key, value>} pairs computed using the supplied Functions. The supplied key and
     * value Functions are called for each element, producing the keys and values to store in the
     * Map. If a null key or value are produced, the element is discarded. If multiple values are
     * mapped to the same key, only the most recently computed one will be stored. Use
     * {@link Functions#identity()} in case no transformation is required to extract the key
     * and/or the value.
     * 
     * @param keyFunction
     *            the key function
     * @param valueFunction
     *            the value function
     * @param map
     *            the Map where to store the extracted {@code <key, value>} pairs
     * @param <K>
     *            the type of key
     * @param <V>
     *            the type of value
     * @param <M>
     *            the type of Map
     * @return the supplied Map
     */
    public final <K, V, M extends Map<K, V>> M toMap(
            final Function<? super T, ? extends K> keyFunction,
            final Function<? super T, ? extends V> valueFunction, final M map) {
        Preconditions.checkNotNull(keyFunction);
        Preconditions.checkNotNull(valueFunction);
        Preconditions.checkNotNull(map);
        toHandler(new Handler<T>() {

            @Override
            public void handle(final T element) {
                if (element != null) {
                    final K key = keyFunction.apply(element);
                    final V value = valueFunction.apply(element);
                    if (key != null && value != null) {
                        map.put(key, value);
                    }
                }
            }

        });
        return map;
    }

    /**
     * Terminal operation returning an immutable ListMultimap indexing the elements of this Stream
     * as {@code <key, value>} pairs computed using the supplied Functions. The supplied key and
     * value Functions are called for each element, producing the keys and values to store. If a
     * null key or value are produced, the element is discarded. Use {@link Functions#identity()}
     * in case no transformation is required to extract the key and/or the value.
     * 
     * @param keyFunction
     *            the key function
     * @param valueFunction
     *            the value function
     * @param <K>
     *            the type of key
     * @param <V>
     *            the type of value
     * @return an immutable ListMultimap
     */
    public final <K, V> ListMultimap<K, V> toMultimap(
            final Function<? super T, ? extends K> keyFunction,
            final Function<? super T, ? extends V> valueFunction) {
        return ImmutableListMultimap.copyOf(toMultimap(keyFunction, valueFunction,
                ArrayListMultimap.<K, V>create()));
    }

    /**
     * Terminal operation storing the elements of this Stream in the supplied Multimap as
     * {@code <key, value>} pairs computed using the supplied Functions. The supplied key and
     * value functions are called for each element, producing the keys and values to store. If a
     * null key or value are produced, the element is discarded. Use {@link Functions#identity()}
     * in case no transformation is required to extract the key and/or the value.
     * 
     * @param keyFunction
     *            the key function
     * @param valueFunction
     *            the value function
     * @param multimap
     *            the Multimap where to store the extracted {@code <key, value>} pairs
     * @param <K>
     *            the type of key
     * @param <V>
     *            the type of value
     * @param <M>
     *            the type of Multimap
     * @return the supplied Multimap
     */
    public final <K, V, M extends Multimap<K, V>> M toMultimap(
            final Function<? super T, ? extends K> keyFunction,
            final Function<? super T, ? extends V> valueFunction, final M multimap) {
        Preconditions.checkNotNull(keyFunction);
        Preconditions.checkNotNull(valueFunction);
        Preconditions.checkNotNull(multimap);
        toHandler(new Handler<T>() {

            @Override
            public void handle(final T element) {
                if (element != null) {
                    final K key = keyFunction.apply(element);
                    final V value = valueFunction.apply(element);
                    if (key != null && value != null) {
                        multimap.put(key, value);
                    }
                }
            }

        });
        return multimap;
    }

    /**
     * Terminal operation returning the only element in this Stream, or the default value
     * specified if there are no elements, multiple elements or an Exception occurs.
     * 
     * @param defaultValue
     *            the default value to return if a unique value cannot be extracted
     * @return the only element in this Stream, or the default value in case that element does not
     *         exist, there are multiple elements or an Exception occurs
     */
    public final T getUnique(final T defaultValue) {
        try {
            final T result = getUnique();
            if (result != null) {
                return result;
            }
        } catch (final Throwable ex) {
            // ignore
        }
        return defaultValue;
    }

    /**
     * Terminal operation returning the only element in this Stream, or <tt>null</tt> if there are
     * no elements. An exception is thrown in case there is more than one element in the Stream.
     * This method is designed for being used with Streams that are expected to return exactly one
     * element, for which it embeds the check on the number of elements.
     * 
     * @return the only element in the Stream
     * @throws IllegalStateException
     *             in case there are multiple elements in the Stream
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public final T getUnique() throws IllegalStateException {
        final AtomicReference<Object> holder = new AtomicReference<Object>();
        toHandler(new Handler<T>() {

            @Override
            public void handle(final T element) {
                if (element != null) {
                    if (holder.get() == null) {
                        holder.set(element);
                    } else {
                        holder.set(EOF);
                        Thread.currentThread().interrupt(); // attempt interupting iteration
                    }
                }
            }

        });
        final Object result = holder.get();
        if (result != EOF) {
            return (T) result;
        }
        throw new IllegalStateException("Stream " + this + " returned more than one element");
    }

    /**
     * Returns a metadata property about the stream. Note that {@code Stream} wrappers obtained
     * through intermediate operations don't have their own properties, but instead access the
     * metadata properties of the source {@code Stream}.
     * 
     * @param name
     *            the name of the property
     * @param type
     *            the type of the property value (conversion will be attempted if available value
     *            has a different type)
     * @param <V>
     *            the type of value
     * @return the value of the property, or null if the property is undefined
     */
    public final <V> V getProperty(final String name, final Class<V> type) {
        Preconditions.checkNotNull(name);
        try {
            Object value = null;
            synchronized (this.state) {
                if (this.state.properties != null) {
                    value = this.state.properties.get(name);
                }
            }
            return Data.convert(value, type);
        } catch (final Throwable ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Sets a metadata property about the stream. Note that {@code Stream} wrappers obtained
     * through intermediate operations don't have their own properties, but instead access the
     * metadata properties of the source {@code Stream}.
     * 
     * @param name
     *            the name of the property
     * @param value
     *            the value of the property, null to clear it
     * @return this {@code Stream}, for call chaining
     */
    public final Stream<T> setProperty(final String name, @Nullable final Object value) {
        Preconditions.checkNotNull(name);
        synchronized (this.state) {
            if (this.state.properties != null) {
                this.state.properties.put(name, value);
            } else if (value != null) {
                this.state.properties = Maps.newHashMap();
                this.state.properties.put(name, value);
            }
        }
        return this;
    }

    /**
     * Gets a timeout possibly set on the {@code Stream} and represented by the absolute
     * milliseconds timestamp when the {@code Stream} will be closed.
     * 
     * @return the milliseconds timestamp when this {@code Stream} will be closed, or null if no
     *         timeout has been set
     */
    public final Long getTimeout() {
        synchronized (this.state) {
            return this.state.timeoutFuture == null ? null : this.state.timeoutFuture
                    .getDelay(TimeUnit.MILLISECONDS) + System.currentTimeMillis();
        }
    }

    /**
     * Sets a timeout by supplying the absolute milliseconds timestamp when the {@code Stream}
     * will be forcedly closed. If the supplied value is null, any previously set timeout is
     * removed. In case the {@code Stream has already been closed}, or has just timed out due to a
     * previously set timeout, calling this method has no effect.
     * 
     * @param timestamp
     *            the milliseconds timestamp when the {@code Stream} will be closed
     * @return this {@code Stream}, for call chaining
     */
    public final Stream<T> setTimeout(@Nullable final Long timestamp) {
        Preconditions.checkArgument(timestamp == null || timestamp > System.currentTimeMillis());
        synchronized (this.state) {
            if (this.state.closed) {
                return this; // NOP, already closed
            }
            if (this.state.timeoutFuture != null) {
                if (!this.state.timeoutFuture.cancel(false)) {
                    return this; // NOP, timeout already occurred
                }
            }
            if (timestamp != null) {
                this.state.timeoutFuture = Data.getExecutor().schedule(new Runnable() {

                    @Override
                    public void run() {
                        close();
                    }

                }, Math.max(0, timestamp - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            }
            return this;
        }
    }

    /**
     * Checks whether this Stream is available, i.e., intermediate and terminal operations can be
     * called. Invoking a terminal operation or closing the Stream will make it non-available.
     * 
     * @return true, if the Stream is available
     */
    public final boolean isAvailable() {
        synchronized (this.state) {
            return this.state.available;
        }
    }

    /**
     * Checks whether this Stream has been closed. Note that a Stream is automatically closed when
     * consumption of its elements by a terminal operation completes.
     * 
     * @return true, if the Stream has been closed
     */
    public final boolean isClosed() {
        synchronized (this.state) {
            return this.state.closed;
        }
    }

    /**
     * Register zero or more objects for activation when this {@code Stream} will be closed. Each
     * supplied object can be a {@code Closeable}, in which case method {@link Closeable#close()}
     * will be called, a {@code Runnable}, in which case method {@link Runnable#run()} will be
     * called, or a {@code Callable}, in which casle method {@link Callable#call()} will be
     * called; any other type of object will be rejected resulting in an exception. In case the
     * {@code Stream} has already been closed, activation of supplied objects will be done
     * immediately.
     * 
     * @param objects
     *            the objects to activate when the {@code Stream} will be closed
     * @return this {@code Stream}, for call chaining.
     */
    public final Stream<T> onClose(final Object... objects) {
        synchronized (this.state) {
            for (final Object object : objects) {
                if (!(object instanceof Closeable) && !(object instanceof Runnable)
                        && !(object instanceof Callable)) {
                    throw new IllegalArgumentException("Illegal object: " + object);
                } else if (this.state.closed) {
                    closeAction(object);
                } else {
                    boolean alreadyContained = false;
                    for (final Object o : this.state.closeObjects) {
                        if (o == object) {
                            alreadyContained = true;
                            break;
                        }
                    }
                    if (!alreadyContained) {
                        this.state.closeObjects.add(object);
                    }
                }
            }
        }
        return this;
    }

    /**
     * Closes this {@code Stream} and releases any resource associated to it. The operation causes
     * any {@code Stream} wrapping or wrapped by this {@code Stream} to be closed. If element
     * iteration through a terminal operation is in progress, it is interrupted resulting in an
     * exception being thrown. If this {@code Stream} has already been closed, then calling this
     * method has no effect.
     */
    @Override
    public final void close() {
        synchronized (this.state) {
            if (this.state.closed) {
                return;
            }
            if (this.state.activeIterator instanceof Closeable) {
                Util.closeQuietly(this.state.activeIterator);
            }
            if (this.state.toHandlerThread != null) {
                this.state.toHandlerThread.interrupt();
            }
            for (final Object object : this.state.closeObjects) {
                closeAction(object);
            }
            this.state.activeIterator = null;
            this.state.toHandlerThread = null;
            this.state.available = false;
            this.state.closed = true;
        }
    }

    /**
     * Returns a string representation of this Stream. The resulting string depends on the actual
     * Stream class. For wrapper Streams, it shows the wrapper parameters and the wrapping
     * hierarchy.
     * 
     * @return a string representation of this Stream
     */
    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder();
        toStringHelper(builder);
        return builder.toString();
    }

    void toStringHelper(final StringBuilder builder) {
        String name = getClass().getSimpleName();
        if (name == null) {
            final Method method = getClass().getEnclosingMethod();
            if (method != null) {
                name = method.getDeclaringClass().getSimpleName() + "." + method.getName()
                        + "-Stream";
            } else {
                name = "anon-Stream";
            }
        }
        builder.append(name);
        final String args = doToString();
        if (args != null) {
            builder.append("<").append(args).append(">");
        }
    }

    final void checkState() {
        synchronized (this.state) {
            if (this.state.closed) {
                throw new IllegalStateException("Stream already closed: " + this);
            } else if (!this.state.available) {
                throw new IllegalStateException("Stream already being iterated: " + this);
            }
        }
    }

    final void closeAction(final Object object) {
        try {
            if (object instanceof Stream<?>) {
                ((Stream<?>) object).doClose();
            } else if (object instanceof Closeable) {
                ((Closeable) object).close();
            } else if (object instanceof Runnable) {
                ((Runnable) object).run();
            } else if (object instanceof Callable<?>) {
                ((Callable<?>) object).call();
            }
        } catch (final Throwable ex) {
            LOGGER.error("Error performing close action on " + object, ex);
        }
    }

    /**
     * Implementation method responsible of producing an Iterator over the elements of the Stream.
     * This method is called by {@link #iterator()} with the guarantee that it is called at most
     * once and with the Stream in the <i>available</i> state. If the returned Iterator implements
     * the {@link Closeable} interface, it will be automatically closed when the Stream is closed.
     * 
     * @return an Iterator over the elements of the Stream
     * @throws Throwable
     *             in case of failure
     */
    protected Iterator<T> doIterator() throws Throwable {
        final ToHandlerIterator<T> iterator = new ToHandlerIterator<T>(this);
        iterator.submit();
        return iterator;
    }

    /**
     * Implementation methods responsible of forwarding all the elements of the Stream to the
     * Handler specified. This method is called by {@link #toHandler(Handler)} to perform internal
     * iteration, with the guarantee that it is called at most once and with the Stream in the
     * <i>available</i> state. As a best practice, the method should intercept
     * {@link Thread#interrupt()} requests and stop iteration, if possible; also remember to call
     * {@link Handler#handle(Object)} with a null argument after the last element is reached, in
     * order to signal the end of the sequence.
     * 
     * @param handler
     *            the {@code Handler} where to forward elements
     * @throws Throwable
     *             in case of failure
     */
    protected void doToHandler(final Handler<? super T> handler) throws Throwable {
        final Iterator<T> iterator = doIterator();
        while (iterator.hasNext()) {
            if (Thread.interrupted()) {
                return;
            }
            handler.handle(iterator.next());
        }
        handler.handle(null);
    }

    /**
     * Implementation method supporting the generation of a string representation of this Stream.
     * The method should return any parameter / state characterizing this {@code Stream}, which is
     * then included within angular brackets in the String denoting the {@code Stream} structure.
     * 
     * @return an optional string with the arguments / state characterizing this {@code Stream},
     *         possibly null
     */
    @Nullable
    protected String doToString() {
        return null;
    }

    /**
     * Implementation method responsible of closing optional resources associated to this Stream.
     * The default implementation does nothing.
     * 
     * @throws Throwable
     *             in case of failure
     */
    protected void doClose() throws Throwable {
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    private static final class State {

        @Nullable
        List<Object> closeObjects = Lists.newArrayList();

        @Nullable
        ScheduledFuture<?> timeoutFuture;

        @Nullable
        Map<String, Object> properties;

        @Nullable
        Iterator<?> activeIterator;

        @Nullable
        Thread toHandlerThread;

        boolean available = true;

        boolean closed = false;

    }

    private abstract static class AbstractIterator<T> extends UnmodifiableIterator<T> implements
            Closeable {

        @Nullable
        private T next;

        @Override
        public final boolean hasNext() {
            if (this.next == null) {
                this.next = advance();
            }
            return this.next != null;
        }

        @Override
        public final T next() {
            if (this.next == null) {
                final T result = advance();
                if (result != null) {
                    return result;
                }
                throw new NoSuchElementException();
            } else {
                final T result = this.next;
                this.next = null;
                return result;
            }
        }

        @Override
        public void close() throws IOException {
        }

        protected abstract T advance();

    }

    private static final class CheckedIterator<T> extends UnmodifiableIterator<T> {

        private final Iterator<T> iterator;

        private final Stream<T> stream;

        private final State state;

        CheckedIterator(final Iterator<T> iterator, final Stream<T> stream) {
            this.iterator = iterator;
            this.stream = stream;
            this.state = stream.state;
        }

        @Override
        public boolean hasNext() {
            checkState();
            boolean result = false;
            try {
                result = this.iterator.hasNext();
            } finally {
                if (!result) {
                    this.stream.close();
                }
            }
            return result;
        }

        @Override
        public T next() {
            checkState();
            try {
                return this.iterator.next();
            } catch (final Throwable ex) {
                this.stream.close();
                throw Throwables.propagate(ex);
            }
        }

        private void checkState() {
            boolean closed;
            synchronized (this.state) {
                closed = this.state.closed;
            }
            Preconditions.checkState(!closed, "Stream has been closed");
        }

    }

    private static final class ToHandlerIterator<T> extends AbstractIterator<T> implements
            Handler<T>, Runnable {

        private final Stream<T> stream;

        private final BlockingQueue<Object> queue;

        private Future<?> future;

        ToHandlerIterator(final Stream<T> stream) {
            this.stream = stream;
            this.queue = new ArrayBlockingQueue<Object>(1024);
            this.future = null;
        }

        public void submit() {
            this.future = Data.getExecutor().submit(this);
        }

        @Override
        public void run() {
            try {
                this.stream.doToHandler(this);
            } catch (final Throwable ex) {
                putUninterruptibly(ex);
                putUninterruptibly(EOF);
            }
        }

        @Override
        public void handle(final T element) {
            try {
                this.queue.put(element == null ? EOF : element);
            } catch (final InterruptedException ex) {
                putUninterruptibly(ex);
                putUninterruptibly(EOF);
                Thread.currentThread().interrupt(); // restore interruption status
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected T advance() {
            try {
                final Object element = this.queue.take();
                if (element == EOF) {
                    return null;
                } else if (element instanceof Throwable) {
                    throw Throwables.propagate((Throwable) element);
                } else {
                    return (T) element;
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt(); // restore interruption status
                throw new RuntimeException("Interrupted while waiting for next element", ex);
            }
        }

        @Override
        public void close() {
            if (this.future != null) {
                this.future.cancel(true);
            }
        }

        private void putUninterruptibly(final Object element) {
            while (true) {
                try {
                    this.queue.put(element);
                    return;
                } catch (final InterruptedException ex) {
                    // ignore
                }
            }
        }

    }

    // STREAM IMPLEMENTATIONS

    private static final class EmptyStream<T> extends Stream<T> {

        @Override
        protected Iterator<T> doIterator() {
            return Iterators.emptyIterator();
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            handler.handle(null);
        }

    }

    private static final class SingletonStream<T> extends Stream<T> {

        private T element;

        SingletonStream(final T element) {
            this.element = Preconditions.checkNotNull(element);
        }

        @Override
        protected Iterator<T> doIterator() {
            return Iterators.singletonIterator(this.element);
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            handler.handle(this.element);
            handler.handle(null);
        }

        @Override
        protected void doClose() throws Throwable {
            this.element = null;
        }
    }

    private static final class IterableStream<T> extends Stream<T> {

        private Iterable<? extends T> iterable;

        IterableStream(final Iterable<? extends T> iterable) {
            this.iterable = Preconditions.checkNotNull(iterable);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Iterator<T> doIterator() throws Throwable {
            return ((Iterable<T>) this.iterable).iterator();
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            for (final T element : this.iterable) {
                handler.handle(element);
            }
            handler.handle(null);
        }

        @Override
        protected void doClose() throws Throwable {
            if (this.iterable instanceof Closeable) {
                ((Closeable) this.iterable).close();
            }
            this.iterable = null;
        }

    }

    private static final class IteratorStream<T> extends Stream<T> {

        private Iterator<? extends T> iterator;

        IteratorStream(final Iterator<? extends T> iterator) {
            this.iterator = Preconditions.checkNotNull(iterator);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Iterator<T> doIterator() {
            return (Iterator<T>) this.iterator;
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            while (this.iterator.hasNext()) {
                if (Thread.interrupted()) {
                    return;
                }
                final T element = this.iterator.next();
                handler.handle(element);
            }
            handler.handle(null);
        }

        @Override
        protected void doClose() throws Throwable {
            if (this.iterator instanceof Closeable) {
                ((Closeable) this.iterator).close();
            }
            this.iterator = null;
        }

    }

    private static final class IterationStream<T> extends Stream<T> {

        private Iteration<? extends T, ?> iteration;

        IterationStream(final Iteration<? extends T, ?> iteration) {
            this.iteration = Preconditions.checkNotNull(iteration);
        }

        @Override
        protected Iterator<T> doIterator() {
            return new AbstractIterator<T>() {

                @Override
                protected T advance() {
                    try {
                        if (IterationStream.this.iteration.hasNext()) {
                            return IterationStream.this.iteration.next();
                        } else {
                            return null;
                        }
                    } catch (final Throwable ex) {
                        throw Throwables.propagate(ex);
                    }
                }

            };
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            while (this.iteration.hasNext()) {
                if (Thread.interrupted()) {
                    return;
                }
                final T element = this.iteration.next();
                handler.handle(element);
            }
            handler.handle(null);
        }

        @Override
        protected void doClose() throws Throwable {
            if (this.iteration instanceof CloseableIteration<?, ?>) {
                ((CloseableIteration<? extends T, ?>) this.iteration).close();
            }
            this.iteration = null;
        }

    }

    private abstract static class DelegatingStream<I, O> extends Stream<O> {

        final Stream<I> delegate;

        DelegatingStream(final Stream<I> delegate) {
            super(delegate.state);
            this.delegate = delegate;
        }

        @Override
        void toStringHelper(final StringBuilder builder) {
            super.toStringHelper(builder);
            builder.append(" (");
            this.delegate.toStringHelper(builder);
            builder.append(")");
        }

    }

    private static final class ConcatStream<I extends Iterable<? extends O>, O> extends
            DelegatingStream<I, O> {

        ConcatStream(final Stream<I> delegate) {
            super(delegate);
        }

        @Override
        protected Iterator<O> doIterator() throws Throwable {
            final Iterator<I> streamIterator = this.delegate.doIterator();
            final Iterator<O> elementIterator = new AbstractIterator<O>() {

                private Stream<? extends O> stream;

                private Iterator<? extends O> iterator;

                @Override
                protected O advance() {
                    while (this.iterator == null || !this.iterator.hasNext()) {
                        if (this.stream != null) {
                            this.stream.close();
                        }
                        if (!streamIterator.hasNext()) {
                            return null;
                        }
                        this.stream = create(streamIterator.next());
                        this.iterator = this.stream.iterator();
                    }
                    return this.iterator.next();
                }

                @Override
                public void close() {
                    if (this.stream != null) {
                        this.stream.close();
                    }
                }

            };
            onClose(elementIterator);
            return elementIterator;
        }

        @Override
        protected void doToHandler(final Handler<? super O> handler) throws Throwable {
            this.delegate.doToHandler(new Handler<I>() {

                @Override
                public void handle(final I iterable) throws Throwable {
                    if (iterable == null) {
                        handler.handle(null);
                    } else {
                        final AtomicBoolean eof = new AtomicBoolean(false);
                        create(iterable).toHandler(new Handler<O>() {

                            @Override
                            public void handle(final O element) throws Throwable {
                                if (element != null) {
                                    handler.handle(element);
                                } else {
                                    eof.set(true);
                                }
                            }

                        });
                        if (!eof.get()) { // halt streamStream.toHandler if interrupted
                            Thread.currentThread().interrupt();
                        }
                    }
                }

            });
        }

    }

    private abstract static class ProcessingStream<I, O> extends DelegatingStream<I, O> {

        final int parallelism;

        ProcessingStream(final Stream<I> delegate, final int parallelism) {
            super(delegate);
            this.parallelism = parallelism;
        }

        @Override
        protected final Iterator<O> doIterator() throws Throwable {
            if (this.parallelism <= 1) {
                return doIteratorSequential();
            } else {
                return doIteratorParallel();
            }
        }

        private Iterator<O> doIteratorSequential() throws Throwable {
            final Iterator<I> iterator = this.delegate.doIterator();
            return new AbstractIterator<O>() {

                @SuppressWarnings("unchecked")
                @Override
                protected O advance() {
                    while (iterator.hasNext()) {
                        final I element = iterator.next();
                        final Object transformed = process(element);
                        if (transformed == EOF) {
                            return null;
                        } else if (transformed != null) {
                            return (O) transformed;
                        }
                    }
                    return null;
                }

            };
        }

        private Iterator<O> doIteratorParallel() throws Throwable {
            final Iterator<I> iterator = this.delegate.doIterator();
            final List<Future<Object>> queue = Lists.newLinkedList();
            final Iterator<O> result = new AbstractIterator<O>() {

                @SuppressWarnings("unchecked")
                @Override
                protected O advance() {
                    while (true) {
                        while (queue.size() < ProcessingStream.this.parallelism
                                && iterator.hasNext()) {
                            offer(queue, iterator.next());
                        }
                        if (queue.isEmpty()) {
                            return null;
                        }
                        final Object output = take(queue);
                        if (output == EOF) {
                            return null;
                        } else if (output != null) {
                            return (O) output;
                        }
                    }
                }

                @Override
                public void close() {
                    for (final Future<Object> future : queue) {
                        try {
                            future.cancel(true);
                        } catch (final Exception ex) {
                            // ignore
                        }
                    }
                }

            };
            onClose(result);
            return result;
        }

        @Override
        protected final void doToHandler(final Handler<? super O> handler) throws Throwable {
            if (this.parallelism <= 1) {
                doToHandlerSequential(handler);
            } else {
                doToHandlerParallel(handler);
            }
        }

        private void doToHandlerSequential(final Handler<? super O> handler) throws Throwable {
            this.delegate.doToHandler(new Handler<I>() {

                private boolean done = false;

                @SuppressWarnings("unchecked")
                @Override
                public void handle(final I element) throws Throwable {
                    if (!this.done) {
                        if (element == null) {
                            handler.handle(null);
                            this.done = true;
                        } else {
                            final Object transformed = process(element);
                            if (transformed == EOF) {
                                handler.handle(null);
                                Thread.currentThread().interrupt();
                                this.done = true;
                            } else if (transformed != null) {
                                handler.handle((O) transformed);
                            }
                        }
                    }
                }

            });
        }

        private void doToHandlerParallel(final Handler<? super O> handler) throws Throwable {
            final List<Future<Object>> queue = Lists.newLinkedList();
            try {
                this.delegate.doToHandler(new Handler<I>() {

                    private boolean done = false;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void handle(final I element) throws Throwable {
                        if (!this.done) {
                            if (element == null) {
                                while (!this.done && !queue.isEmpty()) {
                                    final Object output = take(queue);
                                    if (output == EOF) {
                                        break;
                                    } else if (output != null) {
                                        handler.handle((O) output);
                                    }
                                }
                                handler.handle(null);
                                this.done = true;
                            } else {
                                if (queue.size() == ProcessingStream.this.parallelism) {
                                    final Object output = take(queue);
                                    if (output == EOF) {
                                        handler.handle(null);
                                        Thread.currentThread().interrupt();
                                        this.done = true;
                                    } else if (output != null) {
                                        handler.handle((O) output);
                                    }
                                }
                                if (!this.done) {
                                    offer(queue, element);
                                }
                            }
                        }
                    }

                });
            } finally {
                for (final Future<Object> future : queue) {
                    try {
                        future.cancel(true);
                    } catch (final Exception ex) {
                        // ignore
                    }
                }
            }
        }

        private Object take(final List<Future<Object>> queue) {
            return Futures.get(queue.remove(0), RuntimeException.class);
        }

        private void offer(final List<Future<Object>> queue, final I element) {
            queue.add(Data.getExecutor().submit(new Callable<Object>() {

                @Override
                public Object call() {
                    return process(element);
                }

            }));
        }

        protected abstract Object process(I element);

    }

    private static final class FilterStream<T> extends ProcessingStream<T, T> {

        private final Predicate<? super T> predicate;

        FilterStream(final Stream<T> delegate, final int parallelism,
                final Predicate<? super T> predicate) {
            super(delegate, parallelism);
            this.predicate = Preconditions.checkNotNull(predicate);
        }

        @Override
        protected Object process(final T element) {
            return this.predicate.apply(element) ? element : null;
        }

        @Override
        protected String doToString() {
            return this.predicate + ", " + this.parallelism;
        }

    }

    private static final class TransformElementStream<I, O> extends ProcessingStream<I, O> {

        private final Function<? super I, ? extends O> function;

        TransformElementStream(final Stream<I> delegate, final int parallelism,
                final Function<? super I, ? extends O> function) {
            super(delegate, parallelism);
            this.function = Preconditions.checkNotNull(function);
        }

        @Override
        protected Object process(final I element) {
            return this.function.apply(element);
        }

        @Override
        protected String doToString() {
            return this.function + ", " + this.parallelism;
        }

    }

    private static final class TransformSequenceStream<I, O> extends DelegatingStream<I, O> {

        private final Function<Iterator<I>, Iterator<O>> iteratorFunction;

        private final Function<Handler<O>, Handler<I>> handlerFunction;

        TransformSequenceStream(final Stream<I> delegate,
                @Nullable final Function<Iterator<I>, Iterator<O>> iteratorFunction,
                @Nullable final Function<Handler<O>, Handler<I>> handlerFunction) {
            super(delegate);
            Preconditions.checkArgument(iteratorFunction != null || handlerFunction != null,
                    "At least one function must be supplied");
            this.iteratorFunction = iteratorFunction;
            this.handlerFunction = handlerFunction;
        }

        @Override
        protected Iterator<O> doIterator() throws Throwable {
            if (this.iteratorFunction != null) {
                return this.iteratorFunction.apply(this.delegate.doIterator());
            } else {
                return super.doIterator(); // delegates to doToHandler
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doToHandler(final Handler<? super O> handler) throws Throwable {
            if (this.handlerFunction != null) {
                this.delegate.doToHandler(this.handlerFunction.apply((Handler<O>) handler));
            } else {
                super.doToHandler(handler); // delegates to doIterator
            }
        }

        @Override
        protected String doToString() {
            return this.iteratorFunction == null ? this.handlerFunction.toString()
                    : this.handlerFunction == null ? this.iteratorFunction.toString()
                            : this.iteratorFunction.toString() + ", "
                                    + this.handlerFunction.toString();
        }

    }

    private static final class TransformPathStream<I, O> extends ProcessingStream<I, List<O>> {

        private final Class<O> type;

        private final boolean lenient;

        private final Object[] path;

        TransformPathStream(final Stream<I> delegate, final Class<O> type, final boolean lenient,
                final Object[] path) {
            super(delegate, 0);
            this.type = Preconditions.checkNotNull(type);
            this.lenient = lenient;
            this.path = Preconditions.checkNotNull(path);
        }

        @Override
        protected Object process(final I element) {
            final List<O> elements = Lists.newArrayList();
            path(element, 0, elements);
            return elements;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private void path(final Object object, final int index, final List<O> result) {
            if (object == null) {
                return;
            } else if (object instanceof Iterable && !(object instanceof BindingSet)) {
                for (final Object element : (Iterable) object) {
                    path(element, index, result);
                }
            } else if (object instanceof Iterator) {
                final Iterator<?> iterator = (Iterator<?>) object;
                while (iterator.hasNext()) {
                    path(iterator.next(), index, result);
                }
            } else if (object.getClass().isArray()) {
                final int length = Array.getLength(object);
                for (int i = 0; i < length; ++i) {
                    path(Array.get(object, i), index, result);
                }
            } else if (index == this.path.length) {
                final O element = this.lenient ? Data.convert(object, this.type, null) : //
                        Data.convert(object, this.type);
                if (element != null) {
                    result.add(element);
                }
            } else {
                final Object key = this.path[index];
                if (object instanceof Record) {
                    if (key instanceof URI) {
                        path(((Record) object).get((URI) key), index + 1, result);
                    }
                } else if (object instanceof BindingSet) {
                    if (key instanceof String) {
                        path(((BindingSet) object).getValue((String) key), index + 1, result);
                    }
                } else if (object instanceof Map) {
                    path(((Map<Object, Object>) object).get(key), index + 1, result);
                } else if (object instanceof Multimap) {
                    path(((Multimap<Object, Object>) object).get(this.path), index + 1, result);
                }
            }
        }

    }

    private static final class DistinctStream<T> extends ProcessingStream<T, T> {

        private final Set<T> seen;

        DistinctStream(final Stream<T> delegate) {
            super(delegate, 0); // pure sequential processing
            this.seen = Sets.newHashSet();
        }

        @Override
        protected Object process(final T element) {
            return this.seen.add(element) ? element : null; // could be improved
        }

    }

    private static final class SliceStream<T> extends ProcessingStream<T, T> {

        private final long startIndex;

        private final long endIndex;

        private long index;

        SliceStream(final Stream<T> delegate, final long offset, final long limit) {
            super(delegate, 0); // no parallel evaluation
            Preconditions.checkArgument(offset >= 0, "Negative offset: {}", limit);
            Preconditions.checkArgument(limit >= 0, "Negative limit: {}", limit);
            this.startIndex = offset;
            this.endIndex = offset + limit;
            this.index = 0;
        }

        @Override
        protected Object process(final T element) {
            Object result = null;
            if (this.index >= this.endIndex) {
                result = EOF;
            } else if (this.index >= this.startIndex) {
                result = element;
            }
            ++this.index;
            return result;
        }

    }

    private static final class ChunkStream<T> extends DelegatingStream<T, List<T>> {

        private final int chunkSize;

        ChunkStream(final Stream<T> delegate, final int chunkSize) {
            super(delegate);
            Preconditions.checkArgument(chunkSize > 0, "Invalid chunk size: %d", chunkSize);
            this.chunkSize = chunkSize;
        }

        @Override
        protected Iterator<List<T>> doIterator() throws Throwable {
            final Iterator<T> iterator = this.delegate.doIterator();
            return new AbstractIterator<List<T>>() {

                private final Object[] chunk = new Object[ChunkStream.this.chunkSize];

                @SuppressWarnings("unchecked")
                @Override
                protected List<T> advance() {
                    int index = 0;
                    for (; index < ChunkStream.this.chunkSize && iterator.hasNext(); ++index) {
                        this.chunk[index] = iterator.next();
                    }
                    return index == 0 ? null : (List<T>) ImmutableList.copyOf(this.chunk);
                }

            };
        }

        @Override
        protected void doToHandler(final Handler<? super List<T>> handler) throws Throwable {
            this.delegate.doToHandler(new Handler<T>() {

                private final List<T> chunk = Lists.newArrayList();

                @Override
                public void handle(final T element) throws Throwable {
                    if (element == null) {
                        if (!this.chunk.isEmpty()) {
                            handler.handle(ImmutableList.copyOf(this.chunk));
                        }
                        handler.handle(null);
                    } else {
                        this.chunk.add(element);
                        if (this.chunk.size() == ChunkStream.this.chunkSize) {
                            handler.handle(ImmutableList.copyOf(this.chunk));
                            this.chunk.clear();
                        }
                    }
                }

            });
        }

        @Override
        protected String doToString() {
            return Integer.toString(this.chunkSize);
        }

    }

    private static final class TrackStream<T> extends DelegatingStream<T, T> {

        private final AtomicLong counter;

        private final AtomicBoolean eof;

        public TrackStream(final Stream<T> delegate, @Nullable final AtomicLong counter,
                @Nullable final AtomicBoolean eof) {
            super(delegate);
            this.counter = counter != null ? counter : new AtomicLong();
            this.eof = eof != null ? eof : new AtomicBoolean();
            this.counter.set(0L);
            this.eof.set(false);
        }

        @Override
        protected Iterator<T> doIterator() throws Throwable {
            final Iterator<T> iterator = this.delegate.doIterator();
            return new UnmodifiableIterator<T>() {

                private long count = 0L;

                @Override
                public boolean hasNext() {
                    final boolean result = iterator.hasNext();
                    TrackStream.this.eof.set(result);
                    return result;
                }

                @Override
                public T next() {
                    final T next = iterator.next();
                    TrackStream.this.counter.set(++this.count);
                    return next;
                }

            };
        }

        @Override
        protected void doToHandler(final Handler<? super T> handler) throws Throwable {
            this.delegate.doToHandler(new Handler<T>() {

                private long count = 0L;

                @Override
                public void handle(final T element) throws Throwable {
                    if (element != null) {
                        TrackStream.this.counter.set(++this.count);
                        handler.handle(element);
                    } else {
                        TrackStream.this.eof.set(true);
                        handler.handle(null);
                    }
                }

            });
        }

    }

    private static final class PathComparator implements Comparator<Object> {

        private final Class<? extends Comparable<?>> type;

        private final Object[] path;

        private final boolean lenient;

        PathComparator(final Class<? extends Comparable<?>> type, final boolean lenient,
                final Object... path) {
            this.type = Preconditions.checkNotNull(type);
            this.lenient = lenient;
            this.path = path.clone();
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public int compare(final Object first, final Object second) {
            final Comparable firstKey = path(first, 0);
            final Comparable secondKey = path(second, 0);
            if (firstKey == null) {
                return secondKey == null ? 0 : 1;
            } else {
                return secondKey == null ? -1 : firstKey.compareTo(secondKey);
            }
        }

        @SuppressWarnings({ "unchecked" })
        private Comparable<?> path(final Object object, final int index) {
            if (object == null) {
                return null;
            } else if (object instanceof Iterable && !(object instanceof BindingSet)) {
                final Iterator<?> iterator = ((Iterable<?>) object).iterator();
                return iterator.hasNext() ? path(iterator.next(), index) : null;
            } else if (object instanceof Iterator) {
                final Iterator<?> iterator = (Iterator<?>) object;
                return iterator.hasNext() ? path(iterator.next(), index) : null;
            } else if (object.getClass().isArray()) {
                final int length = Array.getLength(object);
                return length > 0 ? path(Array.get(object, 0), index) : null;
            } else if (index == this.path.length) {
                return this.lenient ? Data.convert(object, this.type, null) : //
                        Data.convert(object, this.type);
            } else {
                final Object key = this.path[index];
                if (object instanceof Record) {
                    return key instanceof URI ? path(((Record) object).get((URI) key), index + 1)
                            : null;
                } else if (object instanceof BindingSet) {
                    return key instanceof String ? path(
                            ((BindingSet) object).getValue((String) key), index + 1) : null;
                } else if (object instanceof Map) {
                    return path(((Map<Object, Object>) object).get(key), index + 1);
                } else if (object instanceof Multimap) {
                    return path(((Multimap<Object, Object>) object).get(this.path), index + 1);
                }
                return null;
            }
        }

    }

}
