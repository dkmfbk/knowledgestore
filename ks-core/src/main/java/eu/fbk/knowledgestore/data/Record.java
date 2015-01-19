package eu.fbk.knowledgestore.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * A record structure characterized by an ID and a generic set of properties.
 * <p>
 * A record is a structured identified by an {@link URI} ID and having a number of key-value
 * properties, where the key is a {@code URI} and the value is a non-empty list of objects, which
 * can be other {@code Record}s, {@link URI}s, {@link BNode}s, {@link Literal}s or
 * {@link Statement}s. Records are used to carry the data of resources, representations, mentions,
 * entities, axioms, contexts and of any structured property value.
 * </p>
 * <p>
 * Records are created via factory methods {@code create()}:
 * </p>
 * <ul>
 * <li>method {@link #create()} creates an empty record, without ID and properties;</li>
 * <li>method {@link #create(URI, URI...)} creates a new record with the ID and the types (values
 * of property {@code rdf:type} supplied;</li>
 * <li>method {@link #create(Record, boolean)} creates a copy of a supplied method, possibly
 * performing deep-cloning of its properties.</li>
 * </ul>
 * <p>
 * Record equality ({@link #equals(Object)}, {@link #hashCode()}) is defined in terms of the
 * record ID only, while {@link #toString()} emits only the record type and ID. Beware that the ID
 * can change during a {@code Record} lifetime (via method {@link #setID(URI)}): this provides for
 * increased flexibility, but pay attention not to change the ID when storing records in indexed
 * data structure such as {@code Set}s and {@code Map}s, which rely on {@code hashCode()} and
 * {@code equals()} to produce constant outcomes. Additional method {@link #toString(boolean)}
 * allows for emitting a complete record representation including its properties, while equality
 * of (selected) properties in different records can be checked by comparing the respective
 * hashes, computed via {@link #hash(URI...)}; the same {@code hash()} method can help in creating
 * syntetic IDs based on the values of some properties (e.g., following a pattern
 * {@code PREFIX + record.hash(p1, p2, ...)}.
 * </p>
 * <p>
 * Access to and manipulation of properties is performed as follows:
 * <ul>
 * <li><b>Listing available properties</b>. Method {@link #getProperties()} returns the list of
 * properties having some value for a record instance.</li>
 * <li><b>Reading properties</b>. The main method is {@link #get(URI)}, which is complemented by a
 * number of auxiliary methods for ease of use. They are described below:
 * <ul>
 * <li>{@link #get(URI)}, {@link #get(URI, Class)} and {@link #get(URI, Class, List)} allow
 * retrieving all the values of a specific property, either as a list of objects or converted to a
 * specific target class; the last methed supports also the specification of a default value,
 * which is returned if the property has no values or if conversion fails.</li>
 * <li>{@link #getUnique(URI)}, {@link #getUnique(URI, Class)},
 * {@link #getUnique(URI, Class, Object)} allow retrieving the unique value of a property, either
 * as an object or converted to a specific class; unless a default value is specified, the methods
 * fail in case multiple values are associated to the property, thus helping enforcing the
 * uniqueness expectation.</li>
 * <li>{@link #isTrue(URI)}, {@link #isFalse(URI)} are convenience methods that can be used for
 * boolean properties; they fail if used on properties having multiple or non-boolean values.</li>
 * <li>{@link #isNull(URI)}, {@link #isUnique(URI)} are convenience methods that can be used to
 * test whether a property has at least or at most one value.</li>
 * <li>{@link #count(URI)} is a convenience method for counting the values of a property; it may
 * be faster than using {@code get()}.</li>
 * </ul>
 * </li>
 * <li><b>Modifying properties</b>. Two types of methods are offered:
 * <ul>
 * <li>modification of individual properties is done via {@link #set(URI, Object, Object...)},
 * {@link #add(URI, Object, Object...)} and {@link #remove(URI, Object, Object...)}, that allow,
 * respectively, to set all the values of a property, to add some new values to a property or to
 * remove existing values from the values of a property. For ease of use, these methods accept (at
 * least) an argument object which can be a {@code Record}, {@code URI}, {@code BNode},
 * {@code Statement}, {@code Literal}, an object convertible to {@code Literal} or any array or
 * iterable of the former types. A list of values is extracted from the supplied objects, and used
 * for modifying the values of the property.</li>
 * <li>modification of multiple properties at once is done via {@link #clear(URI...)} and
 * {@link #retain(URI...)}, which remove all the properties respectively matching or not matching
 * a supplied list, allowing as a special case (no properties specified) to remove all the
 * properties of a record instance.</li>
 * </ul>
 * </ul>
 * </p>
 * <p>
 * Instances of this interface are thread safe. Cloning of record instances (via
 * {@link #create(Record, boolean)}) is supported and is a relatively inexpensive operation; a
 * copy-on-write approach is adopted to reduce the memory usage of cloned objects, which share
 * their state with the source object as long as one of the two is changed.
 * </p>
 */
public final class Record implements Serializable, Comparable<Record> {

    private static final long serialVersionUID = 1L;

    private static final int LENGTH_INCREMENT = 8;

    private static final int OFFSET_OF_ID = 0;

    private static final int OFFSET_OF_SHARED = 1;

    private static final int OFFSET_OF_PROPERTIES = 2;

    private static final ThreadLocal<Integer> INDENT_LEVEL = new ThreadLocal<Integer>();

    private static final String INDENT_STRING = "  ";

    private Object[] state;

    private Record(final URI id) {
        this.state = new Object[OFFSET_OF_PROPERTIES + LENGTH_INCREMENT];
        this.state[OFFSET_OF_ID] = id;
        this.state[OFFSET_OF_SHARED] = Boolean.FALSE;
    }

    private Record(final Record record, final boolean deepClone) {
        synchronized (record) {
            Object[] state = record.state;
            if (deepClone) {
                state = cloneRecursively(state);
            }
            if (state != record.state) {
                state[OFFSET_OF_SHARED] = Boolean.FALSE;
            } else if (state[OFFSET_OF_SHARED] == Boolean.FALSE) {
                state[OFFSET_OF_SHARED] = Boolean.TRUE;
            }
            this.state = state;
        }
    }

    private static Object[] cloneRecursively(final Object[] array) {
        Object[] result = array;
        for (int i = 0; i < array.length; ++i) {
            final Object element = array[i];
            Object newElement = element;
            if (element instanceof Record) {
                newElement = new Record((Record) element, true);
            } else if (element instanceof Object[]) {
                newElement = cloneRecursively((Object[]) element);
            }
            if (newElement != element) {
                if (result == array) {
                    result = array.clone();
                }
                result[i] = newElement;
            }
        }
        return result;
    }

    private static Object encode(final Object object) {
        // the node unchanged is stored; this may change in order to save some memory
        return object;
    }

    private static <T> T decode(final Object object, final Class<T> clazz) {
        return Data.convert(object, clazz);
    }

    @Nullable
    private URI doGetID() {
        return (URI) this.state[OFFSET_OF_ID];
    }

    private void doSetID(@Nullable final URI id) {
        if (!Objects.equal(id, this.state[OFFSET_OF_ID])) {
            if ((Boolean) this.state[OFFSET_OF_SHARED]) {
                this.state = this.state.clone();
            }
            this.state[OFFSET_OF_ID] = id;
        }
    }

    private List<URI> doGetProperties() {
        final int capacity = this.state.length / 2;
        final List<URI> properties = Lists.newArrayListWithCapacity(capacity);
        for (int i = OFFSET_OF_PROPERTIES; i < this.state.length; i += 2) {
            final URI property = (URI) this.state[i];
            if (property != null) {
                properties.add(property);
            }
        }
        return properties;
    }

    private int doCount(final URI property) {
        final int length = this.state.length;
        for (int i = OFFSET_OF_PROPERTIES; i < length; i += 2) {
            if (property.equals(this.state[i])) {
                final Object object = this.state[i + 1];
                if (object instanceof Object[]) {
                    return ((Object[]) object).length;
                } else {
                    return 1;
                }
            }
        }
        return 0;
    }

    @Nullable
    private <T> Object doGet(final URI property, final Class<T> clazz) {
        final int length = this.state.length;
        for (int i = OFFSET_OF_PROPERTIES; i < length; i += 2) {
            if (property.equals(this.state[i])) {
                final Object object = this.state[i + 1];
                if (object instanceof Object[]) {
                    final Object[] array = (Object[]) object;
                    final List<T> list = Lists.newArrayListWithCapacity(array.length);
                    for (final Object element : array) {
                        list.add(decode(element, clazz));
                    }
                    return list;
                } else {
                    return decode(object, clazz);
                }
            }
        }
        return null;
    }

    private void doSet(final URI property, final Collection<Object> nodes) {
        if ((Boolean) this.state[OFFSET_OF_SHARED]) {
            this.state = this.state.clone();
            this.state[OFFSET_OF_SHARED] = Boolean.FALSE;
        }
        final int length = this.state.length;
        if (nodes.isEmpty()) {
            for (int i = OFFSET_OF_PROPERTIES; i < length; i += 2) {
                if (property.equals(this.state[i])) {
                    this.state[i] = null;
                    this.state[i + 1] = null;
                    return;
                }
            }
            return;
        }
        final Object value;
        final int size = nodes.size();
        if (size == 1) {
            value = encode(Iterables.get(nodes, 0));
        } else {
            final Object[] array = new Object[size];
            int index = 0;
            for (final Object node : nodes) {
                array[index++] = encode(node);
            }
            value = array;
        }
        int nullIndex = -1;
        for (int i = OFFSET_OF_PROPERTIES; i < length; i += 2) {
            if (this.state[i] == null) {
                if (nullIndex < 0) {
                    nullIndex = i;
                }
            } else if (property.equals(this.state[i])) {
                this.state[i + 1] = value;
                return;
            }
        }
        if (nullIndex >= 0) {
            this.state[nullIndex] = property;
            this.state[nullIndex + 1] = value;
        } else {
            final Object[] oldState = this.state;
            this.state = new Object[length + LENGTH_INCREMENT];
            System.arraycopy(oldState, 0, this.state, 0, length);
            this.state[length] = property;
            this.state[length + 1] = value;
        }
    }

    /**
     * Creates a new record with no properties and ID assigned.
     * 
     * @return the created record
     */
    public static Record create() {
        return new Record(null);
    }

    /**
     * Creates a new record with the ID and the types specified (property {@code rdf:type}), and
     * no additional properties.
     * 
     * @param id
     *            the ID of the new record, possibly null in order not to assign it
     * @param types
     *            the types of the record, assigned to property {@code rdf:type}
     * @return the created record
     */
    public static Record create(final URI id, final URI... types) {
        final Record record = new Record(id);
        if (types.length > 0) {
            record.set(RDF.TYPE, types);
        }
        return record;
    }

    /**
     * Creates a new record having the same ID and properties of the supplied record, possibly
     * performing a deep-cloning (copy constructor). The difference between shallow- and
     * deep-cloning lies in the handling of property values of {@code Record} type, which are
     * shared by the source and cloned object in case of shallow-cloning, and cloned themselves in
     * case of deep-cloning.
     * 
     * @param record
     *            the reference record to clone
     * @param deepClone
     *            true to perform a deep-cloning, false to perform a shallow-cloning
     * @return the created record clone
     */
    public static Record create(final Record record, final boolean deepClone) {
        return new Record(record, deepClone);
    }

    /**
     * Returns the ID of this record.
     * 
     * @return the ID of this record, possibly null if not previously assigned
     */
    @Nullable
    public synchronized URI getID() {
        return doGetID();
    }

    /**
     * Sets the ID of this record.
     * 
     * @param id
     *            the new ID of this record or null to clear it
     * @return this record object, for call chaining
     */
    public synchronized Record setID(@Nullable final URI id) {
        doSetID(id);
        return this;
    }

    /**
     * Returns the system type for this record, i.e., the {@code rdf:type} URI under the
     * {@code ks:} namespace, if any.
     * 
     * @return the system type or null if not set
     * @throws IllegalArgumentException
     *             in case multiple system types are bound to the record
     */
    @Nullable
    public synchronized URI getSystemType() throws IllegalArgumentException {
        URI result = null;
        for (final URI type : get(RDF.TYPE, URI.class)) {
            if (type.getNamespace().equals(KS.NAMESPACE)) {
                Preconditions.checkArgument(result == null, "Multiple system types: " + result
                        + ", " + type);
                result = type;
            }
        }
        return result;
    }

    /**
     * Returns all the properties currently defined for this record.
     * 
     * @return an immutable list with the properties currently defined for this record, without
     *         repetitions and in no particular order
     */
    public synchronized List<URI> getProperties() {
        return doGetProperties();
    }

    /**
     * Determines whether the property specified is null, i.e., it has no value.
     * 
     * @param property
     *            the property to read
     * @return true if the property has no value
     */
    public synchronized boolean isNull(final URI property) {
        return doCount(property) == 0;
    }

    /**
     * Determines whether the property specified has at most one value.
     * 
     * @param property
     *            the property to read
     * @return true if the property has at most value; false if it has multiple values
     */
    public synchronized boolean isUnique(final URI property) {
        return doCount(property) <= 1;
    }

    /**
     * Determines whether the property specified has been set to true. The method fails if the
     * property has multiple values or has a non-boolean type; if this behaviour is not desired,
     * use {@link #getUnique(URI, Class, Object)} specifying {@code Boolean.class} as the class
     * and an appropriate default value to be returned in case of failure.
     * 
     * @param property
     *            the property to read
     * @return true if the property is set to true; false if the property has no value or has been
     *         set to false
     * @throws IllegalStateException
     *             in case the property has multiple values
     * @throws IllegalArgumentException
     *             in case the property value is not of boolean type
     */
    public boolean isTrue(final URI property) throws IllegalStateException,
            IllegalArgumentException {
        final Boolean value = getUnique(property, Boolean.class);
        return value != null && value.booleanValue();
    }

    /**
     * Determines whether the property specified has been set to false. The method fails if the
     * property has multiple values or has a non-boolean type; if this behaviour is not desired,
     * use {@link #getUnique(URI, Class, Object)} specifying {@code Boolean.class} as the class
     * and an appropriate default value to be returned in case of failure.
     * 
     * @param property
     *            the property to read
     * @return true if the property is set to false; false if the property has no value or has
     *         been set to true
     * @throws IllegalStateException
     *             in case the property has multiple values
     * @throws IllegalArgumentException
     *             in case the property value is not of boolean type
     */
    public boolean isFalse(final URI property) throws IllegalStateException,
            IllegalArgumentException {
        final Boolean value = getUnique(property, Boolean.class);
        return value != null && !value.booleanValue();
    }

    /**
     * Returns the number of values assigned to the property specified. Calling this method may be
     * faster that using {@link #get(URI)}.
     * 
     * @param property
     *            the property
     * @return the number of values
     */
    public synchronized int count(final URI property) {
        return doCount(property);
    }

    /**
     * Returns the unique {@code Object} value of a property, or null if it has no value. Note
     * that this method fails if the property has multiple values; if this is not the desired
     * behaviour, use {@link #getUnique(URI, Class, Object)} supplying an appropriate type (could
     * be {@code Object.class}) and default value to be returned in case of failure.
     * 
     * @param property
     *            the property to read
     * @return the unique {@code Object} value of the property; null if it has no value
     * @throws IllegalStateException
     *             in case the property has multiple values
     */
    @Nullable
    public Object getUnique(final URI property) throws IllegalStateException {
        return getUnique(property, Object.class);
    }

    /**
     * Returns the unique value of the property converted to an instance of a certain class, or
     * null if the property has no value. Note that this method fails if the property has multiple
     * values or its unique value cannot be converted to the requested class; if this is not the
     * desired behavior, use {@link #getUnique(URI, Class, Object)} supplying an appropriate
     * default value to be returned in case of failure.
     * 
     * @param property
     *            the property to read
     * @param valueClass
     *            the class to convert the value to
     * @param <T>
     *            the type of result
     * @return the unique value of the property, converted to the class specified; null if the
     *         property has no value
     * @throws IllegalStateException
     *             in case the property has multiple values
     * @throws IllegalArgumentException
     *             in case the unique property value cannot be converted to the class specified
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T getUnique(final URI property, final Class<T> valueClass)
            throws IllegalStateException, IllegalArgumentException {
        final Object result;
        synchronized (this) {
            result = doGet(property, valueClass);
        }
        if (result == null) {
            return null;
        } else if (result instanceof List<?>) {
            final List<T> list = (List<T>) result;
            final StringBuilder builder = new StringBuilder("Expected one value for property ")
                    .append(property).append(", found ").append(list.size()).append(" values: ");
            for (int i = 0; i < Math.min(3, list.size()); ++i) {
                builder.append(i > 0 ? ", " : "").append(list.get(i));
            }
            builder.append(list.size() > 3 ? ", ..." : "");
            throw new IllegalStateException(builder.toString());
        } else {
            return (T) result;
        }
    }

    /**
     * Returns the unique value of the property converted to an instance of a certain class, or
     * the default value supplied in case of failure.
     * 
     * @param property
     *            the property to read
     * @param valueClass
     *            the class to convert the value to
     * @param defaultValue
     *            the default value to return in case the property has no value
     * @param <T>
     *            the type of result
     * @return the unique value of the property converted to the class specified, on success; the
     *         default value supplied in case the property has no value, has multiple values or
     *         its unique value cannot be converted to the class specified
     */
    @Nullable
    public <T> T getUnique(final URI property, final Class<T> valueClass,
            @Nullable final T defaultValue) {
        try {
            final T value = getUnique(property, valueClass);
            return value == null ? defaultValue : value;
        } catch (final IllegalStateException ex) {
            return defaultValue;
        } catch (final IllegalArgumentException ex) {
            return defaultValue;
        }
    }

    /**
     * Returns the {@code Object} values of the property specified.
     * 
     * @param property
     *            the property to read
     * @return an immutable list with the {@code Object} values of the property, without
     *         repetitions, in no particular order and possibly empty
     */
    public List<Object> get(final URI property) {
        return get(property, Object.class);
    }

    /**
     * Returns the values of the property converted to instances of a certain class. Note that
     * this method fails if conversion is not possible for one or more of the property values; if
     * this is not the desired behavior, use {@link #get(URI, Class, List)} specifying an
     * appropriate default value to be returned in case of conversion failure.
     * 
     * @param property
     *            the property to read
     * @param valueClass
     *            the class values have to be converted to
     * @param <T>
     *            the type of property values
     * @return an immutable list with the values of the property, converted to the class
     *         specified, possibly empty
     * @throws IllegalArgumentException
     *             in case one of the property values cannot be converted to the class specified
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> get(final URI property, final Class<T> valueClass)
            throws IllegalArgumentException {
        final Object result;
        synchronized (this) {
            result = doGet(property, valueClass);
        }
        if (result == null) {
            return ImmutableList.of();
        } else if (result instanceof List<?>) {
            return (List<T>) result;
        } else {
            return ImmutableList.of((T) result);
        }
    }

    /**
     * Returns the values of the property converted to instances of a certain class, or the
     * default value supplied in case of failure or if the property has no values.
     * 
     * @param property
     *            the property to read
     * @param valueClass
     *            the class values have to be converted to
     * @param defaultValue
     *            the default value to return in case conversion fails
     * @param <T>
     *            the type of property values
     * @return an immutable list with the values of the property, converted to the class specified
     *         and possibly empty, on success; the default value supplied in case the property has
     *         no value or conversion fails for some value
     */
    public <T> List<T> get(final URI property, final Class<T> valueClass,
            final List<T> defaultValue) {
        try {
            final List<T> values = get(property, valueClass);
            return values.isEmpty() ? defaultValue : values;
        } catch (final IllegalArgumentException ex) {
            return defaultValue;
        }
    }

    /**
     * Sets the values of the property specified. The method accepts one or more objects as the
     * values; these objects can be {@code Record}s, {@code URI}s, {@code BNode}s,
     * {@code Statement}s, {@code Literal}s, objects convertible to {@code Literal} or any array
     * or iterable of the former types. Setting a property to null has the effect of clearing it.
     * 
     * @param property
     *            the property to set
     * @param first
     *            the first value, array or iterable of values to set, possibly null
     * @param other
     *            additional values, arrays or iterables of values to set (if specified, will be
     *            merged with {@code first}).
     * @return this record object, for call chaining
     * @throws IllegalArgumentException
     *             if one of the supplied values has an unsupported type
     */
    public Record set(final URI property, @Nullable final Object first, final Object... other)
            throws IllegalArgumentException {
        Preconditions.checkNotNull(property);
        final Set<Object> values = Sets.<Object>newHashSet();
        Data.normalize(first, values);
        Data.normalize(other, values);
        synchronized (this) {
            doSet(property, values);
        }
        return this;
    }

    /**
     * Adds one or more values to the property specified. The method accepts one or more objects
     * as the values; these objects can be {@code Record}s, {@code URI}s, {@code BNode}s,
     * {@code Statement}s, {@code Literal}s, objects convertible to {@code Literal} or any array
     * or iterable of the former types.
     * 
     * @param property
     *            the property to modify
     * @param first
     *            the first value, array or iterable of values to add, possibly null
     * @param other
     *            additional values, arrays or iterables of values to set (if specified, will be
     *            merged with {@code first}).
     * @return this record object, for call chaining
     * @throws IllegalArgumentException
     *             if one of the supplied values has an unsupported type
     */
    public Record add(final URI property, @Nullable final Object first, final Object... other)
            throws IllegalArgumentException {
        Preconditions.checkNotNull(property);
        final List<Object> added = Lists.newArrayList();
        Data.normalize(first, added);
        Data.normalize(other, added);
        if (!Iterables.isEmpty(added)) {
            synchronized (this) {
                final Set<Object> values = Sets.newHashSet(get(property));
                final boolean changed = values.addAll(added);
                if (changed) {
                    doSet(property, values);
                }
            }
        }
        return this;
    }

    /**
     * Removes one or more values from the property specified. The method accepts one or more
     * objects as the values; these objects can be {@code Record}s, {@code URI}s, {@code BNode}s,
     * {@code Statement}s, {@code Literal}s, objects convertible to {@code Literal} or any array
     * or iterable of the former types.
     * 
     * @param property
     *            the property to modify
     * @param first
     *            the first value, array or iterable of values to remove, possibly null
     * @param other
     *            additional values, arrays or iterables of values to remove (if specified, will
     *            be merged with {@code first}).
     * @return this record object, for call chaining
     * @throws IllegalArgumentException
     *             if one of the supplied values has an unsupported type
     */
    public Record remove(final URI property, @Nullable final Object first, final Object... other)
            throws IllegalArgumentException {
        Preconditions.checkNotNull(property);
        final List<Object> removed = Lists.newArrayList();
        Data.normalize(first, removed);
        Data.normalize(other, removed);
        if (!removed.isEmpty()) {
            synchronized (this) {
                final Set<Object> values = Sets.newHashSet(get(property));
                final boolean changed = values.removeAll(removed);
                if (changed) {
                    doSet(property, values);
                }
            }
        }
        return this;
    }

    /**
     * Retains only the properties specified, clearing the remaining ones. Note that the ID is not
     * affected.
     * 
     * @param properties
     *            an array with the properties to retain, possibly empty (in which case all the
     *            stored properties will be cleared)
     * @return this record object, for call chaining
     */
    public synchronized Record retain(final URI... properties) {
        for (final URI property : doGetProperties()) {
            boolean retain = false;
            for (int i = 0; i < properties.length; ++i) {
                if (property.equals(properties[i])) {
                    retain = true;
                    break;
                }
            }
            if (!retain) {
                doSet(property, ImmutableSet.<Object>of());
            }
        }
        return this;
    }

    /**
     * Clears the properties specified, or all the stored properties if no property is specified.
     * Note that the ID is not affected.
     * 
     * @param properties
     *            an array with the properties to retain, possibly empty (in which case all the
     *            stored properties will be cleared)
     * @return this record object, for call chaining
     */
    public synchronized Record clear(final URI... properties) {
        final List<URI> propertiesToClear;
        if (properties == null || properties.length == 0) {
            propertiesToClear = doGetProperties();
        } else {
            propertiesToClear = Arrays.asList(properties);
        }
        for (final URI property : propertiesToClear) {
            doSet(property, ImmutableSet.<Object>of());
        }
        return this;
    }

    /**
     * {@inheritDoc} Comparison is based on the record IDs only.
     */
    @Override
    public int compareTo(final Record other) {
        final URI thisID = getID();
        final URI otherID = other.getID();
        if (thisID == null) {
            return otherID == null ? 0 : -1;
        } else {
            return otherID == null ? 1 : thisID.stringValue().compareTo(otherID.stringValue());
        }
    }

    /**
     * {@inheritDoc} Two records are equal if they have the same IDs.
     */
    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Record)) {
            return false;
        }
        final Record other = (Record) object;
        return Objects.equal(getID(), other.getID());
    }

    /**
     * {@inheritDoc} The returned hash code depends only on the record ID.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(getID());
    }

    /**
     * Computes a string-valued hash code of the properties specified, or of all the available
     * properties, if no URI is specified. Order of selected properties and order of values of
     * each property do not matter. A cryptographic hash function is used. Collision probability
     * is negligible. This method can be used to check whether two records have the same (subsets
     * of) properties, by computing and comparing the respective hashes.
     * 
     * @param properties
     *            the properties to hash.
     * @return the computed hash code
     */
    public synchronized String hash(final URI... properties) {
        final List<URI> propertiesToHash;
        if (properties == null || properties.length == 0) {
            propertiesToHash = doGetProperties();
        } else {
            propertiesToHash = Arrays.asList(properties);
        }
        final Hasher hasher = Hashing.md5().newHasher();
        for (final URI property : propertiesToHash) {
            final Object object = doGet(property, Object.class);
            @SuppressWarnings("unchecked")
            final Iterable<Object> nodes = object instanceof List<?> ? (List<Object>) object
                    : ImmutableList.of(object);
            for (final Object node : ((Ordering<Object>) Data.getTotalComparator())
                    .sortedCopy(nodes)) {
                // TODO: this is not efficient! add Node.toBytes
                hasher.putString(Data.toString(node, null, true), Charsets.UTF_16LE);
            }
            hasher.putByte((byte) 0);
        }
        final StringBuilder builder = new StringBuilder(16);
        final byte[] bytes = hasher.hash().asBytes();
        int max = 52;
        for (int i = 0; i < bytes.length; ++i) {
            final int n = (bytes[i] & 0x7F) % max;
            if (n < 26) {
                builder.append((char) (65 + n));
            } else if (n < 52) {
                builder.append((char) (71 + n));
            } else {
                builder.append((char) (n - 4));
            }
            max = 62;
        }
        return builder.toString();
    }

    /**
     * Returns a string representation of the record, optionally using the namespaces supplied and
     * emitting record properties. This method extends {@code #toString()}, optionally allowing to
     * emit also record properties and, recursively, properties of records nested in this record.
     * 
     * @param namespaces
     *            the prefix-to-namespace mappings to be used when emitting property and value
     *            URIs; if null, only non-abbreviated, full URIs will be emitted
     * @param includeProperties
     *            true if record properties should be emitted too
     * @return a string representation of the record, computed based on the
     *         {@code includeProperties} setting
     */
    public synchronized String toString(@Nullable final Map<String, String> namespaces,
            final boolean includeProperties) {
        final URI id = getID();
        final String base = "Record " + (id == null ? "<no id>" : Data.toString(id, namespaces));
        if (!includeProperties) {
            return base;
        }
        final Integer oldIndent = INDENT_LEVEL.get();
        try {
            final int indent = oldIndent == null ? 1 : oldIndent + 1;
            INDENT_LEVEL.set(indent + 1);
            final StringBuilder builder = new StringBuilder(base).append(" {");
            String propertySeparator = "\n";
            final Ordering<Object> ordering = Ordering.from(Data.getTotalComparator());
            for (final URI property : ordering.sortedCopy(doGetProperties())) {
                builder.append(propertySeparator).append(Strings.repeat(INDENT_STRING, indent));
                builder.append(Data.toString(property, namespaces));
                builder.append(" = ");
                final List<Object> values = ordering.sortedCopy(get(property));
                String valueSeparator = values.size() == 1 ? "" : "\n"
                        + Strings.repeat(INDENT_STRING, indent + 1);
                for (final Object value : values) {
                    builder.append(valueSeparator).append(Data.toString(value, namespaces, true));
                    valueSeparator = ",\n" + Strings.repeat(INDENT_STRING, indent + 1);
                }
                propertySeparator = ";\n";
            }
            builder.append(" }");
            return builder.toString();
        } finally {
            INDENT_LEVEL.set(oldIndent);
        }
    }

    /**
     * {@inheritDoc} The returned string contains only the ID of the record.
     */
    @Override
    public String toString() {
        return toString(null, false);
    }

    /**
     * Performs record-to-RDF encoding by converting a stream of records in a stream of RDF
     * statements. Parameter {@code types} specify additional types to be added to encoded
     * records. Type information may be set to null (e.g., because unknown at the time the method
     * is called): in this case, it will be read from metadata attribute {@code "types"} attached
     * to the stream; reading will happen just before decoding will take place, i.e., when a
     * terminal stream operation will be called.
     * 
     * @param stream
     *            the stream of records to encode.
     * @param types
     *            the types to be added to each record of the stream, null if to be read from
     *            stream metadata
     * @return the resulting stream of statements
     */
    @SuppressWarnings("unchecked")
    public static Stream<Statement> encode(final Stream<? extends Record> stream,
            @Nullable final Iterable<? extends URI> types) {
        Preconditions.checkNotNull(stream);
        if (types != null) {
            stream.setProperty("types", types);
        }
        final Stream<Record> records = (Stream<Record>) stream;
        return records.transform(null, new Function<Handler<Statement>, Handler<Record>>() {

            @Override
            public Handler<Record> apply(final Handler<Statement> handler) {
                final Iterable<? extends URI> types = stream.getProperty("types", Iterable.class);
                return new Encoder(handler, types);
            }

        });
    }

    /**
     * Performs RDF-to-record decoding by converting a stream of RDF statements in a stream of
     * records. Parameter {@code types} specify the types of records that have to be extracted
     * from the statement stream, while parameter {@code chunked} specifies whether the input
     * statement stream is chunked, i.e., organized as a sequence of statement chunks with each
     * chunk containing the statements for a record (and its nested records). Chunked RDF streams
     * noticeably speed up decoding, and are always produced by the KnowledgeStore API. Type and
     * chunking information may be set to null (e.g., because unknown at the time the method is
     * called): in this case, they will be read from metadata attributes attached to the stream,
     * named {@code "types"} and {@code "chunked"}; reading will happen just before decoding will
     * take place, i.e., when a terminal stream operation will be called.
     * 
     * @param stream
     *            the stream of statements to decode
     * @param types
     *            the types of records to extract from the statement stream, null if to be read
     *            from stream metadata
     * @param chunked
     *            true if the input statement stream is chunked, null if to be read from stream
     *            metadata
     * @return the resulting stream of records
     */
    public static Stream<Record> decode(final Stream<Statement> stream,
            @Nullable final Iterable<? extends URI> types, @Nullable final Boolean chunked) {
        Preconditions.checkNotNull(stream);
        if (types != null) {
            stream.setProperty("types", types);
        }
        if (chunked != null) {
            stream.setProperty("chunked", chunked);
        }
        return stream.transform(null, new Function<Handler<Record>, Handler<Statement>>() {

            @SuppressWarnings("unchecked")
            @Override
            public Handler<Statement> apply(final Handler<Record> handler) {
                final Iterable<? extends URI> types = stream.getProperty("types", Iterable.class);
                final Boolean chunked = stream.getProperty("chunked", Boolean.class);
                return new Decoder(handler, types, chunked);
            }

        });
    }

    private static class Encoder implements Handler<Record> {

        private final Handler<? super Statement> handler;

        private final Set<URI> types;

        Encoder(final Handler<? super Statement> handler, final Iterable<? extends URI> types) {
            this.handler = Preconditions.checkNotNull(handler);
            this.types = ImmutableSet.copyOf(types);
        }

        @Override
        public void handle(final Record record) throws Throwable {
            if (record != null) {
                emit(record, getID(record), true);
            } else {
                this.handler.handle(null);
            }
        }

        private void emit(final Record record, final URI subject, final boolean addType)
                throws Throwable {

            if (addType) {
                for (final URI type : this.types) {
                    emit(subject, RDF.TYPE, type);
                }
            }

            final List<URI> properties = record.getProperties();
            final List<Record> subRecords = Lists.newArrayList();

            for (final URI property : properties) {
                final List<Object> values = record.get(property);
                for (final Object value : values) {
                    if (value instanceof Value) {
                        final Value v = (Value) value;
                        if (!addType || !property.equals(RDF.TYPE) || !this.types.contains(v)) {
                            emit(subject, property, v);
                        }
                    } else if (value instanceof Record) {
                        final Record rv = (Record) value;
                        emit(subject, property, getID(rv));
                        subRecords.add(rv);
                    } else if (value instanceof Statement) {
                        final Statement s = (Statement) value;
                        final URI id = hash(s);
                        emit(subject, property, id);
                        emit(id, RDF.SUBJECT, s.getSubject());
                        emit(id, RDF.PREDICATE, s.getPredicate());
                        emit(id, RDF.OBJECT, s.getObject());
                    } else {
                        throw new Error("Unexpected type for value: " + value);
                    }
                }
            }

            for (final Record subRecord : subRecords) {
                emit(subRecord, getID(subRecord), false);
            }
        }

        private void emit(final Resource s, final URI p, final Value o) throws Throwable {
            this.handler.handle(Data.getValueFactory().createStatement(s, p, o));
        }

        private URI hash(final Statement statement) {
            return Data.getValueFactory().createURI("triples:" + Data.hash(statement.toString()));
        }

        private URI getID(final Record record) {
            final URI id = record.getID();
            if (id == null) {
                return Data.getValueFactory().createURI("bnode:" + record.hash());
            }
            return id;
        }

    }

    private static class Decoder implements Handler<Statement> {

        private final Handler<? super Record> handler;

        private final Set<URI> types;

        private final boolean chunked;

        private final UUID uuid;

        private final Map<URI, Node> nodes;

        private final List<Node> roots;

        private Node current;

        Decoder(final Handler<? super Record> handler, final Iterable<? extends URI> types,
                final boolean chunked) {
            this.handler = Preconditions.checkNotNull(handler);
            this.types = ImmutableSet.copyOf(types);
            this.chunked = chunked;
            this.uuid = UUID.randomUUID(); // for skolemization
            this.nodes = this.chunked ? Maps.<URI, Node>newLinkedHashMap() : Maps
                    .<URI, Node>newHashMap();
            this.roots = Lists.newArrayList();
            this.current = null;
        }

        @Override
        public void handle(final Statement statement) throws RDFHandlerException {

            if (statement == null) {
                flush(true);
                return;
            }

            final Statement s = skolemize(statement);

            final URI subj = (URI) s.getSubject();
            final URI pred = s.getPredicate();
            final Value obj = s.getObject();

            if (this.current == null || !this.current.id().equals(subj)) {
                this.current = this.nodes.get(subj);
                if (this.current == null) {
                    this.current = new Node(subj);
                    this.nodes.put(subj, this.current);
                }
            }

            this.current.add(s);

            if (pred.equals(RDF.TYPE) && this.types.contains(obj)) {
                this.current.mark();
                if (this.chunked && !this.roots.isEmpty()) {
                    flush(false);
                    final URI threshold = this.roots.get(this.roots.size() - 1).id();
                    final Iterator<URI> iterator = this.nodes.keySet().iterator();
                    while (true) {
                        final URI id = iterator.next();
                        iterator.remove();
                        if (id.equals(threshold)) {
                            break;
                        }
                    }
                    this.roots.clear();
                }
                this.roots.add(this.current);
            }
        }

        private Statement skolemize(final Statement statement) {
            boolean skolemized = false;
            Resource subj = statement.getSubject();
            if (subj instanceof BNode) {
                subj = skolemize((BNode) subj);
                skolemized = true;
            }
            Value obj = statement.getObject();
            if (obj instanceof BNode) {
                obj = skolemize((BNode) obj);
                skolemized = true;
            }
            if (skolemized) {
                final URI pred = statement.getPredicate();
                return Data.getValueFactory().createStatement(subj, pred, obj);
            }
            return statement;
        }

        private URI skolemize(final BNode bnode) {
            final String hash = Data.hash(this.uuid.getLeastSignificantBits(),
                    this.uuid.getMostSignificantBits(), bnode.getID());
            return Data.getValueFactory().createURI("bnode:" + hash);
        }

        private void flush(final boolean complete) throws RDFHandlerException {
            try {
                final List<Node> queue = Lists.newLinkedList();
                for (final Node root : this.roots) {
                    final Record record = (Record) root.visit(root, queue);
                    while (!queue.isEmpty()) {
                        final Node node = queue.remove(0);
                        node.complete(root, this.nodes, queue);
                    }
                    this.handler.handle(record);
                    if (Thread.interrupted()) {
                        throw new RDFHandlerException("Interrupted");
                    }
                }
                if (complete) {
                    this.handler.handle(null);
                }
            } catch (final Throwable ex) {
                Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                throw new RDFHandlerException(ex);
            }
        }

        private static class Node {

            private final URI id;

            private final List<Statement> statements;

            private Object value;

            private Node root;

            private boolean reified;

            private boolean result;

            Node(final URI id) {
                this.id = id;
                this.statements = Lists.newArrayList();
                this.result = false;
            }

            URI id() {
                return this.id;
            }

            void mark() {
                this.result = true;
            }

            void add(final Statement statement) {
                this.statements.add(statement);
                final URI pred = statement.getPredicate();
                this.reified = this.reified || pred.equals(RDF.SUBJECT)
                        || pred.equals(RDF.PREDICATE) || pred.equals(RDF.OBJECT);
            }

            Object visit(final Node root, final List<Node> queue) {
                if (this.root != root) {
                    this.root = root;
                    if (this.reified) {
                        this.value = unreify();
                    } else {
                        this.value = Record.create((URI) this.statements.get(0).getSubject());
                        queue.add(this); // register in the queue so to fill the record next
                    }
                    return !this.result || this == root ? this.value : this.statements.get(0)
                            .getSubject();
                } else if (this.value instanceof Statement) {
                    return this.value;
                }
                return this.statements.get(0).getSubject();
            }

            void complete(final Node root, final Map<URI, Node> nodes, final List<Node> queue) {

                final Record record = (Record) this.value;

                URI property = null;
                final List<Object> values = Lists.newArrayList();

                Collections.sort(this.statements, Data.getTotalComparator());
                for (final Statement statement : this.statements) {
                    if (!statement.getPredicate().equals(property)) {
                        if (property != null) {
                            record.set(property, values);
                        }
                        property = statement.getPredicate();
                        values.clear();
                    }
                    Object value = statement.getObject();
                    if (value instanceof URI) {
                        final Node n = nodes.get(value);
                        if (n != null) {
                            value = n.visit(root, queue);
                        }
                    }
                    values.add(value);
                }
                record.set(property, values);
            }

            private Statement unreify() {
                Resource subj = null;
                URI pred = null;
                Value obj = null;
                for (final Statement statement : this.statements) {
                    final URI property = statement.getPredicate();
                    if (property.equals(RDF.SUBJECT)) {
                        subj = (Resource) statement.getObject();
                    } else if (property.equals(RDF.PREDICATE)) {
                        pred = (URI) statement.getObject();
                    } else if (property.equals(RDF.OBJECT)) {
                        obj = statement.getObject();
                    }
                }
                return Data.getValueFactory().createStatement(subj, pred, obj);
            }

        }

    }

}
