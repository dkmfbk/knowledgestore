package eu.fbk.knowledgestore.data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.openrdf.model.URI;

/**
 * Merge criteria for combining old and new values of record properties.
 * <p>
 * A {@code Criteria} represents (a set of) merge criteria for combining old and new values of
 * selected record properties. Merge criteria are specified when updating records in the
 * KnowledgeStore via its API, and can be also used on their own for manipulating records on the
 * client side.
 * </p>
 * <p>
 * The set of properties a {@code Criteria} object supports may be restricted. Method
 * {@link #getProperties()} returns this set; the result is empty if the {@code Criteria} object
 * supports any property. Methods {@link #appliesTo(URI)} and {@link #appliesToAll()} can be
 * conveniently used for testing whether a specific property or all possible properties are
 * respectively supported by a given {@code Criteria} object.
 * </p>
 * <p>
 * Merging can be performed at two levels:
 * </p>
 * <ul>
 * <li>on a single property, via method {@link #merge(URI, List, List)} that returns the list
 * produced by merging old and new values, without modifying its inputs;</li>
 * <li>over multiple properties in common to a pair of records, via method
 * {@link #merge(Record, Record)}; this method merges values of any property in common to the old
 * and new record which is supported by the {@code Criteria}, storing the resulting values in the
 * old record, which is thus modified in place; if modification of input parameters is not
 * desired, the caller may clone the old record in advance and supply the clone to the
 * {@code merge()} method.</li>
 * </ul>
 * <p>
 * The following {@code Criteria} are supported, and can be instantiated based on specific factory
 * methods:
 * </p>
 * <ul>
 * <li><i>overwrite criteria</i> (factory method {@link #overwrite(URI...)}), consisting in the
 * discarding of old values which are overwritten with new values (even if new values are the
 * empty list, which means the affected property is cleared);</li>
 * <li><i>update criteria</i> (factory method {@link #update(URI...)}), consisting in the
 * replacement of old values with new ones, but only if the list of new values is not empty
 * (otherwise, old values are kept);</li>
 * <li><i>init criteria</i> (factory method {@link #init(URI...)}), consisting in the assignment
 * of new values only if the list of old values is empty, i.e., the property is initialized to the
 * supplied of new values if previously unset, otherwise old values are kept;</li>
 * <li><i>union criteria</i> (factory method {@link #union(URI...)}), consisting in computing and
 * assigning the union of old and new values, removing duplicates (in which case the new value is
 * kept, which for nested records means that properties of old nested records are discarded);</li>
 * <li><i>min criteria</i> (factory method {@link #min(URI...)}), consisting in identifying and
 * assigning the minimum value among the old and new ones (the comparator returned by
 * {@link Data#getTotalComparator()} is used);</li>
 * <li><i>max criteria</i> (factory method {@link #max(URI...)}), consisting in identifying and
 * assigning the maximum value among the old and new ones (the comparator returned by
 * {@link Data#getTotalComparator()} is used);</li>
 * <li><i>composed criteria</i> (factory method {@link #compose(Criteria...)}), consisting in
 * applying the first matching {@code Criteria} of the specified list to an input property; the
 * resulting {@code Criteria} will be able to merge the union of all the properties supported by
 * the composed {@code Criteria}. In case decomposition of a possibly composed {@code Criteria} is
 * desired, method {@link #decompose()} returns the (recursively) composed elementary
 * {@code Criteria} for a certain input {@code Criteria} (the input {@code Criteria} is returned
 * unchanged if not composes).</li>
 * </ul>
 * <p>
 * {@code Criteria} objects are immutable and thus thread safe. Two {@code Criteria} objects are
 * equal if they implement the same strategy (possibly composed) and support the same properties.
 * Serialization to string and deserialization back to a {@code Criteria} object are supported,
 * via methods {@link #toString()}, {@link #toString(Map)} (which accepts a custom namespace map
 * for encoding properties) and {@link #parse(String, Map)}. The string specification of a
 * {@code Criteria} has the following form:
 * {@code criteria1 property11, ..., property1N, ..., criteriaM propertyM1, ... propertyMN} where
 * commas are all optional, the criteria token is one of {@code overwrite}, {@code update},
 * {@code init}, {@code union}, {@code min}, {@code max} (case does not matter) and properties are
 * encoded according to the Turtle / TriG syntax (full URIs between {@code <} and {@code >}
 * characters or QNames).
 * </p>
 */
public abstract class Criteria implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<URI> properties;

    private Criteria(final URI... properties) {
        this.properties = ImmutableSet.copyOf(properties);
    }

    private static Criteria create(final String name, final URI... properties) {

        if (Overwrite.class.getSimpleName().equalsIgnoreCase(name)) {
            return overwrite(properties);
        } else if (Update.class.getSimpleName().equalsIgnoreCase(name)) {
            return update(properties);
        } else if (Init.class.getSimpleName().equalsIgnoreCase(name)) {
            return init(properties);
        } else if (Min.class.getSimpleName().equalsIgnoreCase(name)) {
            return min(properties);
        } else if (Max.class.getSimpleName().equalsIgnoreCase(name)) {
            return max(properties);
        } else if (Union.class.getSimpleName().equalsIgnoreCase(name)) {
            return union(properties);
        } else {
            throw new IllegalArgumentException("Unknown criteria name: " + name);
        }
    }

    /**
     * Parses the supplied string specification of a merge criteria, returning the parsed
     * {@code Criteria} object. The string must adhere to the format specified in the main Javadoc
     * comment.
     * 
     * @param string
     *            the specification of the merge criteria
     * @param namespaces
     *            the namespace map to be used for parsing the string, null if no mapping should
     *            be used
     * @return the parsed {@code Criteria}, on success
     * @throws ParseException
     *             in case the specification string is not valid
     */
    public static Criteria parse(final String string,
            @Nullable final Map<String, String> namespaces) throws ParseException {

        Preconditions.checkNotNull(string);

        final List<Criteria> criteria = Lists.newArrayList();
        final List<URI> uris = Lists.newArrayList();
        String name = null;

        try {
            for (final String token : string.split("[\\s\\,]+")) {
                if ("*".equals(token)) {
                    criteria.add(create(name, uris.toArray(new URI[uris.size()])));
                    name = null;
                    uris.clear();

                } else if (token.startsWith("<") && token.endsWith(">") //
                        || token.indexOf(':') >= 0) {
                    uris.add((URI) Data.parseValue(token, namespaces));

                } else if (name != null || !uris.isEmpty()) {
                    criteria.add(create(name, uris.toArray(new URI[uris.size()])));
                    name = token;
                    uris.clear();

                } else {
                    name = token;
                }
            }

            if (!uris.isEmpty()) {
                criteria.add(create(name, uris.toArray(new URI[uris.size()])));
            }

            return criteria.size() == 1 ? criteria.get(0) : compose(criteria
                    .toArray(new Criteria[criteria.size()]));

        } catch (final Exception ex) {
            throw new ParseException(string, "Invalid criteria string - " + ex.getMessage(), ex);
        }
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>overwrite</i> merge criteria for the
     * properties specified. The overwrite criteria always selects the new values of a property,
     * even if they consist in an empty list that will cause the property to be cleared.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria overwrite(final URI... properties) {
        return new Overwrite(properties);
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>update</i> merge criteria for the
     * properties specified. The update criteria assigns the new values to a property only if they
     * do not consist in the empty list, in which case old values are kept.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria update(final URI... properties) {
        return new Update(properties);
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>init</i> merge criteria for the
     * properties specified. The init criteria assignes the new values to a property only if it
     * has no old value (i.e., old values are the empty list), thus realizing a one-time property
     * initialization mechanism.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria init(final URI... properties) {
        return new Init(properties);
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>union</i> merge criteria for the
     * properties specified. The union criteria assigns the union of old and new values to a
     * property, discarding duplicates. In case duplicates are two nested records with the same ID
     * (thus evaluating equal), the new value is kept.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria union(final URI... properties) {
        return new Union(properties);
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>min</i> merge criteria for the
     * properties specified. The min criteria assignes the minimum value among the old and new
     * ones.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria min(final URI... properties) {
        return new Min(properties);
    }

    /**
     * Creates a {@code Criteria} object implementing the <i>max</i> merge criteria for the
     * properties specified. The max criteria assignes the maximum value among the old and new
     * ones.
     * 
     * @param properties
     *            a vararg array with the properties over which the criteria should be applied; if
     *            empty, the criteria will be applied to any property
     * @return the created {@code Criteria} object
     */
    public static Criteria max(final URI... properties) {
        return new Max(properties);
    }

    /**
     * Creates a {@code Criteria} object that composes the {@code Criteria} objects specified in a
     * <i>composed</i> merge criteria. Given a property whose old and new values have to be
     * merged, the created composed criteria will scan through the supplied list of
     * {@code Criteria} using the first matching one. As a consequence, the created criteria will
     * support all the properties that are supported by at least one of the composed
     * {@code Criteria}; if one of them supports all the properties, then the composed criteria
     * will also support all the properties.
     * 
     * @param criteria
     *            the {@code Criteria} objects to compose
     * @return the created {@code Criteria} object
     */
    public static Criteria compose(final Criteria... criteria) {
        Preconditions.checkArgument(criteria.length > 0, "At least a criteria must be supplied");
        if (criteria.length == 1) {
            return criteria[0];
        } else {
            return new Compose(criteria);
        }
    }

    /**
     * Returns the set of properties supported by this {@code Criteria} object.
     * 
     * @return a set with the supported properties; if empty, all properties are supported
     */
    public final Set<URI> getProperties() {
        return this.properties;
    }

    /**
     * Checks whether the property specified is supported by this {@code Criteria} object.
     * 
     * @param property
     *            the property
     * @return true, if the property is supported
     */
    public final boolean appliesTo(final URI property) {

        if (this.properties.isEmpty() || this.properties.contains(property)) {
            return true;
        }
        Preconditions.checkNotNull(property);
        return false;
    }

    /**
     * Checks whether all properties are supported by this {@code Criteria} object.
     * 
     * @return true, if all properties are supported, with no restriction
     */
    public final boolean appliesToAll() {
        return this.properties.isEmpty();
    }

    /**
     * Merges all supported properties in common to the old and new record specified, storing the
     * results in the old record.
     * 
     * @param oldRecord
     *            the record containing old property values, not null; results of the merging
     *            operation are stored in this record, possibly replacing old values of affected
     *            properties (if this behavious is not desired, clone the old record in advance)
     * @param newRecord
     *            the record containing new property values, not null
     */
    public final void merge(final Record oldRecord, final Record newRecord) {

        Preconditions.checkNotNull(oldRecord);

        for (final URI property : newRecord.getProperties()) {
            if (appliesTo(property)) {
                oldRecord.set(property,
                        merge(property, oldRecord.get(property), newRecord.get(property)));
            }
        }
    }

    /**
     * Merges old and new values of the property specified, returning the resulting list of
     * values. Input value lists are not affected. In case the property specified is not supported
     * by this {@code Criteria} object, the old list of values is returned.
     * 
     * @param property
     *            the property to merge (used to control whether merging should be performed and
     *            which strategy should be adopted in case of a composed {@code Criteria} object)
     * @param oldValues
     *            a list with the old values of the property, not null
     * @param newValues
     *            a list with the new values of the property, not null
     * @return the list of values obtained by applying the merge criteria
     */
    @SuppressWarnings("unchecked")
    public final List<Object> merge(final URI property, final List<? extends Object> oldValues,
            final List<? extends Object> newValues) {

        Preconditions.checkNotNull(oldValues);
        Preconditions.checkNotNull(newValues);

        return doMerge(property, (List<Object>) oldValues, (List<Object>) newValues);
    }

    List<Object> doMerge(final URI property, final List<Object> oldValues,
            final List<Object> newValues) {
        return appliesTo(property) ? doMerge(oldValues, newValues) : oldValues;
    }

    List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
        return oldValues;
    }

    /**
     * Decomposes this {@code Criteria} object in its elementary (i.e., non-composed) components.
     * In case this {@code Criteria} object is not composed, it is directly returned by the method
     * in a singleton list. Otherwise, its components are recursively extracted and returned in a
     * list that reflects their order of use.
     * 
     * @return a list of non-composed {@code Criteria} objects, in the same order as they are
     *         applied in this merge criteria object
     */
    public final List<Criteria> decompose() {
        return doDecompose();
    }

    List<Criteria> doDecompose() {
        return ImmutableList.of(this);
    }

    /**
     * {@inheritDoc} Two {@code Criteria} objects are equal if they implement the same merge
     * criteria over the same properties.
     */
    @Override
    public final boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || object.getClass() != this.getClass()) {
            return false;
        }
        final Criteria other = (Criteria) object;
        return this.properties.equals(other.getProperties());
    }

    /**
     * {@inheritDoc} The returned hash code reflects the specific criteria and supported
     * properties of this {@code Criteria} object.
     */
    @Override
    public final int hashCode() {
        return this.properties.hashCode();
    }

    /**
     * Returns a parseable string representation of this {@code Criteria} object, using the
     * supplied namespace map for encoding property URIs.
     * 
     * @param namespaces
     *            the namespace map to encode property URIs
     * @return the produced string
     */
    public final String toString(@Nullable final Map<String, String> namespaces) {
        final StringBuilder builder = new StringBuilder();
        doToString(builder, namespaces);
        return builder.toString();
    }

    /**
     * {@inheritDoc} This method returns a parseable string representation of this
     * {@code Criteria} object, encoding property URIs as full, non-abbreviated URIs.
     */
    @Override
    public final String toString() {
        return toString(null);
    }

    void doToString(final StringBuilder builder, @Nullable final Map<String, String> namespaces) {
        builder.append(getClass().getSimpleName().toLowerCase()).append(" ");
        if (this.properties.isEmpty()) {
            builder.append("*");
        } else {
            String separator = "";
            for (final URI property : this.properties) {
                builder.append(separator).append(Data.toString(property, namespaces));
                separator = ", ";
            }
        }
    }

    private static final class Overwrite extends Criteria {

        private static final long serialVersionUID = 1L;

        Overwrite(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            return newValues;
        }

    }

    private static final class Update extends Criteria {

        private static final long serialVersionUID = 1L;

        Update(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            return newValues.isEmpty() ? oldValues : newValues;
        }

    }

    private static final class Init extends Criteria {

        private static final long serialVersionUID = 1L;

        Init(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            return oldValues.isEmpty() ? newValues : oldValues;
        }

    }

    private static final class Union extends Criteria {

        private static final long serialVersionUID = 1L;

        Union(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            if (oldValues.isEmpty()) {
                return newValues;
            } else if (newValues.isEmpty()) {
                return oldValues;
            } else {
                final Set<Object> set = Sets.newLinkedHashSet();
                set.addAll(oldValues);
                set.addAll(newValues);
                return ImmutableList.copyOf(set);
            }
        }

    }

    private static final class Min extends Criteria {

        private static final long serialVersionUID = 1L;

        Min(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            if (oldValues.isEmpty()) {
                return newValues.size() <= 1 ? newValues : ImmutableList
                        .of(((Ordering<Object>) Data.getTotalComparator()).min(newValues));
            } else if (newValues.isEmpty()) {
                return oldValues.size() <= 1 ? oldValues : ImmutableList
                        .of(((Ordering<Object>) Data.getTotalComparator()).min(oldValues));
            } else {
                return ImmutableList.of(((Ordering<Object>) Data.getTotalComparator())
                        .min(Iterables.concat(oldValues, newValues)));
            }
        }

    }

    private static final class Max extends Criteria {

        private static final long serialVersionUID = 1L;

        Max(final URI... properties) {
            super(properties);
        }

        @Override
        List<Object> doMerge(final List<Object> oldValues, final List<Object> newValues) {
            if (oldValues.isEmpty()) {
                return newValues.size() <= 1 ? newValues : ImmutableList
                        .of(((Ordering<Object>) Data.getTotalComparator()).max(newValues));
            } else if (newValues.isEmpty()) {
                return oldValues.size() <= 1 ? oldValues : ImmutableList
                        .of(((Ordering<Object>) Data.getTotalComparator()).max(oldValues));
            } else {
                return ImmutableList.of(((Ordering<Object>) Data.getTotalComparator())
                        .max(Iterables.concat(oldValues, newValues)));
            }
        }

    }

    private static final class Compose extends Criteria {

        private static final long serialVersionUID = 1L;

        private final Criteria[] specificCriteria;

        private final Criteria defaultCriteria;

        Compose(final Criteria... criteria) {
            super(extractProperties(criteria));
            Criteria candidateDefaultCriteria = null;
            final ImmutableList.Builder<Criteria> builder = ImmutableList.builder();
            for (final Criteria c : criteria) {
                for (final Criteria d : c.decompose()) {
                    if (!d.appliesToAll()) {
                        builder.add(d);
                    } else if (candidateDefaultCriteria == null) {
                        candidateDefaultCriteria = d;
                    }
                }
            }
            this.specificCriteria = Iterables.toArray(builder.build(), Criteria.class);
            this.defaultCriteria = candidateDefaultCriteria;
        }

        @Override
        List<Object> doMerge(final URI property, final List<Object> oldValues,
                final List<Object> newValues) {
            for (final Criteria c : this.specificCriteria) {
                if (c.appliesTo(property)) {
                    return c.doMerge(oldValues, newValues);
                }
            }
            if (this.defaultCriteria != null) {
                return this.defaultCriteria.doMerge(oldValues, newValues);
            }
            return oldValues;
        }

        @Override
        List<Criteria> doDecompose() {
            final ImmutableList.Builder<Criteria> builder = ImmutableList.builder();
            builder.add(this.specificCriteria);
            builder.add(this.defaultCriteria);
            return builder.build();
        }

        @Override
        void doToString(final StringBuilder builder, //
                @Nullable final Map<String, String> namespaces) {
            String separator = "";
            for (final Criteria c : this.specificCriteria) {
                builder.append(separator);
                c.doToString(builder, namespaces);
                separator = ", ";
            }
            if (this.defaultCriteria != null) {
                builder.append(separator);
                this.defaultCriteria.doToString(builder, namespaces);
            }
        }

        private static URI[] extractProperties(final Criteria... criteria) {
            final List<URI> properties = Lists.newArrayList();
            for (final Criteria c : criteria) {
                properties.addAll(c.properties);
            }
            return properties.toArray(new URI[properties.size()]);
        }

    }

}

// alternative API can be:
// (1) new Criteria().update(DC.TITLE).override(DC.ISSUED)
// - modifiable, verbose if a single option is used
// (2) Criteria.builder().update(DC.TITLE).override(DC.ISSUED).build()
// - verbose, esp if a single option is used
// alternative (2) can be however merged into this implementation, by adding a builder
