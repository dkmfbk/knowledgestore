package eu.fbk.knowledgestore;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;

import eu.fbk.knowledgestore.Outcome.Status;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.ParseException;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;

// best effort basis: will interrupt thread, then it is up to the implementation to
// decide what to do
// for read operations, HTTP connection can be interrupted; for create / merge
// operations, no more data should be sent; delete / update will make the connection
// fail

// properties for which there is no criteria are not modified

public abstract class Operation {

    private final Map<String, String> inheritedNamespaces;

    Map<String, String> namespaces;

    Long timeout;

    Operation(@Nullable final Map<String, String> inheritedNamespaces) {
        this.inheritedNamespaces = inheritedNamespaces != null ? inheritedNamespaces
                : ImmutableMap.<String, String>of();
        this.namespaces = this.inheritedNamespaces;
        this.timeout = null;
    }

    /**
     * Sets the optional timeout for this operation in milliseconds. Passing null or a
     * non-positive value will remove any timeout previously set.
     * 
     * @param timeout
     *            the timeout; null or non-positive to reset
     * @return this operation object for call chaining
     */
    public synchronized Operation timeout(@Nullable final Long timeout) {
        this.timeout = timeout == null || timeout > 0 ? timeout : null;
        return this;
    }

    /**
     * Sets the optional namespaces for this operation. Supplied namespaces overrides the
     * namespaces inherited from the {@code Session}. Passing null will remove any namespace map
     * previously set on the operation.
     * 
     * @param namespaces
     *            the namespace map overriding session namespaces; null to reset
     * @return this operation object for call chaining
     */
    public synchronized Operation namespaces(@Nullable final Map<String, String> namespaces) {
        this.namespaces = namespaces == null ? this.inheritedNamespaces : Data.newNamespaceMap(
                namespaces, this.inheritedNamespaces);
        return this;
    }

    // UTILITY METHODS

    static URI checkType(final URI type) {
        Preconditions.checkNotNull(type, "No type specified");
        if (!type.equals(KS.RESOURCE) && !type.equals(KS.MENTION) && !type.equals(KS.ENTITY)
                && !type.equals(KS.AXIOM)) {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
        return type;
    }

    static XPath conditionFor(final URI property, final Object... allowedValues) {
        final String namespace = property.getNamespace();
        final String prefix = MoreObjects.firstNonNull(//
                Data.namespaceToPrefix(namespace, Data.getNamespaceMap()), "ns");
        return XPath.parse(
                String.format("with %s: <%s> : %s:%s = {}", prefix, namespace, prefix,
                        property.getLocalName()), allowedValues);
    }

    static <T> Handler<T> handlerFor(@Nullable final Collection<? super T> collection) {
        if (collection == null) {
            return null;
        } else {
            return new Handler<T>() {

                @Override
                public void handle(final T element) {
                    if (element != null) {
                        collection.add(element);
                    }
                }

            };
        }
    }

    @Nullable
    static XPath merge(final Iterable<XPath> conditions) {
        return conditions == null || Iterables.isEmpty(conditions) ? null : XPath.compose("and",
                (Object[]) Iterables.toArray(conditions, XPath.class));
    }

    static <T> List<T> add(@Nullable final List<T> list, final T element) {
        if (list == null) {
            return ImmutableList.of(element);
        } else {
            final List<T> tmp = Lists.newArrayList(list);
            tmp.add(element);
            return ImmutableList.copyOf(tmp);
        }
    }

    static <T> Set<T> add(@Nullable final Set<T> set, final T element) {
        if (set == null) {
            return ImmutableSet.of(element);
        } else {
            final List<T> tmp = Lists.newArrayList(set);
            tmp.add(element);
            return ImmutableSet.copyOf(tmp);
        }
    }

    /**
     * Download operation.
     * <p>
     * This operation attempts at fetching the representation associated to a resource with the ID
     * specified. The operation is controlled by two optional parameters:
     * {@link #caching(boolean)} enables or disables the use of intermediate caches, if available,
     * while {@link #accept(String...)} / {@link #accept(Iterable)} require the returned
     * representation to have a certain MIME type (if it is not the case, the invocation fails).
     * </p>
     */
    public abstract static class Download extends Operation {

        private final URI resourceID;

        @Nullable
        private Set<String> mimeTypes;

        private boolean caching;

        /**
         * Creates a new {@code Download} operation instance.
         * 
         * @param inheritedNamespaces
         *            an <b>immutable</b> map of inherited namespaces, possibly null
         * @param resourceID
         *            the ID of the resource whose representation should be retrieved
         */
        protected Download(@Nullable final Map<String, String> inheritedNamespaces,
                final URI resourceID) {
            super(inheritedNamespaces);
            this.resourceID = Preconditions.checkNotNull(resourceID, "Null resource ID");
            this.mimeTypes = null;
            this.caching = true;
        }

        @Override
        public Download timeout(@Nullable final Long timeout) {
            return (Download) super.timeout(timeout);
        }

        @Override
        public Download namespaces(@Nullable final Map<String, String> namespaces) {
            return (Download) super.namespaces(namespaces);
        }

        /**
         * Sets the acceptable MIME type (default: accept everything). Supplied values override
         * previously configured MIME types; passing null will drop the constraint.
         * 
         * @param mimeTypes
         *            the acceptable MIME types; null (default) to drop any constraint
         * @return this operation object, for call chaining
         */
        public final synchronized Download accept(@Nullable final String... mimeTypes) {
            return accept(mimeTypes == null ? null : Arrays.asList(mimeTypes));
        }

        /**
         * Sets the acceptable MIME types (default: accept everything). Supplied values override
         * previously configured MIME types; passing null will drop the constraint.
         * 
         * @param mimeTypes
         *            the acceptable MIME types; null (default) to drop any constraint
         * @return this operation object, for call chaining
         */
        public final synchronized Download accept(
                @Nullable final Iterable<? extends String> mimeTypes) {
            this.mimeTypes = mimeTypes == null ? null : Sets.newLinkedHashSet(mimeTypes);
            return this;
        }

        /**
         * Sets whether the representation can be retrieved from caches (default true).
         * 
         * @param caching
         *            true, if the representation can be retrieved from caches
         * @return this operation object, for call chaining
         */
        public final synchronized Download caching(final boolean caching) {
            this.caching = caching;
            return this;
        }

        /**
         * Executes the operation, returning the requested representation or null, if it does not
         * exist. Note that returned representations MUST be closed after use.
         * 
         * @return the requested representation, or null if it does not exist
         * @throws OperationException
         *             in case of failure (see possible outcome status codes)
         */
        public final synchronized Representation exec() throws OperationException {
            return doExec(this.timeout, this.resourceID, this.mimeTypes, this.caching);
        }

        /**
         * Implementation method responsible of executing the download operation.
         * 
         * @param timeout
         *            the optional timeout for the operation; null if there is no timeout
         * @param resourceID
         *            the ID of the resource whose representation must be returned
         * @param mimeTypes
         *            the acceptable MIME types; null if there is no constraint
         * @param caching
         *            true, if the representation can be retrieved from a cache
         * @return the resource representation, if exists, otherwise null
         * @throws OperationException
         *             in case of failure (see possible outcome status codes)
         */
        @Nullable
        protected abstract Representation doExec(@Nullable Long timeout, URI resourceID,
                @Nullable Set<String> mimeTypes, boolean caching) throws OperationException;

    }

    /**
     * Upload operation.
     * <p>
     * The operation outcome may assume one among the following {@code Status} codes:
     * </p>
     * <table border="1">
     * <tr>
     * <th>Status</th>
     * <th>Explanation</th>
     * <th>Reported as</td>
     * </tr>
     * <tr>
     * <td>{@link Status#OK_CREATED}</td>
     * <td>the file was successfully uploaded without replacing an existing file for the same
     * resource</td>
     * <td>{@code exec()} return value</td>
     * </tr>
     * <tr>
     * <td>{@link Status#OK_MODIFIED}</td>
     * <td>the file was successfully uploaded and replaced an existing file bound to the same
     * resource</td>
     * <td>{@code exec()} return value</td>
     * </tr>
     * <tr>
     * <td>{@link Status#OK_DELETED}</td>
     * <td>the file was successfully deleted</td>
     * <td>{@code exec()} return value</td>
     * </tr>
     * <tr>
     * <td>{@link Status#ERROR_INVALID_INPUT}</td>
     * <td>in case the supplied URI is not valid or a problem is detected in the uploaded file</td>
     * <td>{@code OperationException}</td>
     * </tr>
     * <tr>
     * <td>{@link Status#ERROR_OBJECT_NOT_FOUND}</td>
     * <td>the file cannot be deleted because it does not exist (the associated resource may or
     * may not exist)</td>
     * <td>{@code OperationException}</td>
     * </tr>
     * <tr>
     * <td>{@link Status#ERROR_DEPENDENCY_NOT_FOUND}</td>
     * <td>supplied file cannot be stored because the referenced resource does not exist</td>
     * <td>{@code OperationException}</td>
     * </tr>
     * <tr>
     * <td>{@link Status#ERROR_UNEXPECTED}</td>
     * <td>an unexpected error occurred preventing the successful execution of the operation</td>
     * <td>{@code OperationException}</td>
     * </tr>
     * <tr>
     * <td>{@link Status#ERROR_UNKNOWN}</td>
     * <td>a connectivity problem caused the interruption of the operation whose outcome is
     * unknown</td>
     * <td>{@code OperationException}</td>
     * </tr>
     * </table>
     */
    public abstract static class Upload extends Operation {

        private final URI resourceID;

        @Nullable
        private Representation representation;

        protected Upload(final Map<String, String> inheritedNamespaces, final URI id) {
            super(inheritedNamespaces);
            this.resourceID = Preconditions.checkNotNull(id, "Null resource ID");
            this.representation = null;
        }

        @Override
        public Upload timeout(@Nullable final Long timeout) {
            return (Upload) super.timeout(timeout);
        }

        @Override
        public Upload namespaces(@Nullable final Map<String, String> namespaces) {
            return (Upload) super.namespaces(namespaces);
        }

        /**
         * Sets the representation to upload, if any. Setting null will cause any previously
         * stored representation to be dropped.
         * 
         * @param representation
         *            the representation to upload, if any
         * @return this operation object, for call chaining
         */
        public final synchronized Upload representation(
                @Nullable final Representation representation) {
            this.representation = representation;
            return this;
        }

        /**
         * Executes the operation, returning its outcome.
         * 
         * @return the outcome of the operation
         * @throws OperationException
         *             in case of failure (see possible outcome status codes)
         */
        public final synchronized Outcome exec() throws OperationException {
            return doExec(this.timeout, this.resourceID, this.representation);
        }

        protected abstract Outcome doExec(@Nullable Long timeout, URI resourceID,
                @Nullable Representation representation) throws OperationException;

    }

    public abstract static class Count extends Operation {

        private final URI type;

        @Nullable
        private List<XPath> conditions;

        @Nullable
        private Set<URI> ids;

        protected Count(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.conditions = null;
            this.ids = null;
        }

        @Override
        public Count timeout(@Nullable final Long timeout) {
            return (Count) super.timeout(timeout);
        }

        @Override
        public Count namespaces(@Nullable final Map<String, String> namespaces) {
            return (Count) super.namespaces(namespaces);
        }

        public final synchronized Count conditions(@Nullable final XPath... conditions) {
            return conditions(conditions == null ? null : Arrays.asList(conditions));
        }

        public final synchronized Count conditions(
                @Nullable final Iterable<? extends XPath> conditions) {
            this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
            return this;
        }

        public final synchronized Count condition(final String condition,
                final Object... arguments) throws ParseException {
            this.conditions = add(this.conditions,
                    XPath.parse(this.namespaces, condition, arguments));
            return this;
        }

        public final synchronized Count condition(final URI property,
                final Object... allowedValues) {
            this.conditions = add(this.conditions, conditionFor(property, allowedValues));
            return this;
        }

        public final synchronized Count ids(@Nullable final URI... ids) {
            return ids(ids == null ? null : Arrays.asList(ids));
        }

        public final synchronized Count ids(@Nullable final Iterable<? extends URI> ids) {
            this.ids = ids == null ? null : ImmutableSet.copyOf(ids);
            return this;
        }

        public final synchronized long exec() throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids);
        }

        protected abstract long doExec(@Nullable Long timeout, URI type,
                @Nullable XPath condition, @Nullable Set<URI> ids) throws OperationException;

    }

    public abstract static class Retrieve extends Operation {

        private final URI type;

        @Nullable
        private List<XPath> conditions;

        @Nullable
        private Set<URI> ids;

        @Nullable
        private Set<URI> properties;

        @Nullable
        private Long offset;

        @Nullable
        private Long limit;

        protected Retrieve(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.conditions = null;
            this.ids = null;
            this.properties = null;
            this.offset = null;
            this.limit = null;
        }

        @Override
        public Retrieve timeout(@Nullable final Long timeout) {
            return (Retrieve) super.timeout(timeout);
        }

        @Override
        public Retrieve namespaces(@Nullable final Map<String, String> namespaces) {
            return (Retrieve) super.namespaces(namespaces);
        }

        public final synchronized Retrieve conditions(@Nullable final XPath... conditions) {
            return conditions(conditions == null ? null : Arrays.asList(conditions));
        }

        public final synchronized Retrieve conditions(
                @Nullable final Iterable<? extends XPath> conditions) {
            this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
            return this;
        }

        public final synchronized Retrieve condition(final String condition,
                final Object... arguments) throws ParseException {
            this.conditions = add(this.conditions,
                    XPath.parse(this.namespaces, condition, arguments));
            return this;
        }

        public final synchronized Retrieve condition(final URI property,
                final Object... allowedValues) {
            this.conditions = add(this.conditions, conditionFor(property, allowedValues));
            return this;
        }

        public final synchronized Retrieve ids(@Nullable final URI... ids) {
            return ids(ids == null ? null : Arrays.asList(ids));
        }

        public final synchronized Retrieve ids(@Nullable final Iterable<? extends URI> ids) {
            this.ids = ids == null ? null : ImmutableSet.copyOf(ids);
            return this;
        }

        // no call or empty = all properties

        public final synchronized Retrieve properties(@Nullable final URI... properties) {
            return properties(properties == null ? null : Arrays.asList(properties));
        }

        public final synchronized Retrieve properties(
                @Nullable final Iterable<? extends URI> properties) {
            this.properties = properties == null ? null : ImmutableSet.copyOf(properties);
            return this;
        }

        public final synchronized Retrieve offset(@Nullable final Long offset) {
            this.offset = offset == null || offset <= 0 ? null : offset;
            return this;
        }

        public final synchronized Retrieve limit(@Nullable final Long limit) {
            this.limit = limit == null || limit <= 0 ? null : limit;
            return this;
        }

        public final synchronized Stream<Record> exec() throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids,
                    this.properties, this.offset, this.limit);
        }

        protected abstract Stream<Record> doExec(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids,
                @Nullable final Set<URI> properties, @Nullable Long offset, @Nullable Long limit)
                throws OperationException;

    }

    public abstract static class Create extends Operation {

        private final URI type;

        @Nullable
        private Stream<? extends Record> records;

        protected Create(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.records = null;
        }

        @Override
        public Create timeout(@Nullable final Long timeout) {
            return (Create) super.timeout(timeout);
        }

        @Override
        public Create namespaces(@Nullable final Map<String, String> namespaces) {
            return (Create) super.namespaces(namespaces);
        }

        public final synchronized Create records(@Nullable final Record... records) {
            this.records = records == null ? null : Stream.create(records);
            return this;
        }

        public final synchronized Create records(//
                @Nullable final Iterable<? extends Record> records) {
            this.records = records == null ? null : Stream.create(records);
            return this;
        }

        public final synchronized Outcome exec() throws OperationException {
            return doExec(this.timeout, this.type, this.records, null);
        }

        // TODO: errors from the handlers are logged and cause the invocation to be interrupted
        // eventually; still, the handler will be notified of all the outcomes until the
        // invocation ends

        public final synchronized Outcome exec(@Nullable final Handler<? super Outcome> handler)
                throws OperationException {
            return doExec(this.timeout, this.type, this.records, handler);
        }

        public final synchronized Outcome exec(
                @Nullable final Collection<? super Outcome> collection) throws OperationException {
            return doExec(this.timeout, this.type, this.records, handlerFor(collection));
        }

        protected abstract Outcome doExec(@Nullable Long timeout, final URI type,
                @Nullable final Stream<? extends Record> records,
                @Nullable final Handler<? super Outcome> handler) throws OperationException;

    }

    public abstract static class Merge extends Operation {

        private final URI type;

        @Nullable
        private Stream<? extends Record> records;

        @Nullable
        private Criteria criteria;

        protected Merge(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.records = null;
            this.criteria = null;
        }

        @Override
        public Merge timeout(@Nullable final Long timeout) {
            return (Merge) super.timeout(timeout);
        }

        @Override
        public Merge namespaces(@Nullable final Map<String, String> namespaces) {
            return (Merge) super.namespaces(namespaces);
        }

        public final synchronized Merge records(@Nullable final Record... records) {
            this.records = records == null ? null : Stream.create(records);
            return this;
        }

        public final synchronized Merge records(//
                @Nullable final Iterable<? extends Record> records) {
            this.records = records == null ? null : Stream.create(records);
            return this;
        }

        public final synchronized Merge criteria(@Nullable final Criteria... criteria) {
            return criteria(criteria == null ? null : Arrays.asList(criteria));
        }

        public final synchronized Merge criteria(
                @Nullable final Iterable<? extends Criteria> criteria) {
            this.criteria = criteria == null || Iterables.isEmpty(criteria) ? null : Criteria
                    .compose(Iterables.toArray(criteria, Criteria.class));
            return this;
        }

        public final synchronized Merge criteria(@Nullable final String criteria)
                throws ParseException {
            this.criteria = criteria == null ? null : Criteria.parse(criteria, this.namespaces);
            return this;
        }

        public final synchronized Outcome exec() throws OperationException {
            return doExec(this.timeout, this.type, this.records, this.criteria, null);
        }

        public final synchronized Outcome exec(@Nullable final Handler<? super Outcome> handler)
                throws OperationException {
            return doExec(this.timeout, this.type, this.records, this.criteria, handler);
        }

        public final synchronized Outcome exec(
                @Nullable final Collection<? super Outcome> collection) throws OperationException {
            return doExec(this.timeout, this.type, this.records, this.criteria,
                    handlerFor(collection));
        }

        protected abstract Outcome doExec(@Nullable Long timeout, URI type,
                @Nullable Stream<? extends Record> stream, @Nullable Criteria criteria,
                @Nullable Handler<? super Outcome> handler) throws OperationException;

    }

    public abstract static class Update extends Operation {

        private final URI type;

        @Nullable
        private List<XPath> conditions;

        @Nullable
        private Set<URI> ids;

        @Nullable
        private Record record;

        @Nullable
        private Criteria criteria;

        protected Update(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.conditions = null;
            this.ids = null;
            this.record = null;
            this.criteria = null;
        }

        @Override
        public Update timeout(@Nullable final Long timeout) {
            return (Update) super.timeout(timeout);
        }

        @Override
        public Update namespaces(@Nullable final Map<String, String> namespaces) {
            return (Update) super.namespaces(namespaces);
        }

        public final synchronized Update conditions(@Nullable final XPath... conditions) {
            return conditions(conditions == null ? null : Arrays.asList(conditions));
        }

        public final synchronized Update conditions(
                @Nullable final Iterable<? extends XPath> conditions) {
            this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
            return this;
        }

        public final synchronized Update condition(final String condition,
                final Object... arguments) throws ParseException {
            this.conditions = add(this.conditions,
                    XPath.parse(this.namespaces, condition, arguments));
            return this;
        }

        public final synchronized Update condition(final URI property,
                final Object... allowedValues) {
            this.conditions = add(this.conditions, conditionFor(property, allowedValues));
            return this;
        }

        public final synchronized Update ids(@Nullable final URI... ids) {
            return ids(ids == null ? null : Arrays.asList(ids));
        }

        public final synchronized Update ids(@Nullable final Iterable<? extends URI> ids) {
            this.ids = ids == null ? null : ImmutableSet.copyOf(ids);
            return this;
        }

        public final synchronized Update record(@Nullable final Record record) {
            this.record = record;
            return this;
        }

        public final synchronized Update criteria(@Nullable final Criteria... criteria) {
            return criteria(criteria == null ? null : Arrays.asList(criteria));
        }

        public final synchronized Update criteria(
                @Nullable final Iterable<? extends Criteria> criteria) {
            this.criteria = criteria == null || Iterables.isEmpty(criteria) ? null : Criteria
                    .compose(Iterables.toArray(criteria, Criteria.class));
            return this;
        }

        public final synchronized Update criteria(@Nullable final String criteria)
                throws ParseException {
            this.criteria = criteria == null ? null : Criteria.parse(criteria, this.namespaces);
            return this;
        }

        public final synchronized Outcome exec() throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids, this.record,
                    this.criteria, null);
        }

        public final synchronized Outcome exec(@Nullable final Handler<? super Outcome> handler)
                throws OperationException {
            final Record record = this.record != null ? this.record : Record.create();
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids, record,
                    this.criteria, handler);
        }

        public final synchronized Outcome exec(
                @Nullable final Collection<? super Outcome> collection) throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids, this.record,
                    this.criteria, handlerFor(collection));
        }

        protected abstract Outcome doExec(@Nullable Long timeout, URI type,
                @Nullable XPath condition, @Nullable Set<URI> ids, @Nullable Record record,
                @Nullable Criteria criteria, @Nullable Handler<? super Outcome> handler)
                throws OperationException;

    }

    public abstract static class Delete extends Operation {

        private final URI type;

        @Nullable
        private List<XPath> conditions;

        @Nullable
        private Set<URI> ids;

        protected Delete(final Map<String, String> namespaces, final URI type) {
            super(namespaces);
            this.type = checkType(type);
            this.conditions = null;
            this.ids = null;
        }

        @Override
        public Delete timeout(@Nullable final Long timeout) {
            return (Delete) super.timeout(timeout);
        }

        @Override
        public Delete namespaces(@Nullable final Map<String, String> namespaces) {
            return (Delete) super.namespaces(namespaces);
        }

        public final synchronized Delete conditions(@Nullable final XPath... conditions) {
            return conditions(conditions == null ? null : Arrays.asList(conditions));
        }

        public final synchronized Delete conditions(
                @Nullable final Iterable<? extends XPath> conditions) {
            this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
            return this;
        }

        public final synchronized Delete condition(final String condition,
                final Object... arguments) throws ParseException {
            this.conditions = add(this.conditions,
                    XPath.parse(this.namespaces, condition, arguments));
            return this;
        }

        public final synchronized Delete condition(final URI property,
                final Object... allowedValues) {
            this.conditions = add(this.conditions, conditionFor(property, allowedValues));
            return this;
        }

        public final synchronized Delete ids(@Nullable final URI... ids) {
            return ids(ids == null ? null : Arrays.asList(ids));
        }

        public final synchronized Delete ids(@Nullable final Iterable<? extends URI> ids) {
            this.ids = ids == null ? null : ImmutableSet.copyOf(ids);
            return this;
        }

        public final synchronized Outcome exec() throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids, null);
        }

        public final synchronized Outcome exec(@Nullable final Handler<? super Outcome> handler)
                throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids, handler);
        }

        public final synchronized Outcome exec(
                @Nullable final Collection<? super Outcome> collection) throws OperationException {
            return doExec(this.timeout, this.type, merge(this.conditions), this.ids,
                    handlerFor(collection));
        }

        protected abstract Outcome doExec(@Nullable Long timeout, URI type,
                @Nullable XPath condition, @Nullable Set<URI> ids,
                @Nullable Handler<? super Outcome> handler) throws OperationException;

    }

    public abstract static class Match extends Operation {

        private static final Map<URI, URI> COMPONENT_NORMALIZATION_MAP = //
        ImmutableMap.<URI, URI>builder().put(KS.RESOURCE, KS.RESOURCE)
                .put(KS.MATCHED_RESOURCE, KS.RESOURCE).put(KS.MENTION, KS.MENTION)
                .put(KS.MATCHED_MENTION, KS.MENTION).put(KS.ENTITY, KS.ENTITY)
                .put(KS.MATCHED_ENTITY, KS.ENTITY).build();

        private final Map<URI, Set<XPath>> conditions;

        private final Map<URI, Set<URI>> ids;

        private final Map<URI, Set<URI>> properties;

        protected Match(final Map<String, String> namespaces) {
            super(namespaces);
            this.conditions = Maps.newHashMap();
            this.ids = Maps.newHashMap();
            this.properties = Maps.newHashMap();
        }

        @Override
        public Match timeout(@Nullable final Long timeout) {
            return (Match) super.timeout(timeout);
        }

        @Override
        public Match namespaces(@Nullable final Map<String, String> namespaces) {
            return (Match) super.namespaces(namespaces);
        }

        public final synchronized Match conditions(@Nullable final URI component,
                @Nullable final XPath... conditions) {
            return conditions(component, conditions == null ? null : Arrays.asList(conditions));
        }

        public final synchronized Match conditions(@Nullable final URI component,
                @Nullable final Iterable<? extends XPath> conditions) {
            if (component == null) {
                this.conditions.clear();
            } else {
                this.conditions.put(checkComponent(component), conditions == null ? null
                        : ImmutableSet.copyOf(conditions));
            }
            return this;
        }

        public final synchronized Match condition(final URI component, final String condition,
                final Object... arguments) throws ParseException {
            final URI comp = checkComponent(component);
            final XPath cond = XPath.parse(this.namespaces, condition, arguments);
            this.conditions.put(comp, add(this.conditions.get(comp), cond));
            return this;
        }

        public final synchronized Match condition(final URI component, final URI property,
                final Object... allowedValues) {
            final URI comp = checkComponent(component);
            final XPath cond = conditionFor(property, allowedValues);
            this.conditions.put(comp, add(this.conditions.get(comp), cond));
            return this;
        }

        public final synchronized Match ids(@Nullable final URI component,
                @Nullable final URI... ids) {
            return ids(component, ids == null ? null : Arrays.asList(ids));
        }

        public final synchronized Match ids(@Nullable final URI component,
                @Nullable final Iterable<? extends URI> ids) {
            if (component == null) {
                this.ids.clear();
            } else {
                this.ids.put(checkComponent(component),
                        ids == null ? null : ImmutableSet.copyOf(ids));
            }
            return this;
        }

        // no call = exclude class; empty = all properties

        public final synchronized Match properties(@Nullable final URI component,
                @Nullable final URI... properties) {
            return properties(component, properties == null ? null : Arrays.asList(properties));
        }

        public final synchronized Match properties(@Nullable final URI component,
                @Nullable final Iterable<? extends URI> properties) {
            if (component == null) {
                this.properties.clear();
            } else {
                this.properties.put(checkComponent(component), properties == null ? null
                        : ImmutableSet.copyOf(properties));
            }
            return this;
        }

        public final synchronized Stream<Record> exec() throws OperationException {
            final Map<URI, XPath> conditions = Maps.newHashMap();
            for (final URI component : this.conditions.keySet()) {
                conditions.put(component, merge(this.conditions.get(component)));
            }
            return doExec(this.timeout, conditions, this.ids, this.properties);
        }

        protected abstract Stream<Record> doExec(@Nullable final Long timeout,
                final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
                final Map<URI, Set<URI>> properties) throws OperationException;

        private URI checkComponent(final URI component) {
            final URI uri = COMPONENT_NORMALIZATION_MAP.get(component);
            Preconditions.checkArgument(uri != null, "Invalid match component %s", component);
            return uri;
        }

    }

    // TODO: add missing namespaces based on available namespaces

    /**
     * SPARQL query operation.
     * <p>
     * This operation evaluates a SPARQL query on the KnowledgeStore SPARQL endpoint. All SPARQL
     * query forms are supported:
     * <ul>
     * <li>SELECT queries return a {@code Stream} of {@code BindingSet}s (call
     * {@link Sparql#execTuples()});</li>
     * <li>CONSTRUCT and DESCRIBE queries return a {@code Stream} of {@link Statement}s (call
     * {@link Sparql#execTriples()});</li>
     * <li>ASK queries return a boolean result (call {@link Sparql#execBoolean()}).</li>
     * </ul>
     * </p>
     * <p>
     * The operation can be configured, as usual, by specifying a timeout and supplying optional
     * namespace bindings to be used for parsing the SPARQL expression. In addition,
     * {@link Sparql#defaultGraphs(Iterable) default} and {@link Sparql#namedGraphs(Iterable)
     * named} graphs can be specified at the operation level, overriding the corresponding
     * declarations in the {@code FROM} and {@code FROM NAMED} clauses of the SPARQL query
     * expression.
     * </p>
     */
    public abstract static class Sparql extends Operation {

        private static final Pattern PLACEHOLDER_PATTERN = Pattern
                .compile("(?:(?<=\\A|[^\\\\]))([$][$])(?:(?=\\z|.))");

        private final String expression;

        @Nullable
        private Set<URI> defaultGraphs;

        @Nullable
        private Set<URI> namedGraphs;

        /**
         * Creates a new {@code Sparql} operation instance (to be used in {@code Session}
         * implementations).
         * 
         * @param namespaces
         *            an <b>immutable</b> map of inherited namespaces, possibly null
         * @param expression
         *            the SPARQL query expression, not null and possibly containing {@code $$}
         *            placeholders
         * @param arguments
         *            the values to assign to {@code $$} placeholders
         * @throws ParseException
         *             in case the query expression is not valid
         */
        protected Sparql(final Map<String, String> namespaces, final String expression,
                final Object... arguments) throws ParseException {
            super(namespaces);
            this.expression = expand(expression, arguments);
            this.defaultGraphs = null;
            this.namedGraphs = null;
        }

        @Override
        public Sparql timeout(@Nullable final Long timeout) {
            return (Sparql) super.timeout(timeout);
        }

        @Override
        public Sparql namespaces(@Nullable final Map<String, String> namespaces) {
            return (Sparql) super.namespaces(namespaces);
        }

        /**
         * Sets the optional default graphs for the query, overriding the default graphs specified
         * in the <tt>FROM</tt> clause. Passing null will remove any default graph previously set.
         * 
         * @param defaultGraphs
         *            a vararg array with the non-null URIs of the default graphs, possibly empty;
         *            pass null to remove any default graph previously set
         * @return this operation object for call chaining
         */
        public final synchronized Sparql defaultGraphs(@Nullable final URI... defaultGraphs) {
            return defaultGraphs(defaultGraphs == null ? null : Arrays.asList(defaultGraphs));
        }

        /**
         * Sets the optional default graphs for the query, overriding default graphs specified in
         * the <tt>FROM</tt> clause. Passing null will remove any default graph previously set.
         * 
         * @param defaultGraphs
         *            an {@code Iterable} with the non-null URIs of the default graphs, possibly
         *            empty; pass null to remove any default graph previously set
         * @return this operation object for call chaining
         */
        public final synchronized Sparql defaultGraphs(//
                @Nullable final Iterable<URI> defaultGraphs) {
            this.defaultGraphs = defaultGraphs == null ? null : ImmutableSet.copyOf(defaultGraphs);
            return this;
        }

        /**
         * Sets the optional named graphs for the query, overriding named graphs specified in the
         * <tt>FROM NAMED</tt> clause. Passing null will remove any named graph previously set.
         * 
         * @param namedGraphs
         *            a vararg array with the non-null URIs of the named graphs, possibly empty;
         *            pass null to remove any named graph previously set
         * @return this operation object for call chaining
         */
        public final synchronized Sparql namedGraphs(@Nullable final URI... namedGraphs) {
            return namedGraphs(namedGraphs == null ? null : Arrays.asList(namedGraphs));
        }

        /**
         * Sets the optional named graphs for the query, overriding named graphs specified in the
         * <tt>FROM NAMED</tt> clause. Passing null will remove any named graph previously set.
         * 
         * @param namedGraphs
         *            an {@code Iterable} with the non-null URIs of the named graphs, possibly
         *            empty; pass null to remove any named graph previously set
         * @return this operation object for call chaining
         */
        public final synchronized Sparql namedGraphs(@Nullable final Iterable<URI> namedGraphs) {
            this.namedGraphs = namedGraphs == null ? null : ImmutableSet.copyOf(namedGraphs);
            return this;
        }

        /**
         * Evaluates the query, returning its boolean result; applicable to ASK queries.
         * 
         * @return the boolean result of the query
         * @throws OperationException
         *             on failure (see possible outcome status codes)
         */
        public final synchronized boolean execBoolean() throws OperationException {
            return doExec(this.timeout, Boolean.class, this.expression, this.defaultGraphs,
                    this.namedGraphs).getUnique();
        }

        /**
         * Evaluates the query, returning a {@code Stream} with the resulting {@code Statement}s;
         * applicable to CONSTRUCT and DESCRIBE queries.
         * 
         * @return the resulting {@code Stream} of {@code Statement}s
         * @throws OperationException
         *             on failure (see possible outcome status codes)
         */
        public final synchronized Stream<Statement> execTriples() throws OperationException {
            return doExec(this.timeout, Statement.class, this.expression, this.defaultGraphs,
                    this.namedGraphs);
        }

        /**
         * Evaluates the query, returning a {@code Stream} with the resulting {@code BindingSet}s;
         * applicable to SELECT queries. After access to the returned {@code Stream}, a
         * {@code List<String>} with output variables can be obtained by querying the
         * {@code Stream} metadata attribute {@code variables}.
         * 
         * @return the resulting {@code Stream} of {@code BindingSet}s
         * @throws OperationException
         *             on failure (see possible outcome status codes)
         */
        public final synchronized Stream<BindingSet> execTuples() throws OperationException {
            return doExec(this.timeout, BindingSet.class, this.expression, this.defaultGraphs,
                    this.namedGraphs);
        }

        /**
         * Implementation method responsible of executing the SPARQL operation.
         * 
         * @param timeout
         *            the optional timeout for the operation; null if there is no timeout
         * @param type
         *            the expected type of elements for the resulting {@code Stream}; either
         *            {@link Statement}, {@link BindingSet} or {@link Boolean}
         * @param expression
         *            the SPARQL query expression
         * @param defaultGraphs
         *            the optional set of default graphs overriding the ones possibly specified in
         *            the {@code FROM} query clause; if null, no override should take place
         * @param namedGraphs
         *            the optional set of named graphs overriding the ones possibly specified in
         *            the {@code FROM NAMED} query clause; if null, no override should take place
         * @param <T>
         *            the type of result elements
         * @return a {@code Stream} with the result of the query
         * @throws OperationException
         *             in case of failure (see possible outcome status codes)
         */
        protected abstract <T> Stream<T> doExec(@Nullable final Long timeout, final Class<T> type,
                final String expression, @Nullable final Set<URI> defaultGraphs,
                @Nullable final Set<URI> namedGraphs) throws OperationException;

        private String expand(final String expression, final Object... arguments)
                throws ParseException {
            int expansions = 0;
            String result = expression;
            final Matcher matcher = PLACEHOLDER_PATTERN.matcher(expression);
            try {
                if (matcher.find()) {
                    final StringBuilder builder = new StringBuilder();
                    int last = 0;
                    do {
                        Object arg = arguments[expansions++];
                        builder.append(expression.substring(last, matcher.start(1)));
                        builder.append(arg instanceof Number ? arg : Data.toString(arg, null,
                                false));
                        last = matcher.end(1);
                    } while (matcher.find());
                    builder.append(expression.substring(last, expression.length()));
                    result = builder.toString();
                }
            } catch (final IndexOutOfBoundsException ex) {
                throw new ParseException(expression, "No argument supplied for placeholder #"
                        + expansions);
            }
            if (expansions != arguments.length) {
                throw new ParseException(expression, "Expression string contains " + expansions
                        + " placholders, but " + arguments.length + " arguments where supplied");
            }
            return result;
        }

    }

}
