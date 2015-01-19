package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;

import javax.annotation.Nullable;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Variant;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.server.http.UIConfig;

public abstract class Resource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Resource.class);

    private static final ThreadLocal<RequestContext> THREAD_CONTEXT //
    = new ThreadLocal<RequestContext>();

    @Context
    private javax.ws.rs.core.Application application;

    @Context
    private Request request;

    @Context
    private ResourceInfo resource;

    @Context
    private UriInfo uri;

    @Nullable
    private final RequestContext context; // A solution based on injection should be rather used

    Resource() {
        this.context = Preconditions.checkNotNull(THREAD_CONTEXT.get());
    }

    final UIConfig getUIConfig() {
        return Application.unwrap(this.application).getUIConfig();
    }

    final Application getApplication() {
        return Application.unwrap(this.application);
    }

    final KnowledgeStore getStore() {
        return Application.unwrap(this.application).getStore();
    }

    final Session getSession() {
        if (this.context.session == null) {
            this.context.session = getStore().newSession(getUsername(), null);
            this.context.closeables.add(this.context.session);
        }
        return this.context.session;
    }

    final URI getInvocationID() {
        return this.context.invocationID;
    }

    @Nullable
    final URI getObjectID() {
        return this.context.objectID;
    }

    @Nullable
    final String getUsername() {
        return this.context.username;
    }

    final String getMethod() {
        return this.request.getMethod();
    }

    final UriInfo getUriInfo() {
        return this.uri;
    }

    final boolean isChunkedInput() {
        return this.context.chunkedInput;
    }

    final boolean isCachingEnabled() {
        return this.context.cachingEnabled;
    }

    final long getTimeout() {
        return this.context.timeout;
    }

    final <T extends Closeable> T closeQuietly(@Nullable final T closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Throwable ex) {
                LOGGER.error("Exception caught closing " + closeable.getClass().getSimpleName(),
                        ex);
            }
        }
        return closeable;
    }

    final <T extends Closeable> T closeOnCompletion(@Nullable final T closeable) {
        if (closeable != null) {
            this.context.closeables.add(closeable);
        }
        return closeable;
    }

    final void check(final boolean condition, final Outcome.Status errorStatus,
            @Nullable final String errorMessage, final Object... errorArgs)
            throws OperationException {
        if (!condition) {
            throw new OperationException(newOutcome(errorStatus, errorMessage == null ? null
                    : String.format(errorMessage, errorArgs)));
        }
    }

    final <T> T checkNotNull(final T object, final Outcome.Status errorStatus,
            @Nullable final String errorMessage, final Object... errorArgs)
            throws OperationException {
        if (object == null) {
            throw new OperationException(newOutcome(errorStatus, errorMessage == null ? null
                    : String.format(errorMessage, errorArgs)));
        }
        return object;
    }

    final void init(final boolean modification, @Nullable final String responseType)
            throws OperationException {
        doInit(modification, false, responseType, null, null);
    }

    final void init(final boolean modification, @Nullable final String responseType,
            @Nullable final Date getLastModified, @Nullable final String getTag)
            throws OperationException {
        doInit(modification, true, responseType, getLastModified, getTag);
    }

    private void doInit(final boolean modification, final boolean exists,
            @Nullable final String responseType, @Nullable final Date getLastModified,
            @Nullable final String getTag) throws OperationException {

        // Determine returned variant
        this.context.variant = computeVariant(responseType);

        // Evaluate preconditions, based on available parameters (last modified, tag)
        final ResponseBuilder builder;
        if (!exists) {
            builder = this.request.evaluatePreconditions();

        } else {
            // Initialize last modified
            final Date lastModified = getLastModified != null ? getLastModified : getApplication()
                    .getLastModified();

            // Initialize etag
            final EntityTag etag = new EntityTag(String.format("%s,%s,%s", getTag != null ? getTag
                    : Long.toString(lastModified.getTime(), 16), this.context.variant
                    .getMediaType().toString(), this.context.variant.getEncoding()));

            // Check preconditions
            builder = this.request.evaluatePreconditions(lastModified, etag);

            // Store last modified and etag for later inclusion in response, in case of retrieval
            if ("GET".equalsIgnoreCase(this.request.getMethod())
                    || "HEAD".equalsIgnoreCase(this.request.getMethod())) {
                this.context.lastModified = lastModified;
                this.context.etag = etag;
            }
        }

        // If preconditions failed, return the Response built by JAX-RS
        if (builder != null) {
            // Note: no Outcome entity sent here as it can confuse clients; also, in case of 304
            // Not Modified, an entity MUST not be sent.
            throw new WebApplicationException(builder.build());
        }

        // Interrupt processing in case of a probe request
        if (this.uri.getQueryParameters().containsKey(Protocol.PARAMETER_PROBE)) {
            String newURI = this.uri.getRequestUri().toString();
            int start = newURI.indexOf('?' + Protocol.PARAMETER_PROBE);
            if (start < 0) {
                start = newURI.indexOf('&' + Protocol.PARAMETER_PROBE);
            }
            int end = newURI.indexOf('&', start + 1);
            if (end < 0) {
                end = newURI.length();
            }
            newURI = newURI.substring(0, start) + newURI.substring(end);
            final Response redirect = Response.status(Status.FOUND)
                    .location(java.net.URI.create(newURI)).build();
            throw new WebApplicationException(redirect);
        }

        // Register modification; unregister it when request processing completes
        if (modification) {
            getApplication().beginModification();
            closeOnCompletion(new Closeable() {

                @Override
                public void close() throws IOException {
                    getApplication().endModification();
                }

            });
        }
    }

    private Variant computeVariant(@Nullable final String mimeType) throws OperationException {

        // Determine supported media types from supplied type or @Produces annotation
        MediaType[] types = null;
        if (mimeType != null) {
            types = parseMediaTypes(mimeType);
        } else {
            types = new MediaType[] { MediaType.WILDCARD_TYPE };
            final Method method = this.resource.getResourceMethod();
            if (method != null) {
                final Produces produces = method.getAnnotation(Produces.class);
                if (produces != null) {
                    types = parseMediaTypes(produces.value());
                }
            }
        }

        // Determine supported encodings from supplied encoding or using defaults
        final String[] encodings = new String[] { "identity", "gzip", "deflate" };

        // Perform negotiation and return the result, failing if there is no acceptable variant
        final Variant variant = this.request.selectVariant(Variant.mediaTypes(types)
                .encodings(encodings).build());
        check(variant != null, Outcome.Status.ERROR_NOT_ACCEPTABLE, null);
        return variant;
    }

    private MediaType[] parseMediaTypes(final String... strings) {
        final List<MediaType> list = Lists.newArrayList();
        for (final String string : strings) {
            for (final String token : Splitter.on(',').trimResults().omitEmptyStrings()
                    .split(string)) {
                list.add(MediaType.valueOf(token));
            }
        }
        return list.toArray(new MediaType[list.size()]);
    }

    final ResponseBuilder newResponseBuilder(final int status, @Nullable final Object entity,
            @Nullable final GenericType<?> type) {
        return newResponseBuilder(Status.fromStatusCode(status), entity, type);
    }

    final ResponseBuilder newResponseBuilder(final Status status, @Nullable final Object entity,
            @Nullable final GenericType<?> type) {
        Preconditions.checkState(this.context.variant != null);
        final ResponseBuilder builder = Response.status(status);
        if (entity != null) {
            builder.entity(type == null ? entity : new GenericEntity<Object>(entity, type
                    .getType()));
            builder.variant(this.context.variant);
            final CacheControl cacheControl = new CacheControl();
            cacheControl.setNoStore(true);
            if ("GET".equalsIgnoreCase(this.request.getMethod())
                    || "HEAD".equalsIgnoreCase(this.request.getMethod())) {
                builder.lastModified(this.context.lastModified);
                builder.tag(this.context.etag);
                if (isCachingEnabled()) {
                    cacheControl.setNoStore(false);
                    cacheControl.setMaxAge(0); // always stale, must revalidate each time
                    cacheControl.setMustRevalidate(true);
                    cacheControl.setPrivate(getUsername() != null);
                    cacheControl.setNoTransform(true);
                }
            }
            builder.cacheControl(cacheControl);
        }
        return builder;
    }

    final Outcome newOutcome(final Outcome.Status status, @Nullable final String message,
            final Object... messageArgs) {
        return Outcome.create(status, getInvocationID(), getObjectID(), message == null ? null
                : String.format(message, messageArgs));
    }

    final OperationException newException(final Outcome.Status status,
            @Nullable final Throwable cause, @Nullable final String message, final Object... args) {
        String actualMessage = message;
        if (cause != null) {
            actualMessage = message == null ? cause.getMessage() : message + " - "
                    + cause.getMessage();
        }
        return new OperationException(newOutcome(status, actualMessage, args), cause);
    }

    static void begin(final URI invocationID, @Nullable final URI objectID,
            @Nullable final String username, final boolean chunkedInput,
            final boolean cachingEnabled, final long timeout) {
        THREAD_CONTEXT.set(new RequestContext(invocationID, objectID, username, chunkedInput,
                cachingEnabled, timeout));
    }

    static void end() {
        final RequestContext context = THREAD_CONTEXT.get();
        if (context != null) {
            context.close();
            THREAD_CONTEXT.set(null);
        }
    }

    private static final class RequestContext implements Closeable {

        final URI invocationID;

        @Nullable
        final URI objectID;

        @Nullable
        final String username;

        final boolean chunkedInput;

        final boolean cachingEnabled;

        final long timeout;

        final List<Closeable> closeables;

        @Nullable
        Session session;

        @Nullable
        Variant variant;

        @Nullable
        Date lastModified;

        @Nullable
        EntityTag etag;

        private boolean closed;

        RequestContext(final URI invocationID, final URI objectID, final String username,
                final boolean chunkedInput, final boolean cacheEnabled, final long timeout) {
            this.invocationID = Preconditions.checkNotNull(invocationID);
            this.objectID = objectID;
            this.username = username;
            this.chunkedInput = chunkedInput;
            this.cachingEnabled = cacheEnabled;
            this.timeout = timeout;
            this.closeables = Lists.newArrayList();
            this.closed = false;
        }

        @Override
        public void close() {
            if (this.closed) {
                return;
            }
            try {
                for (final Closeable closeable : this.closeables) {
                    try {
                        closeable.close();
                    } catch (final Throwable ex) {
                        LOGGER.error("Error closing " + closeable.getClass().getSimpleName(), ex);
                    }
                }
                this.closeables.clear();
                this.session = null;
                this.variant = null;
                this.lastModified = null;
                this.etag = null;
            } finally {
                this.closed = true;
            }
        }

    }

}
