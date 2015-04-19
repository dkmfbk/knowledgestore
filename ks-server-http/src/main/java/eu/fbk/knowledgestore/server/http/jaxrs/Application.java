package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.ServletContext;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.message.internal.HttpDateFormat;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.mvc.mustache.MustacheMvcFeature;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.ParseException;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.internal.Logging;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.internal.jaxrs.Serializer;
import eu.fbk.knowledgestore.server.http.UIConfig;

public final class Application extends javax.ws.rs.core.Application {

    public static final String STORE_ATTRIBUTE = "store";

    public static final String TRACING_ATTRIBUTE = "tracing";

    public static final String RESOURCE_ATTRIBUTE = "resource";

    public static final String UI_ATTRIBUTE = "ui";

    public static final int DEFAULT_TIMEOUT = 600000; // 600 sec; TODO: make this customizable

    public static final int GRACE_PERIOD = 5000; // 5 sec extra beyond timeout

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private static final String SERVER = String.format("KnowledgeStore/%s Jetty/%s",
            Util.getVersion("eu.fbk.knowledgestore", "ks-core", "devel"), Server.getVersion());

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");

    private static ThreadLocal<URI> INVOCATION_ID = new ThreadLocal<URI>();

    private static ThreadLocal<URI> OBJECT_ID = new ThreadLocal<URI>();

    private static ThreadLocal<List<MediaType>> ACCEPT = new ThreadLocal<List<MediaType>>(); // \m/

    private static ThreadLocal<Future<?>> TIMEOUT_FUTURE = new ThreadLocal<Future<?>>();

    private static long invocationCounter = 0;

    private final UIConfig uiConfig;

    private final KnowledgeStore store;

    private final Set<Class<?>> classes;

    private final Set<Object> singletons;

    private final Map<String, Object> properties;

    private int pendingModifications;

    private Date lastModified;

    @SuppressWarnings("unchecked")
    public Application(@Context final ServletContext context) {
        this((UIConfig) context.getAttribute(UI_ATTRIBUTE), //
                (KnowledgeStore) context.getAttribute(STORE_ATTRIBUTE), //
                (Boolean) context.getAttribute(TRACING_ATTRIBUTE), //
                (Iterable<? extends Class<?>>) context.getAttribute(RESOURCE_ATTRIBUTE));
    }

    public Application(final UIConfig uiConfig, final KnowledgeStore store,
            final Boolean enableTracing, final Iterable<? extends Class<?>> resourceClasses) {

        // keep track of KS and UI config
        this.store = Preconditions.checkNotNull(store);
        this.uiConfig = Preconditions.checkNotNull(uiConfig);

        // define JAX-RS classes
        final ImmutableSet.Builder<Class<?>> classes = ImmutableSet.builder();
        classes.add(DeflateEncoder.class);
        classes.add(GZipEncoder.class);
        for (final Class<?> resourceClass : resourceClasses) {
            classes.add(resourceClass);
        }
        classes.add(Converter.class);
        classes.add(Filter.class);
        classes.add(Mapper.class);
        classes.add(Serializer.class);
        classes.add(MustacheMvcFeature.class);
        this.classes = classes.build();

        // define singletons
        this.singletons = ImmutableSet.of();

        // define JAX-RS properties
        final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(ServerProperties.APPLICATION_NAME, "KnowledgeStore");
        if (Boolean.TRUE.equals(enableTracing)) {
            properties.put(ServerProperties.TRACING, "ALL");
            properties.put(ServerProperties.TRACING_THRESHOLD, "TRACE");

            // note: in a particular instance we observed 1GB ram being used by Jersey monitoring
            // code after 1h uptime and only three requests received by the server (!). Therefore,
            // enable these settings only if strictly necessary
            properties.put(ServerProperties.MONITORING_STATISTICS_ENABLED, true);
            properties.put(ServerProperties.MONITORING_STATISTICS_MBEANS_ENABLED, true);
        }
        properties.put(ServerProperties.WADL_FEATURE_DISABLE, false);
        properties.put(ServerProperties.JSON_PROCESSING_FEATURE_DISABLE, true); // JSONLD used
        properties.put(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true); // not used
        properties.put(ServerProperties.MOXY_JSON_FEATURE_DISABLE, true); // not used
        properties.put(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 8192); // default value
        properties.put(MustacheMvcFeature.CACHE_TEMPLATES, true);
        properties.put(MustacheMvcFeature.TEMPLATE_BASE_PATH,
                "/eu/fbk/knowledgestore/server/http/jaxrs/");
        this.properties = properties.build();

        // Initialize globally last modified variables
        this.pendingModifications = 0;
        this.lastModified = new Date();
    }

    public UIConfig getUIConfig() {
        return this.uiConfig;
    }

    public KnowledgeStore getStore() {
        return this.store;
    }

    @Override
    public Set<Class<?>> getClasses() {
        return this.classes;
    }

    @Override
    public Set<Object> getSingletons() {
        return this.singletons;
    }

    @Override
    public Map<String, Object> getProperties() {
        return this.properties;
    }

    public synchronized Date getLastModified() {
        return this.pendingModifications == 0 ? this.lastModified : new Date();
    }

    synchronized void beginModification() {
        ++this.pendingModifications;
    }

    synchronized void endModification() {
        --this.pendingModifications;
        if (this.pendingModifications == 0) {
            this.lastModified = new Date();
        }
    }

    static Application unwrap(final javax.ws.rs.core.Application application) {
        if (application instanceof Application) {
            return (Application) application;
        } else if (application instanceof ResourceConfig) {
            return (Application) ((ResourceConfig) application).getApplication();
        }
        Preconditions.checkNotNull(application, "Null application");
        throw new IllegalArgumentException("Invalid application class "
                + application.getClass().getName());
    }

    @Provider
    static final class Converter implements ParamConverterProvider {

        private static final ParamConverter<URI> URI_CONVERTER = new ParamConverter<URI>() {

            @Override
            public URI fromString(final String string) {
                try {
                    return (URI) Data.parseValue(string, Data.getNamespaceMap());
                } catch (final ParseException ex) {
                    throw new WebApplicationException(ex.getMessage(), Status.BAD_REQUEST);
                }
            }

            @Override
            public String toString(final URI uri) {
                return Data.toString(uri, null); // no QNames for max compatibility
            }

        };

        private static final ParamConverter<XPath> XPATH_CONVERTER = new ParamConverter<XPath>() {

            @Override
            public XPath fromString(final String string) {
                try {
                    return XPath.parse(Data.getNamespaceMap(), string);
                } catch (final ParseException ex) {
                    throw new WebApplicationException(ex.getMessage(), Status.BAD_REQUEST);
                }
            }

            @Override
            public String toString(final XPath xpath) {
                return xpath.toString();
            }

        };

        private static final ParamConverter<Criteria> CRITERIA_CONVERTER = new ParamConverter<Criteria>() {

            @Override
            public Criteria fromString(final String string) {
                try {
                    return Criteria.parse(string, Data.getNamespaceMap());
                } catch (final ParseException ex) {
                    throw new WebApplicationException(ex.getMessage(), Status.BAD_REQUEST);
                }
            }

            @Override
            public String toString(final Criteria criteria) {
                return criteria.toString();
            }

        };

        @SuppressWarnings("unchecked")
        @Override
        public <T> ParamConverter<T> getConverter(final Class<T> rawType, final Type genericType,
                final Annotation[] annotations) {

            if (rawType.equals(URI.class)) {
                return (ParamConverter<T>) URI_CONVERTER;
            } else if (rawType.equals(XPath.class)) {
                return (ParamConverter<T>) XPATH_CONVERTER;
            } else if (rawType.equals(Criteria.class)) {
                return (ParamConverter<T>) CRITERIA_CONVERTER;
            }
            return null;
        }

    }

    @Provider
    @PreMatching
    static final class Filter implements ContainerRequestFilter, ContainerResponseFilter,
            WriterInterceptor {

        private static final String PROPERTY_TIMESTAMP = "timestamp";

        @Override
        public void filter(final ContainerRequestContext request) throws IOException {

            // Keep timestamp
            final long timestamp = System.currentTimeMillis();
            request.setProperty(PROPERTY_TIMESTAMP, timestamp);

            // Extract Accept types either from headers or query parameters
            List<MediaType> acceptTypes = request.getAcceptableMediaTypes();
            String accept = request.getUriInfo().getQueryParameters()
                    .getFirst(Protocol.PARAMETER_ACCEPT);
            if (accept == null) {
                accept = MoreObjects.firstNonNull(request.getHeaderString(HttpHeaders.ACCEPT),
                        "*/*");
            } else {
                request.getHeaders().putSingle(HttpHeaders.ACCEPT, accept);
                acceptTypes = Lists.newArrayList();
                for (final String type : accept.split(",")) {
                    acceptTypes.add(MediaType.valueOf(type.trim()));
                }
            }

            // Extract timeout parameter
            long timeout = DEFAULT_TIMEOUT;
            try {
                final Thread thread = Thread.currentThread();
                final String timeoutString = Strings.nullToEmpty(
                        request.getUriInfo().getQueryParameters()
                                .getFirst(Protocol.PARAMETER_TIMEOUT)).trim();
                final long theTimeout = "".equals(timeoutString) ? DEFAULT_TIMEOUT : Long
                        .parseLong(timeoutString) * 1000;
                timeout = theTimeout;
                TIMEOUT_FUTURE.set(Data.getExecutor().schedule(new Runnable() {

                    @Override
                    public void run() {
                        synchronized (Filter.this) {
                            LOGGER.info("Http: Request timed out after {} ms", theTimeout);
                            thread.interrupt(); // Let's hope this will enforce the timeout
                        }
                    }

                }, timeout + GRACE_PERIOD, TimeUnit.MILLISECONDS));
            } catch (final Throwable ex) {
                // Ignore invalid timeout
            }

            // Extract information from the request
            final URI invocationID = extractInvocationID(request);
            final URI objectID = extractObjectID(request);
            final String username = extractUsername(request);
            final boolean chunkedInput = extractChunkedInput(request);
            final boolean cachingEnabled = extractCachingEnabled(request);

            // Store relevant attribute as request properties (not visible to REST resources)
            INVOCATION_ID.set(invocationID);
            OBJECT_ID.set(objectID);
            ACCEPT.set(acceptTypes); // required by mapper

            // Update the MDC context with the invocation ID, so to bind log messages to it.
            // Invocation ID will be removed from MDC when request processing is complete
            MDC.put(Logging.MDC_CONTEXT, invocationID.stringValue());

            // Configure REST resources for this request
            Resource.begin(invocationID, objectID, username, chunkedInput, cachingEnabled, timeout);

            // Store invocation ID and record type as request headers to be used by serializers
            request.getHeaders().putSingle(Protocol.HEADER_INVOCATION, invocationID.stringValue());

            // Log the request
            if (LOGGER.isDebugEnabled()) {
                final String etag = request.getHeaders().getFirst(HttpHeaders.IF_NONE_MATCH);
                final String lastModified = reformatDate(request.getHeaders().getFirst(
                        HttpHeaders.IF_MODIFIED_SINCE));
                final StringBuilder builder = new StringBuilder("Http: ");
                builder.append(request.getMethod());
                builder.append(' ').append(request.getUriInfo().getRequestUri());
                builder.append(' ').append(accept);
                final String type = request.getHeaderString(HttpHeaders.CONTENT_TYPE);
                if (type != null) {
                    builder.append(' ').append(type);
                }
                final String encoding = request.getHeaderString(HttpHeaders.CONTENT_ENCODING);
                if (encoding != null) {
                    builder.append(' ').append(encoding);
                }
                if (etag != null) {
                    builder.append(' ').append(etag);
                }
                if (lastModified != null) {
                    builder.append('/').append(lastModified);
                }
                final Principal user = request.getSecurityContext().getUserPrincipal();
                if (user != null) {
                    builder.append(' ').append(user.getName());
                }
                LOGGER.debug(builder.toString());
            }
        }

        @Override
        public void filter(final ContainerRequestContext request,
                final ContainerResponseContext response) throws IOException {

            try {
                // Retrieve relevant attributes of the request
                final URI invocationID = INVOCATION_ID.get();

                // Set response headers
                response.getHeaders().putSingle("Server", SERVER);
                response.getHeaders().add(Protocol.HEADER_INVOCATION, invocationID.stringValue());

                // Log the response
                if (LOGGER.isDebugEnabled()) {
                    final long elapsed = System.currentTimeMillis()
                            - (Long) request.getProperty(PROPERTY_TIMESTAMP);
                    final StringBuilder builder = new StringBuilder();
                    builder.append("Http: status ");
                    builder.append(response.getStatus());
                    if (response.hasEntity()) {
                        final String etag = response.getHeaderString(HttpHeaders.ETAG);
                        if (etag != null) {
                            builder.append(", ").append(etag);
                        } else {
                            builder.append(", ").append(response.getMediaType());
                        }
                        try {
                            final Date lastModified = response.getLastModified();
                            if (lastModified != null) {
                                synchronized (DATE_FORMAT) {
                                    builder.append(", ").append(DATE_FORMAT.format(lastModified));
                                }
                            }
                        } catch (final Throwable ex) {
                            // ignore parsing errors
                        }
                    }
                    builder.append(", ").append(elapsed).append(" ms");
                    LOGGER.debug(builder.toString());
                }

            } finally {
                // Restore MDC and Resource thread-level data if processing ends here (no entity)
                if (response.getEntity() == null) {
                    complete();
                }
            }
        }

        @Override
        public void aroundWriteTo(final WriterInterceptorContext context) throws IOException,
                WebApplicationException {

            try {
                // Emit the response body
                context.proceed();

            } finally {
                // Restore MDC and Resource thread-level data
                complete();
            }
        }

        private void complete() {
            Resource.end();
            final Future<?> future = TIMEOUT_FUTURE.get();
            if (future != null) {
                TIMEOUT_FUTURE.set(null);
                future.cancel(false);
                // synchronization force waiting for the timeout runnable to complete
                synchronized (Filter.this) {
                    Thread.interrupted(); // clear interrupted status
                }
            }
            MDC.remove(Logging.MDC_CONTEXT);
        }

        private static URI extractInvocationID(final ContainerRequestContext request) {
            final String id = request.getHeaderString(Protocol.HEADER_INVOCATION);
            if (id != null) {
                try {
                    return Data.getValueFactory().createURI(id);
                } catch (final Throwable ex) {
                    // not valid: ignore
                }
            }
            final long ts = System.currentTimeMillis();
            long counterSnapshot;
            synchronized (Application.class) {
                ++invocationCounter;
                if (invocationCounter < ts) {
                    invocationCounter = ts;
                }
                counterSnapshot = invocationCounter;
            }
            return Data.getValueFactory().createURI("req:" + Long.toString(counterSnapshot, 32));
        }

        private static URI extractObjectID(final ContainerRequestContext request) {
            final List<String> ids = request.getUriInfo().getQueryParameters().get("id");
            if (ids != null && ids.size() == 1) {
                try {
                    return (URI) Data.parseValue(ids.get(0), Data.getNamespaceMap());
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            return null;
        }

        private static String extractUsername(final ContainerRequestContext request) {
            final SecurityContext context = request.getSecurityContext();
            if (context != null && context.getUserPrincipal() != null) {
                final Principal principal = context.getUserPrincipal();
                if (principal != null) {
                    return principal.getName();
                }
            }
            return null;
        }

        private static boolean extractChunkedInput(final ContainerRequestContext request) {
            final List<String> values = request.getHeaders().get(Protocol.HEADER_CHUNKED);
            return values != null && values.size() == 1 && "true".equalsIgnoreCase(values.get(0));
        }

        private static boolean extractCachingEnabled(final ContainerRequestContext request) {
            final List<String> values = request.getHeaders().get("Cache-Control");
            if (values != null && values.size() == 1) {
                try {
                    final CacheControl cacheControl = CacheControl.valueOf(values.get(0));
                    return !cacheControl.isNoCache() && !cacheControl.isNoStore();
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            return true; // default
        }

        @Nullable
        private static String reformatDate(@Nullable final String httpDate) {
            if (httpDate != null) {
                try {
                    final Date date = HttpDateFormat.readDate(httpDate);
                    synchronized (DATE_FORMAT) {
                        return DATE_FORMAT.format(date);
                    }
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            return null;
        }

    }

    @Provider
    @Produces(Protocol.MIME_TYPES_RDF)
    static final class Mapper implements ExceptionMapper<Throwable> {

        private static final List<MediaType> RDF_TYPES;

        static {
            final ImmutableList.Builder<MediaType> builder = ImmutableList.builder();
            for (final String token : Protocol.MIME_TYPES_RDF.split(",")) {
                builder.add(MediaType.valueOf(token.trim()));
            }
            RDF_TYPES = builder.build();
        }

        private static MediaType selectType() {
            for (final MediaType acceptableType : ACCEPT.get()) {
                for (final MediaType supportedType : RDF_TYPES) {
                    if (acceptableType.isCompatible(supportedType)) {
                        return supportedType;
                    }
                }
            }
            return RDF_TYPES.get(0);
        }

        @Override
        public Response toResponse(final Throwable throwable) {

            // Try to unwrap the exception
            final Throwable ex = throwable instanceof RuntimeException
                    && throwable.getCause() instanceof OperationException ? throwable.getCause()
                    : throwable;

            // Retrieve relevant attributes of the request
            final URI invocationID = INVOCATION_ID.get();
            final URI objectID = OBJECT_ID.get();

            // Determine HTTP status and Outcome from the exception
            int httpStatus;
            MultivaluedMap<String, Object> headers = null;
            Outcome outcome = null;

            if (ex instanceof OperationException) {
                outcome = ((OperationException) ex).getOutcome();
                httpStatus = outcome.getStatus().getHTTPStatus();

            } else if (ex instanceof WebApplicationException) {
                Outcome.Status status = null;
                final Response exResponse = ((WebApplicationException) ex).getResponse();
                headers = exResponse.getHeaders();
                httpStatus = exResponse.getStatus();
                if (httpStatus >= 400 && httpStatus != Status.PRECONDITION_FAILED.getStatusCode()) {
                    status = Outcome.Status.valueOf(httpStatus);
                    outcome = Outcome.create(status, invocationID, objectID, exResponse
                            .hasEntity() ? exResponse.getEntity().toString() : ex.getMessage());
                }

            } else {
                httpStatus = Status.INTERNAL_SERVER_ERROR.getStatusCode();
                outcome = Outcome.create(Outcome.Status.ERROR_UNEXPECTED, invocationID, objectID,
                        ex.getMessage() + " [" + ex.getClass().getSimpleName() + "]");
            }

            // Log the exception in case of server error
            if (httpStatus >= 500) {
                LOGGER.error("Http: reporting server error " + httpStatus, ex);
            } else if (httpStatus >= 400) {
                LOGGER.debug("Http: reporting client error: " + httpStatus + " - "
                        + ex.getMessage() + " (" + ex.getClass().getSimpleName() + ")");
            }

            // Build and return the response.
            final ResponseBuilder builder = Response.status(httpStatus);
            if (outcome != null && httpStatus >= 400
                    && httpStatus != Status.PRECONDITION_FAILED.getStatusCode()) {
                final CacheControl cacheControl = new CacheControl();
                cacheControl.setNoStore(true);
                builder.entity(
                        new GenericEntity<Stream<Outcome>>(Stream.create(outcome),
                                Protocol.STREAM_OF_OUTCOMES.getType())).cacheControl(cacheControl)
                        .type(selectType());
            }
            if (headers != null) {
                for (final Map.Entry<String, List<Object>> entry : headers.entrySet()) {
                    final String name = entry.getKey();
                    for (final Object value : entry.getValue()) {
                        builder.header(name, value);
                    }
                }
            }
            return builder.build();
        }
    }

}
