package eu.fbk.knowledgestore.client;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HttpHeaders;
import com.google.common.net.UrlEscapers;
import eu.fbk.knowledgestore.AbstractKnowledgeStore;
import eu.fbk.knowledgestore.AbstractSession;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.Outcome.Status;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.*;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.internal.jaxrs.Serializer;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;
import eu.fbk.knowledgestore.vocabulary.NIE;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.message.GZipEncoder;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.core.*;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

// TODO: decide where to place the Configuration class

public final class Client extends AbstractKnowledgeStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private static final String USER_AGENT = String.format(
            "KnowledgeStore/%s Apache-HttpClient/%s",
            Util.getVersion("eu.fbk.knowledgestore", "ks-core", "devel"),
            Util.getVersion("org.apache.httpcomponents", "httpclient", "unknown"));

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");

    private static final String MIME_TYPE_RDF = "application/x-tql"; // "text/turtle";

    private static final String MIME_TYPE_TUPLE = "text/tab-separated-values";

    private static final String MIME_TYPE_BOOLEAN = "text/boolean";

    private static final int DEFAULT_MAX_CONNECTIONS = 2;

    private static final boolean DEFAULT_VALIDATE_SERVER = true;

    private static final int DEFAULT_CONNECTION_TIMEOUT = 1000; // 1 sec

    private static final boolean DEFAULT_COMPRESSION_ENABLED = LoggerFactory.getLogger(
            "org.apache.http.wire").isDebugEnabled();;

    private final String serverURL;

    private final boolean compressionEnabled;

    private final HttpClientConnectionManager connectionManager;

    private final javax.ws.rs.client.Client client;

    private final Map<String, String> targets; // path -> URI

    private Client(final Builder builder) {

        String url = Preconditions.checkNotNull(builder.serverURL);
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        final int timeout;
        timeout = MoreObjects.firstNonNull(builder.connectionTimeout, DEFAULT_CONNECTION_TIMEOUT);
        Preconditions.checkArgument(timeout >= 0, "Invalid connection timeout %d", timeout);

        this.serverURL = url;
        this.compressionEnabled = MoreObjects.firstNonNull(builder.compressionEnabled,
                DEFAULT_COMPRESSION_ENABLED);
        this.connectionManager = createConnectionManager(
                MoreObjects.firstNonNull(builder.maxConnections, DEFAULT_MAX_CONNECTIONS),
                MoreObjects.firstNonNull(builder.validateServer, DEFAULT_VALIDATE_SERVER));
        this.client = createJaxrsClient(this.connectionManager, timeout, builder.proxy);
        this.targets = Maps.newConcurrentMap();
    }

    public synchronized String getServerURL() {
        checkNotClosed();
        return this.serverURL;
    }

    @Override
    protected Session doNewSession(@Nullable final String username, @Nullable final String password) {
        return new SessionImpl(username, password);
    }

    @Override
    protected void doClose() {
        try {
            this.client.close();
        } finally {
            this.connectionManager.shutdown();
        }
    }

    private static PoolingHttpClientConnectionManager createConnectionManager(
            final int maxConnections, final boolean validateServer) {

        // Setup SSLContext and HostnameVerifier based on validateServer parameter
        final SSLContext sslContext;
        HostnameVerifier hostVerifier;
        try {
            if (validateServer) {
                sslContext = SSLContext.getDefault();
                hostVerifier = new DefaultHostnameVerifier();
            } else {
                sslContext = SSLContext.getInstance(Protocol.HTTPS_PROTOCOLS[0]);
                sslContext.init(null, new TrustManager[] { new X509TrustManager() {

                    @Override
                    public void checkClientTrusted(final X509Certificate[] chain,
                            final String authType) {
                    }

                    @Override
                    public void checkServerTrusted(final X509Certificate[] chain,
                            final String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                } }, null);
                hostVerifier = NoopHostnameVerifier.INSTANCE;
            }
        } catch (final Throwable ex) {
            throw new RuntimeException("SSL configuration failed", ex);
        }

        // Create HTTP connection factory
        final ConnectionSocketFactory httpConnectionFactory = PlainConnectionSocketFactory
                .getSocketFactory();

        // Create HTTPS connection factory
        final ConnectionSocketFactory httpsConnectionFactory = new SSLConnectionSocketFactory(
                sslContext, Protocol.HTTPS_PROTOCOLS, Protocol.HTTPS_CIPHER_SUITES, hostVerifier);

        // Create pooled connection manager
        final PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", httpConnectionFactory)
                        .register("https", httpsConnectionFactory).build());

        // Setup max concurrent connections
        manager.setMaxTotal(maxConnections);
        manager.setDefaultMaxPerRoute(maxConnections);
        manager.setValidateAfterInactivity(1000); // validate connection after 1s idle
        return manager;
    }

    private static javax.ws.rs.client.Client createJaxrsClient(
            final HttpClientConnectionManager connectionManager, final int connectionTimeout,
            @Nullable final ProxyConfig proxy) {

        // Configure requests
        final RequestConfig requestConfig = RequestConfig.custom()//
                .setExpectContinueEnabled(false) //
                .setRedirectsEnabled(false) //
                .setConnectionRequestTimeout(connectionTimeout) //
                .setConnectTimeout(connectionTimeout) //
                .setSocketTimeout(10000)
                .build();

        // Configure client
        final ClientConfig config = new ClientConfig();
        config.connectorProvider(new ApacheConnectorProvider());
        config.property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager);
        config.property(ApacheClientProperties.REQUEST_CONFIG, requestConfig);
        config.property(ApacheClientProperties.DISABLE_COOKIES, true); // not needed
        config.property(ClientProperties.REQUEST_ENTITY_PROCESSING,
                RequestEntityProcessing.CHUNKED); // required to stream data to the server
        if (proxy != null) {
            config.property(ClientProperties.PROXY_URI, proxy.getURL());
            config.property(ClientProperties.PROXY_USERNAME, proxy.getUsername());
            config.property(ClientProperties.PROXY_PASSWORD, proxy.getPassword());
        }

        // Register filter and custom serializer
        config.register(Serializer.class);
        config.register(GZipEncoder.class);

        // Create and return a configured JAX-RS client
        return ClientBuilder.newClient(config);
    }

    private final class SessionImpl extends AbstractSession {

        private final String authorization;

        SessionImpl(@Nullable final String username, @Nullable final String password) {
            super(Data.newNamespaceMap(Data.newNamespaceMap(), Data.getNamespaceMap()), username,
                    password);
            final String actualUsername = MoreObjects.firstNonNull(username, "");
            final String actualPassword = MoreObjects.firstNonNull(password, "");
            final String authorizationString = actualUsername + ":" + actualPassword;
            final byte[] authorizationBytes = authorizationString.getBytes(Charsets.ISO_8859_1);
            this.authorization = "Basic " + BaseEncoding.base64().encode(authorizationBytes);
        }

        @Override
        protected Status doFail(final Throwable ex, final AtomicReference<String> message)
                throws Throwable {

            if (ex instanceof WebApplicationException) {
                final Response response = ((WebApplicationException) ex).getResponse();
                try {
                    final RDFFormat format = RDFFormat.forMIMEType(response.getMediaType()
                            .toString());
                    final Outcome outcome = Outcome.decode(
                            RDFUtil.readRDF((InputStream) response.getEntity(), format, null,
                                    null, false), false).getUnique();
                    message.set(outcome.getMessage());
                    return outcome.getStatus();
                } catch (final Throwable ex2) {
                    LOGGER.error("Unable to decode error body", ex2);
                    return Status.valueOf(response.getStatus());
                } finally {
                    response.close();
                }

            } else if (ex instanceof ResponseProcessingException) {
                final Response response = ((ResponseProcessingException) ex).getResponse();
                try {
                    final StringBuilder builder = new StringBuilder(
                            "Client side error (server response: ");
                    builder.append(response.getStatus());
                    if (response.hasEntity()) {
                        final String etag = response.getHeaderString(HttpHeaders.ETAG);
                        builder.append(", ").append(etag != null ? etag : response.getMediaType());
                        final Date lastModified = response.getLastModified();
                        if (lastModified != null) {
                            synchronized (DATE_FORMAT) {
                                builder.append(", ").append(DATE_FORMAT.format(lastModified));
                            }
                        }
                    }
                    message.set(builder.toString());
                    return Status.valueOf(response.getStatus());
                } finally {
                    response.close();
                }

            } else {
                return super.doFail(ex, message);
            }
        }

        @Override
        @Nullable
        protected Representation doDownload(@Nullable final Long timeout, final URI id,
                @Nullable final Set<String> mimeTypes, final boolean useCaches) throws Throwable {

            final String query = query(Protocol.PARAMETER_ID, id);

            final Map<String, Object> headers = Maps.newHashMap();
            if (mimeTypes != null) {
                headers.put(HttpHeaders.ACCEPT, mimeTypes);
            }
            if (!useCaches) {
                final CacheControl cacheControl = new CacheControl();
                cacheControl.setNoStore(true);
                headers.put(HttpHeaders.CACHE_CONTROL, cacheControl);
            }

            try {
                return invoke(HttpMethod.GET, Protocol.PATH_REPRESENTATIONS, query, headers, null,
                        new GenericType<Representation>(Representation.class), timeout);

            } catch (final WebApplicationException ex) {
                if (ex.getResponse().getStatus() == 404) {
                    ex.getResponse().close();
                    return null;
                }
                throw ex;
            }
        }

        @Override
        protected Outcome doUpload(@Nullable final Long timeout, final URI id,
                final Representation representation) throws Exception {

            final String path = Protocol.PATH_REPRESENTATIONS;
            final String query = query(Protocol.PARAMETER_ID, id);
            final Entity<?> entity = representation == null ? null : entity(representation);
            return invoke(HttpMethod.PUT, path, query, null, entity, Protocol.STREAM_OF_OUTCOMES,
                    timeout).getUnique();
        }

        @Override
        protected long doCount(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids) throws Throwable {

            final String path = Protocol.pathFor(type) + "/" + Protocol.SUBPATH_COUNT;
            final String query = query(Protocol.PARAMETER_CONDITION, condition,
                    Protocol.PARAMETER_ID, ids);
            final Statement result = invoke(HttpMethod.GET, path, query, null, null,
                    Protocol.STREAM_OF_STATEMENTS, timeout).getUnique();
            return Data.convert(result.getObject(), Long.class);
        }

        @Override
        protected Stream<Record> doRetrieve(@Nullable final Long timeout, final URI type,
                @Nullable final XPath condition, @Nullable final Set<URI> ids,
                @Nullable final Set<URI> properties, @Nullable final Long offset,
                @Nullable final Long limit) throws Throwable {

            final String path = Protocol.pathFor(type);
            final String query = query(//
                    Protocol.PARAMETER_CONDITION, condition, //
                    Protocol.PARAMETER_ID, ids, //
                    Protocol.PARAMETER_PROPERTY, properties, //
                    Protocol.PARAMETER_OFFSET, offset, //
                    Protocol.PARAMETER_LIMIT, limit);
            final Stream<Record> result = invoke(HttpMethod.GET, path, query, null, null,
                    Protocol.STREAM_OF_RECORDS, timeout);
            result.setProperty("types", ImmutableSet.of(type));
            return result;
        }

        @Override
        protected void doCreate(@Nullable final Long timeout, final URI type,
                final Stream<? extends Record> records, final Handler<? super Outcome> handler)
                throws Exception {

            final String path = Protocol.pathFor(type) + "/" + Protocol.SUBPATH_CREATE;
            final Entity<?> entity = entity(records, type);
            final Stream<Outcome> result = invoke(HttpMethod.POST, path, null, null, entity,
                    Protocol.STREAM_OF_OUTCOMES, timeout);
            result.toHandler(handler);
        }

        @Override
        protected void doMerge(@Nullable final Long timeout, final URI type,
                final Stream<? extends Record> records, final Criteria criteria,
                final Handler<? super Outcome> handler) throws Exception {

            final String path = Protocol.pathFor(type) + "/" + Protocol.SUBPATH_MERGE;
            final String query = query(Protocol.PARAMETER_CRITERIA, criteria);
            final Entity<?> entity = entity(records, type);
            final Stream<Outcome> result = invoke(HttpMethod.POST, path, query, null, entity,
                    Protocol.STREAM_OF_OUTCOMES, timeout);
            result.toHandler(handler);
        }

        @Override
        protected void doUpdate(@Nullable final Long timeout, final URI type,
                final XPath condition, final Set<URI> ids, final Record record,
                final Criteria criteria, final Handler<? super Outcome> handler) throws Exception {

            final String path = Protocol.pathFor(type) + "/" + Protocol.SUBPATH_UPDATE;
            final String query = query(//
                    Protocol.PARAMETER_CRITERIA, criteria, //
                    Protocol.PARAMETER_CONDITION, condition, //
                    Protocol.PARAMETER_ID, ids);
            final Entity<?> entity = entity(Stream.create(record), type);
            final Stream<Outcome> result = invoke(HttpMethod.POST, path, query, null, entity,
                    Protocol.STREAM_OF_OUTCOMES, timeout);
            result.toHandler(handler);
        }

        @Override
        protected void doDelete(@Nullable final Long timeout, final URI type,
                final XPath condition, final Set<URI> ids, final Handler<? super Outcome> handler)
                throws Exception {

            final String path = Protocol.pathFor(type) + "/" + Protocol.SUBPATH_DELETE;
            final String query = query(//
                    Protocol.PARAMETER_CONDITION, condition, //
                    Protocol.PARAMETER_ID, ids);
            final Stream<Outcome> result = invoke(HttpMethod.POST, path, query, null, null,
                    Protocol.STREAM_OF_OUTCOMES, timeout);
            result.toHandler(handler);
        }

        @Override
        protected Stream<Record> doMatch(@Nullable final Long timeout,
                final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
                final Map<URI, Set<URI>> properties) throws Exception {
            // TODO
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <T> Stream<T> doSparql(@Nullable final Long timeout, final Class<T> type,
                final String expression, final Set<URI> defaultGraphs, final Set<URI> namedGraphs)
                throws Exception {

            final String path = Protocol.PATH_SPARQL;
            final String query = query(//
                    Protocol.PARAMETER_QUERY, expression, //
                    Protocol.PARAMETER_DEFAULT_GRAPH, defaultGraphs, //
                    Protocol.PARAMETER_NAMED_GRAPH, namedGraphs);
            GenericType<?> responseType;
            if (type == Statement.class) {
                responseType = Protocol.STREAM_OF_STATEMENTS;
            } else if (type == BindingSet.class) {
                responseType = Protocol.STREAM_OF_TUPLES;
            } else if (type == Boolean.class) {
                responseType = Protocol.STREAM_OF_BOOLEANS;
            } else {
                throw new Error("Unexpected result type: " + type);
            }
            return (Stream<T>) invoke(HttpMethod.GET, path, query, null, null, responseType,
                    timeout);
        }

        @Override
        protected Outcome doSparqlUpdate(@Nullable Long timeout, @Nullable Stream<? extends Statement> statements) throws Throwable {
            final String path = Protocol.PATH_UPDATE;
            final GenericEntity<Stream<Statement>> entity = new GenericEntity<Stream<Statement>>((Stream<Statement>) statements, Protocol.STREAM_OF_STATEMENTS.getType());
            Entity<?> entityEntity = Entity.entity(entity, new Variant(MediaType.valueOf(MIME_TYPE_RDF), (String) null, Client.this.compressionEnabled ? "gzip" : "identity"));
            return invoke(HttpMethod.POST, path, null, null, entityEntity, Protocol.STREAM_OF_OUTCOMES, timeout).getUnique();
        }

        @Override
        protected Outcome doSparqlDelete(@Nullable Long timeout, @Nullable Stream<? extends Statement> statements) throws Throwable {
            final String path = Protocol.PATH_DELETE;
            final GenericEntity<Stream<Statement>> entity = new GenericEntity<Stream<Statement>>((Stream<Statement>) statements, Protocol.STREAM_OF_STATEMENTS.getType());
            Entity<?> entityEntity = Entity.entity(entity, new Variant(MediaType.valueOf(MIME_TYPE_RDF), (String) null, Client.this.compressionEnabled ? "gzip" : "identity"));
            return invoke(HttpMethod.POST, path, null, null, entityEntity, Protocol.STREAM_OF_OUTCOMES, timeout).getUnique();
        }

        private String query(final Object... queryNameValues) {
            final StringBuilder builder = new StringBuilder();
            final Escaper escaper = UrlEscapers.urlFormParameterEscaper();
            String separator = "?";
            for (int i = 0; i < queryNameValues.length; i += 2) {
                final Object name = queryNameValues[i].toString();
                final Object value = queryNameValues[i + 1];
                if (value == null) {
                    continue;
                }
                final Iterable<?> iterable = value instanceof Iterable<?> ? (Iterable<?>) value
                        : ImmutableSet.of(value);
                for (final Object element : iterable) {
                    if (element == null) {
                        continue;
                    }
                    String encoded;
                    if (element instanceof Value && !name.equals(Protocol.PARAMETER_DEFAULT_GRAPH)
                            && !name.equals(Protocol.PARAMETER_NAMED_GRAPH)) {
                        encoded = Data.toString(element, Data.getNamespaceMap());
                    } else {
                        encoded = element.toString();
                    }
                    builder.append(separator).append(name).append("=");
                    builder.append(escaper.escape(encoded));
                    separator = "&";
                }
            }
            return builder.toString();
        }

        private Entity<Representation> entity(final Representation representation) {
            final String mimeType = representation.getMetadata().getUnique(NIE.MIME_TYPE,
                    String.class, MediaType.APPLICATION_OCTET_STREAM);
            final Variant variant = new Variant(MediaType.valueOf(mimeType), (String) null,
                    Client.this.compressionEnabled ? "gzip" : "identity");
            return Entity.entity(representation, variant);
        }

        @SuppressWarnings("unchecked")
        private Entity<GenericEntity<Stream<Record>>> entity(
                final Stream<? extends Record> records, final URI type) {

            records.setProperty("types", ImmutableSet.of(type));
            final GenericEntity<Stream<Record>> entity = new GenericEntity<Stream<Record>>(
                    (Stream<Record>) records, Protocol.STREAM_OF_RECORDS.getType());
            return Entity.entity(entity, new Variant(MediaType.valueOf(MIME_TYPE_RDF),
                    (String) null, Client.this.compressionEnabled ? "gzip" : "identity"));
        }

        private <T> T invoke(final String method, final String path, @Nullable final String query,
                @Nullable final Map<String, Object> headers,
                @Nullable final Entity<?> requestEntity, final GenericType<T> responseType,
                @Nullable final Long timeout) {

            // Determine target URI based on path and stored redirections
            final String action = method + ":" + path;
            final String target = Client.this.targets.get(action);
            final String uri = target != null ? target : Client.this.serverURL + "/" + path;

            // Do a probe first in case we don't know whether the method / URI pair is restricted
            String actualQuery = query;
            Entity<?> actualRequestEntity = requestEntity;
            if (target == null) {
                actualQuery = Strings.isNullOrEmpty(query) ? "?probe=true" : query + "&probe=true";
                if (requestEntity != null) {
                    final Variant variant = requestEntity.getVariant();
                    actualRequestEntity = Entity.entity(new byte[0],
                            new Variant(variant.getMediaType(), (String) null, "identity"));
                }
            }

            // Encode timeout
            if (timeout != null) {
                final long timeoutInSeconds = Math.max(1, timeout / 1000);
                actualQuery = Strings.isNullOrEmpty(actualQuery) ? "?timeout=" + timeoutInSeconds
                        : actualQuery + "&timeout=" + timeoutInSeconds;
            }

            // Determine Accept MIME type based on expected (Java) response type
            String acceptType = MediaType.WILDCARD;
            if (responseType.equals(Protocol.STREAM_OF_RECORDS)
                    || responseType.equals(Protocol.STREAM_OF_OUTCOMES)
                    || responseType.equals(Protocol.STREAM_OF_STATEMENTS)) {
                acceptType = MIME_TYPE_RDF;
            } else if (responseType.equals(Protocol.STREAM_OF_TUPLES)) {
                acceptType = MIME_TYPE_TUPLE;
            } else if (responseType.equals(Protocol.STREAM_OF_BOOLEANS)) {
                acceptType = MIME_TYPE_BOOLEAN;
            }

            // Create an invocation builder for the target URI + query string
            final Invocation.Builder invoker = Client.this.client.target(
                    actualQuery == null ? uri : uri + actualQuery).request(acceptType);

            // Add custom headers, if any.
            if (headers != null) {
                for (final Map.Entry<String, Object> entry : headers.entrySet()) {
                    invoker.header(entry.getKey(), entry.getValue());
                }
            }

            // Add invocation ID and User-Agent headers
            invoker.header(HttpHeaders.USER_AGENT, USER_AGENT);
            invoker.header(Protocol.HEADER_INVOCATION, getInvocationID().stringValue());

            // Reject response compression, if disabled
            invoker.header(HttpHeaders.ACCEPT_ENCODING,
                    Client.this.compressionEnabled ? "gzip, deflate, identity" : "identity");

            // Add credentials IFF the HTTPS scheme is used
            if (uri.startsWith("https")) {
                invoker.header(HttpHeaders.AUTHORIZATION, this.authorization);
            }

            // Log the request
            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder("Http: ");
                builder.append(method).append(' ')
                        .append(actualQuery == null ? uri : uri + actualQuery);
                if (actualRequestEntity != null) {
                    Type type = actualRequestEntity.getEntity().getClass();
                    if (type.equals(GenericEntity.class)) {
                        type = ((GenericEntity<?>) actualRequestEntity.getEntity()).getType();
                    }
                    builder.append(' ').append(Util.formatType(type));
                    builder.append(' ').append(actualRequestEntity.getMediaType());
                }
                if (getUsername() != null) {
                    builder.append(' ').append(getUsername());
                }
                LOGGER.debug(builder.toString());
            }

            // Perform the request
            final long timestamp = System.currentTimeMillis();
            final Response response = actualRequestEntity == null ? invoker.method(method) : //
                    invoker.method(method, actualRequestEntity);
            final long elapsed = System.currentTimeMillis() - timestamp;

            // Log the response
            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder("Http: ");
                builder.append(response.getStatus());
                if (response.hasEntity()) {
                    final String etag = response.getHeaderString(HttpHeaders.ETAG);
                    builder.append(", ").append(etag != null ? etag : response.getMediaType());
                    final Date lastModified = response.getLastModified();
                    if (lastModified != null) {
                        synchronized (DATE_FORMAT) {
                            builder.append(", ").append(DATE_FORMAT.format(lastModified));
                        }
                    }
                }
                builder.append(", ").append(elapsed).append(" ms");
                LOGGER.debug(builder.toString());
            }

            // On redirection, close response, update targets map and try again
            final int status = response.getStatus();
            if (status == 302 || status == 307 || status == 308) {
                response.close();
                String newURI = response.getHeaderString(HttpHeaders.LOCATION);
                final int index = newURI.indexOf('?');
                newURI = index < 0 ? newURI : newURI.substring(0, index);
                Client.this.targets.put(action, newURI);
                LOGGER.debug("Http: stored redirection: {} -> {}", path, newURI);
                return invoke(method, path, query, headers, requestEntity, responseType, timeout);
            }

            // Otherwise, update targets map and either return response or fail
            Client.this.targets.put(action, uri);
            if (status / 100 == 2) {
                if (Representation.class.isAssignableFrom(responseType.getRawType())) {
                    response.bufferEntity();
                }
                final T result = response.readEntity(responseType);
                if (result instanceof Stream<?>) {
                    ((Stream<?>) result).onClose(new Runnable() {

                        @Override
                        public void run() {
                            response.close();
                        }

                    });
                }
                return result;
            } else {
                Util.closeQuietly(response);
                throw new WebApplicationException(response);
            }
        }
    }

    public static Builder builder(final String serverURL) {
        return new Builder(serverURL);
    }

    public static class Builder {

        String serverURL;

        @Nullable
        Integer maxConnections;

        @Nullable
        Integer connectionTimeout;

        @Nullable
        Boolean compressionEnabled;

        @Nullable
        Boolean validateServer;

        @Nullable
        ProxyConfig proxy;

        Builder(final String serverURL) {
            this.serverURL = Preconditions.checkNotNull(serverURL);
        }

        public Builder maxConnections(@Nullable final Integer maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public Builder connectionTimeout(@Nullable final Integer connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder compressionEnabled(@Nullable final Boolean compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return this;
        }

        public Builder validateServer(@Nullable final Boolean validateServer) {
            this.validateServer = validateServer;
            return this;
        }

        public Builder proxy(@Nullable final ProxyConfig proxy) {
            this.proxy = proxy;
            return this;
        }

        public Client build() {
            return new Client(this);
        }

    }

}
