package eu.fbk.knowledgestore.server.http;

import ch.qos.logback.access.jetty.RequestLogImpl;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import eu.fbk.knowledgestore.ForwardingKnowledgeStore;
import eu.fbk.knowledgestore.KnowledgeStore;
import eu.fbk.knowledgestore.Session;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.runtime.Component;
import eu.fbk.knowledgestore.server.http.jaxrs.*;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

// TODO: check
// https://jersey.java.net/apidocs/2.5.1/jersey/org/glassfish/jersey/server/filter/UriConnegFilter.html

// TODO: add DOSFilter and QOSFilter from jetty-servlets

// TODO: getters and tostring

public class HttpServer extends ForwardingKnowledgeStore implements Component {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);

    private static final String DEFAULT_HOST = "0.0.0.0";

    private static final String DEFAULT_PATH = "/";

    private static final int DEFAULT_HTTP_PORT = 8080;

    private static final int DEFAULT_HTTPS_PORT = 8443;

    private static final int DEFAULT_ACCEPTORS = -1; // -1 = platform specific

    private static final int DEFAULT_SELECTORS = -1; // -1 = platform specific

    private static final KeystoreConfig DEFAULT_KEYSTORE_CONFIG = new KeystoreConfig(
            HttpServer.class.getResource("HttpServer.jks").toString(), "kspass", null, null);

    private static final String DEFAULT_REALM = "KnowledgeStore";

    private static final UIConfig DEFAULT_UI_CONFIG = UIConfig.builder().build();

    private static final long STOP_TIMEOUT = 1000; // wait 1 s before forcing closure

    private final KnowledgeStore delegate;

    private final Server server;

    private HttpServer(final Builder builder) {
        this.delegate = Preconditions.checkNotNull(builder.delegate);
        this.server = createJettyServer(builder);
    }

    @Override
    protected KnowledgeStore delegate() {
        return this.delegate;
    }

    @Override
    public void init() throws IOException {

        if (this.delegate instanceof Component) {
            ((Component) this.delegate).init();
        }

        try {
            this.server.start();
            if (LOGGER.isInfoEnabled()) {
                final StringBuilder builder = new StringBuilder("Jetty ").append(
                        Server.getVersion()).append(" started, listening on ");
                builder.append(((ServerConnector) this.server.getConnectors()[0]).getHost());
                builder.append(", port(s)");
                for (final Connector connector : this.server.getConnectors()) {
                    builder.append(" ").append(((ServerConnector) connector).getPort());
                }
                builder.append(", base '").append(this.server.getAttribute("base")).append('\'');
                LOGGER.info(builder.toString());
            }

        } catch (final Exception ex) {
            throw ex instanceof RuntimeException ? (RuntimeException) ex
                    : new RuntimeException(ex);
        }
    }

    @Override
    public Session newSession() throws IllegalStateException {
        return this.delegate.newSession();
    }

    @Override
    public Session newSession(final String username, final String password)
            throws IllegalStateException {
        return this.delegate.newSession(username, password);
    }

    @Override
    public boolean isClosed() {
        return this.delegate.isClosed();
    }

    @Override
    public void close() {
        try {
            this.server.setStopTimeout(STOP_TIMEOUT);
            this.server.stop();
            LOGGER.info("Jetty {} stopped", Server.getVersion());
        } catch (final Exception ex) {
            // Just log a warning without additional detail, as Jetty already prints log info
            LOGGER.warn("Jetty {} stopped (with errors)", Server.getVersion());
        } finally {
            this.delegate.close();
        }
    }

    private Server createJettyServer(final Builder builder) {

        // Retrieve common attributes
        final String host = MoreObjects.firstNonNull(builder.host, DEFAULT_HOST);
        String base = MoreObjects.firstNonNull(builder.path, DEFAULT_PATH);
        final int acceptors = MoreObjects.firstNonNull(builder.acceptors, DEFAULT_ACCEPTORS);
        final int selectors = MoreObjects.firstNonNull(builder.selectors, DEFAULT_SELECTORS);
        final boolean debug = Boolean.TRUE.equals(builder.debug);
        base = base.startsWith("/") ? base : "/" + base;

        // Retrieve the scheduler
        final ExecutorService scheduler = Data.getExecutor();

        // Create HTTP server
        final Server server = new Server(new ExecutorThreadPool(scheduler) {

            @Override
            protected void doStop() throws Exception {
                // Do nothing (default behaviour is shutting down the supplied scheduler, which is
                // something we do not want.
            }

        });
        server.setDumpAfterStart(false);
        server.setDumpBeforeStop(false);
        server.setStopAtShutdown(true);
        // server.setSendDateHeader(true);
        // server.setSendServerVersion(true); // custom header sent

        // enable JMX support
        final MBeanContainer jmxContainer = new MBeanContainer(
                ManagementFactory.getPlatformMBeanServer());
        server.addBean(jmxContainer);

        // configure security
        final SecurityConfig sc = builder.securityConfig;
        String realm = DEFAULT_REALM;
        Set<String> anonymousRoles = SecurityConfig.ALL_ROLES;
        if (sc != null) {
            realm = MoreObjects.firstNonNull(sc.getRealm(), realm);
            anonymousRoles = sc.getAnonymousRoles();
            final String userdb = sc.getUserdbURL().toString();
            final HashLoginService login = new HashLoginService();
            login.setName(sc.getRealm());
            login.setConfig(userdb);
            login.setRefreshInterval(userdb.startsWith("file://") ? 60000 : 0);
            server.addBean(login);
        }

        // add HTTP and HTTPS connectors
        final int httpPort = MoreObjects.firstNonNull(builder.httpPort, DEFAULT_HTTP_PORT);
        final int httpsPort = MoreObjects.firstNonNull(builder.httpsPort, DEFAULT_HTTPS_PORT);
        if (httpPort > 0) {
            Preconditions.checkArgument(httpPort < 65536, "Invalid HTTP port %s", httpPort);
            server.addConnector(createServerConnector(server, host, httpPort, acceptors,
                    selectors, createHttpConnectionFactory(httpsPort, false)));
        }
        if (httpsPort > 0) {
            Preconditions.checkArgument(httpsPort < 65536 && httpsPort != httpPort,
                    "Invalid HTTPS port %s", httpsPort);
            final KeystoreConfig kc = MoreObjects.firstNonNull(builder.keystoreConfig,
                    DEFAULT_KEYSTORE_CONFIG);
            server.addConnector(createServerConnector(server, host, httpsPort, acceptors,
                    selectors, createSSLConnectionFactory(kc),
                    createHttpConnectionFactory(httpsPort, true)));
        }

        // configure Webapp handler
        final WebAppContext webappHandler = new WebAppContext();
        webappHandler.setThrowUnavailableOnStartupException(true);
        webappHandler.setTempDirectory(new File(System.getProperty("java.io.tmpdir") + "/"
                + UUID.randomUUID().toString()));
        webappHandler.setResourceBase(getClass().getClassLoader()
                .getResource("eu/fbk/knowledgestore/server/http/jaxrs").toExternalForm());
        webappHandler.setParentLoaderPriority(true);
        webappHandler.setMaxFormContentSize(Integer.MAX_VALUE);
        webappHandler.setDescriptor(createWebXml(realm, anonymousRoles, debug).toString());
        webappHandler.setConfigurationDiscovered(false);
        webappHandler.setCompactPath(true);
        webappHandler.setClassLoader(getClass().getClassLoader());
        webappHandler.setContextPath(base);
        webappHandler.getServletContext().setAttribute(Application.STORE_ATTRIBUTE, this);
        webappHandler.getServletContext().setAttribute(Application.TRACING_ATTRIBUTE, debug);
        webappHandler.getServletContext().setAttribute(Application.UI_ATTRIBUTE,
                MoreObjects.firstNonNull(builder.uiConfig, DEFAULT_UI_CONFIG));
        webappHandler.getServletContext().setAttribute(
                Application.RESOURCE_ATTRIBUTE,
                ImmutableList.of(Root.class, Files.class,
                        eu.fbk.knowledgestore.server.http.jaxrs.Resources.class, Mentions.class,
                        // Entities.class, Axioms.class, Match.class,
                        Sparql.class, SparqlUpdate.class));

        // configure request logging using logback access
        RequestLogHandler requestLogHandler = null;
        if (builder.logConfigLocation != null) {
            final RequestLogImpl requestLog = new RequestLogImpl();
            requestLog.setQuiet(true);
            requestLog.setResource((builder.logConfigLocation.startsWith("/") ? "" : "/")
                    + builder.logConfigLocation);
            requestLogHandler = new RequestLogHandler();
            requestLogHandler.setRequestLog(requestLog);
        }

        // add proxy pass reverse support
        final Handler handler = webappHandler;

        // configure request statistics collector (accessible via JMX)
        final StatisticsHandler statHandler = new StatisticsHandler();

        // configure CLF logging
        if (requestLogHandler == null) {
            statHandler.setHandler(handler);
        } else {
            final HandlerCollection multiHandler = new HandlerCollection();
            multiHandler.setHandlers(new Handler[] { handler, requestLogHandler });
            statHandler.setHandler(multiHandler);
        }

        server.setHandler(statHandler);
        server.setAttribute("base", base);

        // return configured server
        return server;
    }

    private ServerConnector createServerConnector(final Server server, final String host,
            final int port, final int acceptors, final int selectors,
            final ConnectionFactory... connectionFactories) {

        final ServerConnector connector = new ServerConnector(server, null, null, null, acceptors,
                selectors, connectionFactories);
        connector.setHost(host);
        connector.setPort(port);
        connector.setReuseAddress(true); // to avoid respawning issues with supervisord
        connector.addBean(new ConnectorStatistics());
        return connector;
    }

    private HttpConnectionFactory createHttpConnectionFactory(final int httpsPort,
            final boolean customizer) {

        final HttpConfiguration config = new HttpConfiguration();
        if (httpsPort > 0) {
            config.setSecureScheme("https");
            config.setSecurePort(httpsPort);
            if (customizer) {
                config.addCustomizer(new SecureRequestCustomizer());
            }
        }

        config.setOutputBufferSize(32 * 1024);
        config.setRequestHeaderSize(8 * 1024);
        config.setResponseHeaderSize(8 * 1024);
        config.setSendServerVersion(true);
        config.setSendDateHeader(true);
        final HttpConnectionFactory factory = new HttpConnectionFactory(config);
        return factory;
    }

    private SslConnectionFactory createSSLConnectionFactory(final KeystoreConfig keystore) {

        final Resource keystoreResource = Resource.newResource(keystore.getURL());

        final SslContextFactory contextFactory = new SslContextFactory();
        contextFactory.setEnableCRLDP(false);
        contextFactory.setEnableOCSP(false);
        contextFactory.setNeedClientAuth(false);
        contextFactory.setWantClientAuth(false);
        contextFactory.setValidateCerts(false);
        contextFactory.setValidatePeerCerts(false);
        contextFactory.setRenegotiationAllowed(false); // avoid vulnerability
        contextFactory.setSessionCachingEnabled(true); // optimization
        contextFactory.setKeyStoreResource(keystoreResource);
        contextFactory.setKeyStorePassword(keystore.getPassword());
        contextFactory.setCertAlias(keystore.getAlias());
        contextFactory.setKeyStoreType(keystore.getType());
        contextFactory.setIncludeCipherSuites("TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
                "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA",
                "SSL_RSA_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
                "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA"); // check RFC 5430

        final SslConnectionFactory factory = new SslConnectionFactory(contextFactory, "http/1.1");
        return factory;
    }

    private URL createWebXml(final String realmName,
            final Iterable<? extends String> anonymousRoles, final boolean debug) {
        try {
            // load web.xml from classpath
            String webxml = Resources.toString(HttpServer.class.getResource("HttpServer.web.xml"),
                    Charsets.UTF_8);

            // disable TeeFilter if not in debug mode
            if (!debug) {
                int index = webxml.indexOf("<filter-name>TeeFilter</filter-name>");
                final int start1 = webxml.lastIndexOf("<filter>", index);
                final int end1 = webxml.indexOf("</filter>", index) + 9;
                index = webxml.indexOf("<filter-name>TeeFilter</filter-name>", end1);
                final int start2 = webxml.lastIndexOf("<filter-mapping>", index);
                final int end2 = webxml.indexOf("</filter-mapping>", index) + 17;
                webxml = webxml.substring(0, start1) + webxml.substring(end1, start2)
                        + webxml.substring(end2);
            }

            // replace references to <realm-name>
            StringBuilder builder = new StringBuilder();
            int index = 0;
            int last = 0;
            while ((index = webxml.indexOf("<realm-name>", last)) >= 0) {
                index = index + 12;
                builder.append(webxml.substring(last, index));
                builder.append(realmName);
                index = webxml.indexOf("</realm-name>", index);
                last = index + 13;
                builder.append(webxml.substring(index, last));
            }
            builder.append(webxml.substring(last));
            webxml = builder.toString();

            // disable security constraints for anonymous roles
            builder = new StringBuilder();
            last = 0;
            while ((index = webxml.indexOf("<security-constraint>", last)) >= 0) {
                builder.append(webxml.substring(last, index));
                last = webxml.indexOf("</security-constraint>", index) + 22;
                final int start1 = index;
                int end1 = -1;
                final Set<String> roles = Sets.newHashSet();
                int start2 = index + 21;
                while (true) {
                    index = webxml.indexOf("<role-name>", start2);
                    if (index < 0 || index >= last) {
                        break;
                    }
                    end1 = end1 >= 0 ? end1 : index;
                    index += 11;
                    final int roleStart = index;
                    index = webxml.indexOf("</role-name>", index);
                    roles.add(webxml.substring(roleStart, index));
                    start2 = index + 12;
                }
                last = webxml.indexOf("</security-constraint>", start2) + 22;
                for (final String role : anonymousRoles) {
                    roles.remove(role);
                }
                if (!roles.isEmpty()) {
                    builder.append(webxml.substring(start1, end1));
                    for (final String role : roles) {
                        builder.append("<role-name>").append(role).append("</role-name>");
                    }
                    builder.append(webxml.substring(start2, last));
                }
            }
            builder.append(webxml.substring(last));
            webxml = builder.toString();

            // save filtered web.xml to temporary file, returning associated URL
            final File file = File.createTempFile("knowledgestore_", ".web.xml");
            file.deleteOnExit();
            com.google.common.io.Files.write(webxml, file, Charsets.UTF_8);
            return file.toURI().toURL();

        } catch (final Exception ex) {
            throw new Error("Cannot configure web.xml descriptor: " + ex.getMessage(), ex);
        }
    }

    public static Builder builder(final KnowledgeStore delegate) {
        return new Builder(delegate);
    }

    public static class Builder {

        final KnowledgeStore delegate;

        @Nullable
        String host;

        @Nullable
        Integer httpPort;

        @Nullable
        Integer httpsPort;

        @Nullable
        String path;

        @Nullable
        String proxyHttpRoot; // TODO: handle this

        @Nullable
        String proxyHttpsRoot; // TODO: handle this

        @Nullable
        KeystoreConfig keystoreConfig;

        @Nullable
        Integer acceptors;

        @Nullable
        Integer selectors;

        @Nullable
        SecurityConfig securityConfig;

        @Nullable
        UIConfig uiConfig;

        @Nullable
        Boolean debug;

        @Nullable
        String logConfigLocation;

        Builder(final KnowledgeStore store) {
            this.delegate = Preconditions.checkNotNull(store);
        }

        public Builder host(@Nullable final String host) {
            this.host = host;
            return this;
        }

        public Builder httpPort(@Nullable final Integer httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder httpsPort(@Nullable final Integer httpsPort) {
            this.httpsPort = httpsPort;
            return this;
        }

        public Builder path(@Nullable final String path) {
            this.path = path;
            return this;
        }

        public Builder proxyHttpRoot(@Nullable final String proxyHttpRoot) {
            this.proxyHttpRoot = proxyHttpRoot;
            return this;
        }

        public Builder proxyHttpsRoot(@Nullable final String proxyHttpsRoot) {
            this.proxyHttpsRoot = proxyHttpsRoot;
            return this;
        }

        public Builder acceptors(@Nullable final Integer acceptors) {
            this.acceptors = acceptors;
            return this;
        }

        public Builder selectors(@Nullable final Integer selectors) {
            this.selectors = selectors;
            return this;
        }

        public Builder keystoreConfig(@Nullable final KeystoreConfig keystoreConfig) {
            this.keystoreConfig = keystoreConfig;
            return this;
        }

        public Builder securityConfig(@Nullable final SecurityConfig securityConfig) {
            this.securityConfig = securityConfig;
            return this;
        }

        public Builder uiConfig(@Nullable final UIConfig uiConfig) {
            this.uiConfig = uiConfig;
            return this;
        }

        public Builder debug(@Nullable final Boolean debug) {
            this.debug = debug;
            return this;
        }

        public Builder logConfigLocation(@Nullable final String logConfigLocation) {
            this.logConfigLocation = logConfigLocation;
            return this;
        }

        public HttpServer build() {
            return new HttpServer(this);
        }

    }

}
