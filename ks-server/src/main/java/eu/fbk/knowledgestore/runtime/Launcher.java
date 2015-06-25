package eu.fbk.knowledgestore.runtime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.RDFUtil;

/**
 * A general-purpose configurer and launcher of a {@code Component} instance.
 * <p>
 * The {@code Launcher} class provides the {@link #main(String...)} method for instantiating,
 * starting and stopping a {@link Component} instance, which is configured via system properties
 * and command line arguments.
 * </p>
 * <p>
 * The {@code Launcher} class is meant to be used in shell scripts provided by application
 * developers whose goal is to run a service. With respect to application developers, the
 * behaviour of the {@code Launcher} can be configured by supplying a number of system properties
 * (can be easily done when invoking the JVM in scripts). The following (optional) system
 * properties are supported:
 * </p>
 * <ul>
 * <li>{@code launcher.executable} - the name of the command line executable / script, used for
 * logging and generating the help message;</li>
 * <li>{@code launcher.description} - a description telling what the program does, used for
 * generating the help message;</li>
 * <li>{@code launcher.config} - default location of configuration file / resource;</li>
 * <li>{@code launcher.logging} - default location of Logback configuration file (to be used in
 * case the location in the configuration is missing or wrong)</li>
 * </ul>
 * <p>
 * The end user is instead supposed to interact with the {@code Launcher} via command line options
 * and by supplying a configuration file that controls how the component should be instantiated.
 * The following command line options are recognized and documented to end user:
 * </p>
 * <ul>
 * <li>{@code -c, --config} - the location of the configuration file / resource (if not supplied,
 * the default is used);</li>
 * <li>{@code -v, --version} - causes the program to display version and copyright information,
 * and then terminate;</li>
 * <li>{@code -h, --help} - causes the program to display the help message, and then terminate.</li>
 * </ul>
 * <p>
 * Concerning the configuration, it must be supplied in an RDF file (any syntax supported by
 * Sesame is fine) whose content is fed to {@link Factory} to instantiate the {@code Component}
 * and its dependencies. Inside the configuration file, triples having as subject
 * {@code <obj:launcher>} are specified by the user to control details of the execution
 * environment (threading, logging) and to specify which component to instantiate. More in
 * details, the following triples are recognized:
 * </p>
 * <ul>
 * <li>{@code <obj:launcher> <java:logConfig> "LOCATION"} - supplies the location of the logback
 * configuration file;</li>
 * <li>{@code <obj:launcher> <java:threadName> "PATTERN"} - supplies the pattern for thread names
 * (default is {@code worker-%02d});</li>
 * <li>{@code <obj:launcher> <java:threadCount> "COUNT"^^^xsd:int} - supplies the number of
 * threads in the pool (default is 32);</li>
 * <li>{@code <obj:launcher> <java:component> <java:....>} - specifies the component to
 * instantiate and run as service.</li>
 * </ul>
 * <p>
 * The {@code main()} method retrieves system properties and command line options. It then handles
 * {@code -v} and {@code -h} requests, if supplied, otherwise proceeds with loading the
 * configuration and launching the component. Two operation modes are supported:
 * </p>
 * <ul>
 * <li><i>Standalone execution</i>. Configuration is read, logging is configured and the the
 * component is instantiated and started. Then the program waits for incoming SIGINT / SIGUSR2
 * signals or user input from the console. Key {@code q} / SIGING (CTRL-C) causes the component to
 * be stopped and the program to terminate. Key {@code r} / SIGUSR2 causes the component to be
 * stopped and reinstantiated / restarted, with the configuration being reloaded. Key {@code i}
 * causes status information to be displayed on STDOUT. Status information includes uptime,
 * memory, GC and threads statistics; in addition, {@link ThreadMXBean#findDeadlockedThreads()} is
 * used to detect and report possible thread deadlocks.</li>
 * <li><i>Execution via Apache Commons-Daemon</i>. This is achieved by using
 * {@code org.apache.commons.daemon.support.DaemonWrapper} and configuring it to lanch this class,
 * supplying additional arguments {@code __start} to start the application and {@code __stop} to
 * stop it. In this case no signal handling or console monitoring is performed, relying on
 * Commons-Daemon for managing the lifecycle of the program.</li>
 * </ul>
 * <p>
 * The {@code main()} method returns as exit codes the following 'pseudo' standard values (see
 * {@code sysexit.h}):
 * </p>
 * <ul>
 * <li>0 - success;</li>
 * <li>64 - command line syntax errors;</li>
 * <li>78 - configuration errors;</li>
 * <li>74 - I/O errors during component configuration / initialization;</li>
 * <li>69 - other error.</li>
 * </ul>
 */
public final class Launcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Launcher.class);

    private static final int EX_OK = 0; // success (sysexit.h)

    private static final int EX_USAGE = 64; // command used incorrectly (sysexit.h)

    private static final int EX_CONFIG = 78; // something unconfigured/misconfigured (sysexit.h)

    private static final int EX_IOERR = 74; // some error occurred while douing I/O (sysexit.h)

    private static final int EX_UNAVAILABLE = 69; // catch-all when something fails (sysexit.h)

    private static final String SIGNAL_SHUTDOWN = "INT";

    private static final String SIGNAL_RELOAD = "USR2";

    private static final String SIGNAL_STATUS = "STATUS"; // actually not a proper signal

    private static final String PROGRAM_EXECUTABLE = retrieveProperty("launcher.executable",
            String.class, "ks");

    @Nullable
    private static final String PROGRAM_DESCRIPTION = retrieveProperty("launcher.description",
            String.class, null);

    private static final String PROGRAM_DISCLAIMER = retrieveResource(Launcher.class.getName()
            .replace('.', '/') + ".disclaimer");

    private static final String PROGRAM_VERSION = retrieveVersion();

    private static final String DEFAULT_CONFIG = retrieveProperty("launcher.config", String.class,
            "config.xml");

    private static final String DEFAULT_THREAD_NAME = "worker-%02d";

    private static final int DEFAULT_THREAD_COUNT = 32;

    private static final String DEFAULT_LOG_CONFIG = retrieveProperty("launcher.logging",
            String.class, "logback.xml");

    private static final String PROPERTY_LOG_CONFIG = "logConfig";

    private static final String PROPERTY_THREAD_COUNT = "threadCount";

    private static final String PROPERTY_THREAD_NAME = "threadName";

    private static final String PROPERTY_COMPONENT = "component";

    private static final URI LAUNCHER_URI = new URIImpl("obj:launcher");

    private static final int WIDTH = 80;

    private static Component component;

    /**
     * Program entry point. See class documentation for the supported features.
     * 
     * @param args
     *            command line arguments
     */
    public static void main(final String... args) {

        // Configure command line options
        final Options options = new Options();
        options.addOption("c", "config", true, "use service configuration file / classpath "
                + "resource (default '" + DEFAULT_CONFIG + "')");
        options.addOption("v", "version", false,
                "display version and copyright information, then exit");
        options.addOption("h", "help", false, "display usage information, then exit");

        // Initialize exit status
        int status = EX_OK;

        try {
            // Parse command line and handle different commands
            final CommandLine cmd = new GnuParser().parse(options, args);
            if (cmd.hasOption("v")) {
                // Show version and copyright (http://www.gnu.org/prep/standards/standards.html)
                System.out.println(String.format(
                        "%s (FBK KnowledgeStore) %s\njava %s bit (%s) %s\n%s", PROGRAM_EXECUTABLE,
                        PROGRAM_VERSION, System.getProperty("sun.arch.data.model"),
                        System.getProperty("java.vendor"), System.getProperty("java.version"),
                        PROGRAM_DISCLAIMER));

            } else if (cmd.hasOption("h")) {
                // Show usage (done later) and terminate
                status = EX_USAGE;

            } else {
                // Run the service. Retrieve the configuration
                final String configLocation = cmd.getOptionValue('c', DEFAULT_CONFIG);

                // Differentiate between normal run, commons-daemon start, commons-daemon stop
                if (cmd.getArgList().contains("__start")) {
                    start(configLocation); // commons-daemon start
                } else if (cmd.getArgList().contains("__stop")) {
                    stop(); // commons-deamon stop
                } else {
                    run(configLocation); // normal execution
                }
            }

        } catch (final ParseException ex) {
            // Display error message and then usage on syntax error
            System.err.println("SYNTAX ERROR: " + ex.getMessage());
            status = EX_USAGE;

        } catch (final ServiceConfigurationError ex) {
            // Display error message and stack trace and terminate on configuration error
            System.err.println("INVALID CONFIGURATION: " + ex.getMessage());
            Throwables.getRootCause(ex).printStackTrace();
            status = EX_CONFIG;

        } catch (final Throwable ex) {
            // Display error message and stack trace on generic error
            System.err.print("EXECUTION FAILED: ");
            ex.printStackTrace();
            status = ex instanceof IOException ? EX_IOERR : EX_UNAVAILABLE;
        }

        // Display usage information if necessary
        if (status == EX_USAGE) {
            final PrintWriter out = new PrintWriter(System.out);
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printUsage(out, WIDTH, PROGRAM_EXECUTABLE, options);
            if (PROGRAM_DESCRIPTION != null) {
                formatter.printWrapped(out, WIDTH, "\n" + PROGRAM_DESCRIPTION.trim());
            }
            out.println("\nOptions");
            formatter.printOptions(out, WIDTH, options, 2, 2);
            out.flush();
        }

        // Display exit status for convenience
        if (status != EX_OK) {
            System.err.println("[exit status: " + status + "]");
        } else {
            System.out.println("[exit status: " + status + "]");
        }

        // Flush STDIN and STDOUT before exiting (we noted truncated outputs otherwise)
        System.out.flush();
        System.err.flush();

        // Force exiting (in case there are threads still running)
        System.exit(status);
    }

    private static void run(final String configLocation) throws Throwable {

        Preconditions.checkNotNull(configLocation);

        final AtomicReference<String> pendingSignal = new AtomicReference<String>(null);
        final Thread mainThread = Thread.currentThread();
        final Lock lock = new ReentrantLock();

        // Define shutdown handler
        final Thread shutdownHandler = new Thread("shutdown") {

            @Override
            public void run() {
                pendingSignal.set(SIGNAL_SHUTDOWN);
                mainThread.interrupt(); // delegate processing to main thread
                lock.lock();
                lock.unlock();
            }

        };

        // Define reload signal handler - if supported
        final AtomicReference<Object> oldHandlerHolder = new AtomicReference<Object>(null);
        Object reloadHandler = null;
        try {
            new Signal(SIGNAL_RELOAD); // fail if signal not supported
            reloadHandler = new SignalHandler() {

                @Override
                public void handle(final Signal signal) {
                    final String name = signal.getName();
                    try {
                        pendingSignal.compareAndSet(null, name);
                        mainThread.interrupt();

                    } finally {
                        final Object oldHandler = oldHandlerHolder.get();
                        if (oldHandler != null) {
                            ((SignalHandler) oldHandler).handle(signal);
                        }
                    }
                }

            };
        } catch (final Throwable ex) {
            // Cannot register signal handlers (on Windows, or sun.misc.Signal unavailable)
        }

        start(configLocation);
        lock.lock();

        try {
            // Register shutdown hook and signal handler, if supported
            java.lang.Runtime.getRuntime().addShutdownHook(shutdownHandler);
            if (reloadHandler != null) {
                try {
                    oldHandlerHolder.set(Signal.handle(new Signal(SIGNAL_RELOAD),
                            (SignalHandler) reloadHandler));
                } catch (final Throwable ex) {
                    reloadHandler = null;
                }
            }

            // Emit instructions to reload, stop, get info of service
            if (LOGGER.isInfoEnabled()) {
                final StringBuilder builder = new StringBuilder("Issue ");
                builder.append("q\\n/SIG").append(SIGNAL_SHUTDOWN).append(" to end, ");
                final String sig = reloadHandler == null ? "" : "/SIG" + SIGNAL_RELOAD;
                builder.append("r\\n").append(sig).append(" to reload, ");
                builder.append("i\\n").append(" to show info");
                LOGGER.info(builder.toString());
            }

            // Enter loop where signals and terminal input are checked and processed
            while (true) {
                try {
                    while (pendingSignal.get() == null && System.in.available() > 0) {
                        final char ch = (char) System.in.read();
                        if (ch == 'q' || ch == 'Q') {
                            pendingSignal.set(SIGNAL_SHUTDOWN);
                        } else if (ch == 'r' || ch == 'R') {
                            pendingSignal.set(SIGNAL_RELOAD);
                        } else if (ch == 'i' || ch == 'I') {
                            pendingSignal.set(SIGNAL_STATUS);
                        }
                    }
                } catch (final IOException ex) {
                    // Ignore
                }

                try {
                    if (pendingSignal.get() == null) {
                        Thread.sleep(1000);
                    }
                } catch (final InterruptedException ex) {
                    // Ignore
                }

                final String signal = pendingSignal.getAndSet(null);

                if (SIGNAL_SHUTDOWN.equals(signal)) {
                    break;
                } else if (SIGNAL_RELOAD.equals(signal)) {
                    stop();
                    start(configLocation);
                } else if (SIGNAL_STATUS.equals(signal)) {
                    LOGGER.info(status(true));
                }
            }

        } finally {
            try {
                // Stop the application
                stop();

            } finally {
                // Restore signal handlers and remove shutdown hook
                if (reloadHandler != null) {
                    final SignalHandler oldHandler = (SignalHandler) oldHandlerHolder.get();
                    Signal.handle(new Signal(SIGNAL_RELOAD), oldHandler);
                }

                try {
                    java.lang.Runtime.getRuntime().removeShutdownHook(shutdownHandler);
                } catch (final Throwable ex) {
                    // ignore, may be due to shutdown in progress
                }

                try {
                    // Stop logging, flushing data to log files (seems to be necessary)
                    ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
                } catch (final Throwable ex) {
                    // ignore
                }

                lock.unlock();
            }
        }
    }

    private static void start(final String configLocation) throws Throwable {

        Preconditions.checkNotNull(configLocation);

        // Abort if already running
        if (component != null) {
            return;
        }

        // Retrieve the configuration
        final List<Statement> config;
        final InputStream stream = retrieveURL(configLocation).openStream();
        final RDFFormat format = RDFFormat.forFileName(configLocation);
        config = RDFUtil.readRDF(stream, format, Data.getNamespaceMap(), null, false).toList();
        stream.close();

        // Extract launcher parameters
        String threadName = DEFAULT_THREAD_NAME;
        int threadCount = DEFAULT_THREAD_COUNT;
        String logConfig = DEFAULT_LOG_CONFIG;
        URI componentURI = null;
        for (final Statement statement : config) {
            final Resource s = statement.getSubject();
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();
            if (s.equals(LAUNCHER_URI)) {
                if (p.getLocalName().equals(PROPERTY_THREAD_NAME)) {
                    threadName = Data.convert(o, String.class);
                } else if (p.getLocalName().equals(PROPERTY_THREAD_COUNT)) {
                    threadCount = Data.convert(o, Integer.class);
                } else if (p.getLocalName().equals(PROPERTY_LOG_CONFIG)) {
                    logConfig = Data.convert(o, String.class);
                } else if (p.getLocalName().equals(PROPERTY_COMPONENT)) {
                    componentURI = (URI) o;
                }
            }
        }

        // Configure executor
        Data.setExecutor(Util.newScheduler(threadCount, threadName, true));

        // Configure logging
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        try {
            final JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();
            configurator.doConfigure(retrieveURL(logConfig));
        } catch (final JoranException je) {
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
        }
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        // Log relevant information
        String vendor = System.getProperty("java.vendor");
        final int index = vendor.indexOf(' ');
        vendor = index < 0 ? vendor : vendor.substring(0, index);
        final String header = String.format("%s %s / java %s (%s) %s / %s", PROGRAM_EXECUTABLE,
                PROGRAM_VERSION, System.getProperty("sun.arch.data.model"), vendor,
                System.getProperty("java.version"), System.getProperty("os.name")).toLowerCase();
        final String line = Strings.repeat("-", header.length());
        LOGGER.info(line);
        LOGGER.info(header);
        LOGGER.info(line);
        LOGGER.info("Using: {}", configLocation);
        LOGGER.info("Using: {}", logConfig);
        LOGGER.info("Using: {} threads", threadCount);

        // Instantiate the component
        final Component newComponent;
        try {
            newComponent = Factory.instantiate(config, componentURI, Component.class);
        } catch (final Throwable ex) {
            throw new ServiceConfigurationError("Configuration failed: " + ex.getMessage(), ex);
        }

        // Init/start the component
        newComponent.init();
        LOGGER.info("Service started");
        component = newComponent;
    }

    private static void stop() {

        LOGGER.info("Stopping service ...");

        if (component == null) {
            return;
        }

        try {
            component.close();
            LOGGER.info("Service stopped");
        } catch (final Throwable ex) {
            LOGGER.error("Close failed: " + ex.getMessage(), ex);
        }

        component = null;
    }

    private static String status(final boolean verbose) {

        final StringBuilder builder = new StringBuilder();

        // Emit application status
        builder.append(component != null ? "running" : "not running");

        // Emit uptime and percentage spent in GC
        final long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
        final long days = uptime / (24 * 60 * 60 * 1000);
        final long hours = uptime / (60 * 60 * 1000) - days * 24;
        final long minutes = uptime / (60 * 1000) - (days * 24 + hours) * 60;
        long gctime = 0;
        for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gctime += bean.getCollectionTime(); // assume 1 bean or they don't work in parallel
        }
        builder.append(", ").append(days == 0 ? "" : days + "d")
                .append(hours == 0 ? "" : hours + "h").append(minutes).append("m uptime (")
                .append(gctime * 100 / uptime).append("% gc)");

        // Emit memory usage
        final MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        final MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        final long used = heap.getUsed() + nonHeap.getUsed();
        final long committed = heap.getCommitted() + nonHeap.getCommitted();
        final long mb = 1024 * 1024;
        long max = 0;
        for (final MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
            max += bean.getPeakUsage().getUsed(); // assume maximum at same time in all pools
        }
        builder.append("; ").append(used / mb).append("/").append(committed / mb).append("/")
                .append(max / mb).append(" MB mem used/committed/max");

        // Emit thread numbers
        final int numThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        final int daemonThreads = ManagementFactory.getThreadMXBean().getDaemonThreadCount();
        final int maxThreads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        final long startedThreads = ManagementFactory.getThreadMXBean()
                .getTotalStartedThreadCount();
        builder.append("; ").append(daemonThreads).append("/").append(numThreads - daemonThreads)
                .append("/").append(maxThreads).append("/").append(startedThreads)
                .append(" threads daemon/non-daemon/max/started");

        // Look for deadlocked threads;
        final long[] deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads();

        // Emit verbose thread info
        if (verbose || deadlocked != null) {
            int maxState = 10; // "deadlocked".length()
            int maxName = 0;
            final Set<Thread> threads = Thread.getAllStackTraces().keySet();
            final ThreadInfo[] infos = ManagementFactory.getThreadMXBean().dumpAllThreads(false,
                    false);
            for (final ThreadInfo info : infos) {
                maxState = Math.max(maxState, info.getThreadState().toString().length());
                maxName = Math.max(maxName, info.getThreadName().length());
            }
            for (final ThreadInfo info : infos) {
                String state = info.getThreadState().toString().toLowerCase();
                if (deadlocked != null) {
                    for (final long id : deadlocked) {
                        if (info.getThreadId() == id) {
                            state = "deadlocked";
                        }
                    }
                }
                boolean daemon = false;
                boolean interrupted = false;
                for (final Thread thread : threads) {
                    if (thread.getName().equals(info.getThreadName())) {
                        daemon = thread.isDaemon();
                        interrupted = thread.isInterrupted();
                        break;
                    }
                }
                StackTraceElement element = null;
                final StackTraceElement[] trace = info.getStackTrace();
                if (trace != null && trace.length > 0) {
                    element = trace[0];
                    for (int i = 0; i < trace.length; ++i) {
                        if (trace[i].getClassName().startsWith("eu.fbk")) {
                            element = trace[i];
                            break;
                        }
                    }
                }
                builder.append(String.format("\n  %-11s  %-" + maxState + "s  %-" + maxName
                        + "s  ", (daemon ? "" : "non-") + "daemon" + (interrupted ? "*" : ""),
                        state, info.getThreadName()));
                builder.append(element == null ? "" : element.toString());
            }
        }

        // Emit collected info
        return builder.toString();
    }

    private static String retrieveVersion() {

        final String name = "META-INF/maven/eu.fbk.knowledgestore/ks-runtime/pom.properties";
        final URL url = Launcher.class.getClassLoader().getResource(name);
        if (url == null) {
            return "devel";
        }

        try {
            final InputStream stream = url.openStream();
            try {
                final Properties properties = new Properties();
                properties.load(stream);
                return properties.getProperty("version").trim();
            } finally {
                stream.close();
            }

        } catch (final IOException ex) {
            return "unknown version";
        }
    }

    private static URL retrieveURL(final String name) {
        try {
            URL url = Launcher.class.getClassLoader().getResource(name);
            if (url == null) {
                final File file = new File(name);
                if (file.exists() && !file.isDirectory()) {
                    url = file.toURI().toURL();
                }
            }
            return Preconditions.checkNotNull(url);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Invalid path: " + name, ex);
        }
    }

    private static String retrieveResource(final String name) {
        try {
            return Resources.toString(retrieveURL(name), Charsets.UTF_8);
        } catch (final IOException ex) {
            throw new Error("Cannot load " + name + ": " + ex.getMessage(), ex);
        }
    }

    @Nullable
    private static <T> T retrieveProperty(final String property, final Class<T> type,
            final T defaultValue) {
        final String value = System.getProperty(property);
        if (value != null) {
            try {
                return Data.convert(value, type);
            } catch (final Throwable ex) {
                LOGGER.warn("Could not retrieve property '" + property + "'", ex);
            }
        }
        return defaultValue;
    }

    private Launcher() {
    }

}
