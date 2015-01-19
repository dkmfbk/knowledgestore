package eu.fbk.knowledgestore.internal;

import java.io.Closeable;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

    public static URL getURL(final String location) {
        URL url = null;
        try {
            url = Resources.getResource(location.startsWith("/") ? location.substring(1)
                    : location);
            if (url != null) {
                return url;
            }
        } catch (final Exception ex) {
            // not a classpath resource - ignore
        }
        try {
            final File file = new File(location);
            if (file.exists() && file.isFile()) {
                return file.toURI().toURL();
            }
        } catch (final Exception ex) {
            // not a file - ignore
        }
        try {
            return new URL(location);
        } catch (final Exception ex) {
            // not a valid URL
            throw new IllegalArgumentException("Cannot extract a URL from: " + location);
        }
    }

    public static String getResource(final Class<?> referenceClass, final String resourceName) {
        try {
            final URL url = referenceClass.getResource(resourceName);
            return Resources.toString(url, Charsets.UTF_8);
        } catch (final IOException ex) {
            throw new Error("Missing resource '" + resourceName + "': " + ex.getMessage(), ex);
        }
    }

    public static String getVersion(final String groupId, final String artifactId,
            final String defaultValue) {
        final URL url = Util.class.getClassLoader().getResource(
                "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties");
        String version = defaultValue;
        if (url != null) {
            try {
                final InputStream stream = url.openStream();
                try {
                    final Properties properties = new Properties();
                    properties.load(stream);
                    version = properties.getProperty("version").trim();
                } finally {
                    stream.close();
                }
            } catch (final IOException ex) {
                version = "unknown";
            }
        }
        return version;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static String formatType(final Type type) {
        final TypeToken<?> token = TypeToken.of(type);
        final Class<?> clazz = token.getRawType();
        String name = clazz.getSimpleName();
        if (name.isEmpty()) {
            Class<?> parent = clazz.getSuperclass();
            if (parent == null && clazz.getInterfaces().length > 0) {
                parent = clazz.getInterfaces()[0];
            }
            if (parent != null) {
                name = token.getSupertype((Class) parent).toString();
            }
        }
        return name;
    }

    @Nullable
    public static <T> T closeQuietly(@Nullable final T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (final Throwable ex) {
                LOGGER.error("Error closing " + object.getClass().getSimpleName(), ex);
            }
        }
        return object;
    }

    @Nullable
    public static InputStream interceptClose(@Nullable final InputStream stream,
            @Nullable final Runnable runnable) {
        if (stream == null || runnable == null) {
            return stream;
        }
        final Map<String, String> mdc = Logging.getMDC();
        return new FilterInputStream(stream) {

            private boolean closed;

            @Override
            public void close() throws IOException {
                if (this.closed) {
                    return;
                }
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    super.close();
                    runnable.run();
                } finally {
                    this.closed = true;
                    Logging.setMDC(oldMdc);
                }
            }

        };
    }

    @Nullable
    public static OutputStream interceptClose(@Nullable final OutputStream stream,
            @Nullable final Runnable runnable) {
        if (stream == null || runnable == null) {
            return stream;
        }
        final Map<String, String> mdc = Logging.getMDC();
        return new FilterOutputStream(stream) {

            private boolean closed;

            @Override
            public void close() throws IOException {
                if (this.closed) {
                    return;
                }
                final Map<String, String> oldMdc = Logging.getMDC();
                try {
                    Logging.setMDC(mdc);
                    super.close();
                    runnable.run();
                } finally {
                    this.closed = true;
                    Logging.setMDC(oldMdc);
                }
            }

        };
    }

    public static ListeningScheduledExecutorService newScheduler(final int numThreads,
            final String nameFormat, final boolean daemon) {
        final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(daemon)
                .setNameFormat(nameFormat)
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

                    @Override
                    public void uncaughtException(final Thread thread, final Throwable ex) {
                        LOGGER.error("Uncaught exception in thread " + thread.getName(), ex);
                    }

                }).build();
        return decorate(Executors.newScheduledThreadPool(numThreads, factory));
    }

    public static ListeningExecutorService decorate(final ExecutorService executor) {
        Preconditions.checkNotNull(executor);
        if (executor instanceof MDCExecutorService) {
            return (MDCExecutorService) executor;
        } else if (executor instanceof ListeningExecutorService) {
            return new MDCExecutorService((ListeningExecutorService) executor);
        } else {
            return new MDCExecutorService(MoreExecutors.listeningDecorator(executor));
        }
    }

    public static ListeningScheduledExecutorService decorate(
            final ScheduledExecutorService executor) {
        if (executor instanceof MDCScheduledExecutorService) {
            return (MDCScheduledExecutorService) executor;
        } else if (executor instanceof ListeningScheduledExecutorService) {
            return new MDCScheduledExecutorService((ListeningScheduledExecutorService) executor);
        } else {
            // return MoreExecutors.listeningDecorator(executor);
            return new MDCScheduledExecutorService(MoreExecutors.listeningDecorator(executor));
        }
    }

    private static class MDCScheduledExecutorService extends MDCExecutorService implements
            ListeningScheduledExecutorService {

        MDCScheduledExecutorService(final ListeningScheduledExecutorService delegate) {
            super(Preconditions.checkNotNull(delegate));
        }

        @Override
        ListeningScheduledExecutorService delegate() {
            return (ListeningScheduledExecutorService) super.delegate();
        }

        @Override
        public ListenableScheduledFuture<?> schedule(final Runnable command, final long delay,
                final TimeUnit unit) {
            return delegate().schedule(wrap(command, MDC.getCopyOfContextMap()), delay, unit);
        }

        @Override
        public <V> ListenableScheduledFuture<V> schedule(final Callable<V> callable,
                final long delay, final TimeUnit unit) {
            return delegate().schedule(wrap(callable, MDC.getCopyOfContextMap()), delay, unit);
        }

        @Override
        public ListenableScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
                final long initialDelay, final long period, final TimeUnit unit) {
            return delegate().scheduleAtFixedRate(wrap(command, MDC.getCopyOfContextMap()),
                    initialDelay, period, unit);
        }

        @Override
        public ListenableScheduledFuture<?> scheduleWithFixedDelay(final Runnable command,
                final long initialDelay, final long delay, final TimeUnit unit) {
            return delegate().scheduleWithFixedDelay(wrap(command, MDC.getCopyOfContextMap()),
                    initialDelay, delay, unit);
        }

    }

    private static class MDCExecutorService implements ListeningExecutorService {

        private final ListeningExecutorService delegate;

        MDCExecutorService(final ListeningExecutorService delegate) {
            this.delegate = Preconditions.checkNotNull(delegate);
        }

        ListeningExecutorService delegate() {
            return this.delegate;
        }

        Runnable wrap(final Runnable runnable, final Map<String, String> mdcMap) {
            return new Runnable() {

                @Override
                public void run() {
                    Map<String, String> oldMap = null;
                    try {
                        if (mdcMap != null) {
                            oldMap = MDC.getCopyOfContextMap();
                            MDC.setContextMap(mdcMap);
                        }
                        runnable.run();
                    } catch (final Throwable ex) {
                        LOGGER.error("Uncaught exception in thread "
                                + Thread.currentThread().getName() + ": " + ex.getMessage(), ex);
                        throw Throwables.propagate(ex);
                    } finally {
                        if (oldMap != null) {
                            MDC.setContextMap(oldMap);
                        }
                    }
                }
            };
        }

        <T> Callable<T> wrap(final Callable<T> callable, final Map<String, String> mdcMap) {
            return new Callable<T>() {

                @Override
                public T call() throws Exception {
                    Map<String, String> oldMap = null;
                    try {
                        if (mdcMap != null) {
                            oldMap = MDC.getCopyOfContextMap();
                            MDC.setContextMap(mdcMap);
                        }
                        return callable.call();
                    } catch (final Throwable ex) {
                        LOGGER.error("Uncaught exception in thread "
                                + Thread.currentThread().getName() + ": " + ex.getMessage(), ex);
                        Throwables.propagateIfPossible(ex, Exception.class);
                        throw new RuntimeException(ex);
                    } finally {
                        if (oldMap != null) {
                            MDC.setContextMap(oldMap);
                        }
                    }
                }
            };
        }

        <T> Collection<Callable<T>> wrap(final Collection<? extends Callable<T>> callables,
                final Map<String, String> mdcMap) {
            final List<Callable<T>> result = Lists.newArrayListWithCapacity(callables.size());
            for (final Callable<T> callable : callables) {
                result.add(wrap(callable, mdcMap));
            }
            return result;
        }

        @Override
        public void shutdown() {
            delegate().shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate().shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate().isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate().isTerminated();
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit)
                throws InterruptedException {
            return delegate().awaitTermination(timeout, unit);
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException {
            return delegate().invokeAny(wrap(tasks, MDC.getCopyOfContextMap()));
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout,
                final TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return delegate().invokeAny(wrap(tasks, MDC.getCopyOfContextMap()), timeout, unit);
        }

        @Override
        public void execute(final Runnable command) {
            delegate().execute(wrap(command, MDC.getCopyOfContextMap()));
        }

        @Override
        public <T> ListenableFuture<T> submit(final Callable<T> task) {
            return delegate().submit(wrap(task, MDC.getCopyOfContextMap()));
        }

        @Override
        public ListenableFuture<?> submit(final Runnable task) {
            return delegate().submit(wrap(task, MDC.getCopyOfContextMap()));
        }

        @Override
        public <T> ListenableFuture<T> submit(final Runnable task, final T result) {
            return delegate().submit(wrap(task, MDC.getCopyOfContextMap()), result);
        }

        @Override
        public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            return delegate().invokeAll(wrap(tasks, MDC.getCopyOfContextMap()));
        }

        @Override
        public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks,
                final long timeout, final TimeUnit unit) throws InterruptedException {
            return delegate().invokeAll(wrap(tasks, MDC.getCopyOfContextMap()), timeout, unit);
        }

    }

}
