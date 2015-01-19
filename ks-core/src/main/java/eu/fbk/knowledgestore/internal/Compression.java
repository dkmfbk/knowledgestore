package eu.fbk.knowledgestore.internal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// WARNING: on windows, if Java app is killed without shutdown hooks running, external processes
// launched from this class may not be terminated.

public final class Compression {

    private static final Logger LOGGER = LoggerFactory.getLogger(Compression.class);

    private static final String FACTORY_CLASS = "org.apache.commons.compress.compressors."
            + "CompressorStreamFactory";

    private static final String READ_METHOD = "createCompressorInputStream";

    private static final String WRITE_METHOD = "createCompressorOutputStream";

    private static final Map<String, String> CMD_MAP = Maps.newHashMap();

    public static final Compression NONE = new Compression("NONE", ImmutableList.<String>of(),
            ImmutableList.<String>of(), null, null, null, null);

    public static final Compression GZIP = new Compression("GZIP", ImmutableList.of(
            "application/gzip", "application/x-gzip"), ImmutableList.of("gz"), "%s -dc %s",
            "%s -dc", "sh -c '%s -9c > %s'", "%s -9c");

    public static final Compression BZIP2 = new Compression("BZIP2", ImmutableList.of(
            "application/bzip2", "application/x-bzip2"), ImmutableList.of("bz2"), "%s -dkc %s",
            "%s -dc", "sh -c '%s -9c > %s'", "%s -9c");

    public static final Compression XZ = new Compression("XZ",
            ImmutableList.of("application/x-xz"), ImmutableList.of("xz"), "%s -dkc %s", "%s -dc",
            "sh -c '%s -9c > %s'", "%s -9c");

    private static Set<Compression> register = ImmutableSet.of(NONE, GZIP, BZIP2, XZ);

    private final String name;

    private final List<String> mimeTypes;

    private final List<String> fileExtensions;

    @Nullable
    private final String readFileCmd;

    @Nullable
    private final String readPipeCmd;

    @Nullable
    private final String writeFileCmd;

    @Nullable
    private final String writePipeCmd;

    public Compression(final String name, final Iterable<? extends String> mimeTypes,
            final Iterable<? extends String> fileExtensions, @Nullable final String readFileCmd,
            @Nullable final String readPipeCmd, @Nullable final String writeFileCmd,
            @Nullable final String writePipeCmd) {
        this.name = Preconditions.checkNotNull(name);
        this.mimeTypes = ImmutableList.copyOf(mimeTypes);
        this.fileExtensions = ImmutableList.copyOf(fileExtensions);
        this.readFileCmd = readFileCmd;
        this.readPipeCmd = readPipeCmd;
        this.writeFileCmd = writeFileCmd;
        this.writePipeCmd = writePipeCmd;
    }

    /**
     * Returns the name of this compression format.
     * 
     * @return a human readable name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns all the MIME types associated to this compression format, ranked in preference
     * order.
     * 
     * @return an immutable list of MIME formats, not empty
     */
    public List<String> getMIMETypes() {
        return this.mimeTypes;
    }

    /**
     * Returns all the file extensions associated to this compression format, ranked in preference
     * order.
     * 
     * @return an immutable list of file extension, not empty
     */
    public List<String> getFileExtensions() {
        return this.fileExtensions;
    }

    public InputStream read(@Nullable final Executor executor, final File file) throws IOException {

        Preconditions.checkNotNull(file);
        if (!file.exists()) {
            throw new IllegalArgumentException("File " + file + " does not exist");
        }

        if (this == NONE) {
            return new BufferedInputStream(new FileInputStream(file));
        }

        if (this.readFileCmd != null && executor != null) {
            final String command = String.format(this.readFileCmd,
                    quote(lookupProgram(this.name)), quote(file.getAbsolutePath()));
            try {
                final Process process = Runtime.getRuntime().exec(tokenize(command));
                logStream(executor, this.name, process.getErrorStream(), true);
                return wrap(process.getInputStream(), process);
            } catch (final Throwable ex) {
                invalidateProgram(this.name);
                LOGGER.debug("Failed to run: ", command);
            }
        }

        InputStream stream = null;
        try {
            final Class<?> clazz = Class.forName(FACTORY_CLASS);
            final Method method = clazz.getMethod(READ_METHOD, String.class, InputStream.class);
            final Object factory = clazz.newInstance();
            stream = new FileInputStream(file);
            return (InputStream) method.invoke(factory, this.name, stream);
        } catch (final Throwable ex) {
            Util.closeQuietly(stream);
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
        }

        throw new IllegalArgumentException("Cannot decompress " + this + " file " + file);
    }

    public InputStream read(@Nullable final Executor executor, final InputStream stream)
            throws IOException {

        Preconditions.checkNotNull(stream);

        if (this == NONE) {
            return stream;
        }

        if (this.readPipeCmd != null && executor != null) {
            final String command = String
                    .format(this.readPipeCmd, quote(lookupProgram(this.name)));
            try {
                final Process process = Runtime.getRuntime().exec(tokenize(command));
                copyStream(executor, stream, process.getOutputStream());
                logStream(executor, this.name, process.getErrorStream(), true);
                return wrap(process.getInputStream(), process);
            } catch (final Throwable ex) {
                invalidateProgram(this.name);
                LOGGER.debug("Failed to run: ", command);
            }
        }

        try {
            final Class<?> clazz = Class.forName(FACTORY_CLASS);
            final Method method = clazz.getMethod(READ_METHOD, String.class, InputStream.class);
            final Object factory = clazz.newInstance();
            return (InputStream) method.invoke(factory, this.name, stream);
        } catch (final Throwable ex) {
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
        }

        throw new IllegalArgumentException("Cannot decompress " + this + " stream");
    }

    public OutputStream write(@Nullable final Executor executor, final File file)
            throws IOException {

        Preconditions.checkNotNull(file);

        if (this == NONE) {
            return new BufferedOutputStream(new FileOutputStream(file));
        }

        if (this.writeFileCmd != null && executor != null) {
            final String command = String.format(this.writeFileCmd,
                    quote(lookupProgram(this.name)), quote(file.getAbsolutePath()));
            try {
                final Process process = Runtime.getRuntime().exec(tokenize(command));
                logStream(executor, this.name, process.getInputStream(), false);
                logStream(executor, this.name, process.getErrorStream(), true);
                return wrap(process.getOutputStream(), process);
            } catch (final Throwable ex) {
                invalidateProgram(this.name);
                LOGGER.debug("Failed to run: ", command);
            }
        }

        OutputStream stream = null;
        try {
            final Class<?> clazz = Class.forName(FACTORY_CLASS);
            final Method method = clazz.getMethod(WRITE_METHOD, String.class, OutputStream.class);
            final Object factory = clazz.newInstance();
            stream = new FileOutputStream(file);
            return (OutputStream) method.invoke(factory, this.name, stream);
        } catch (final Throwable ex) {
            Util.closeQuietly(stream);
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
        }

        throw new IllegalArgumentException("Cannot compress " + this + " file " + file);
    }

    public OutputStream write(@Nullable final Executor executor, final OutputStream stream)
            throws IOException {

        Preconditions.checkNotNull(stream);

        if (this == NONE) {
            return stream;
        }

        if (this.writePipeCmd != null && executor != null) {
            final String command = String.format(this.writePipeCmd,
                    quote(lookupProgram(this.name)));
            try {
                final Process process = Runtime.getRuntime().exec(tokenize(command));
                copyStream(executor, process.getInputStream(), stream);
                logStream(executor, this.name, process.getErrorStream(), true);
                return wrap(process.getOutputStream(), process);
            } catch (final Throwable ex) {
                invalidateProgram(this.name);
                LOGGER.debug("Failed to run: ", command);
            }
        }

        try {
            final Class<?> clazz = Class.forName(FACTORY_CLASS);
            final Method method = clazz.getMethod(WRITE_METHOD, String.class, OutputStream.class);
            final Object factory = clazz.newInstance();
            return (OutputStream) method.invoke(factory, this.name, stream);
        } catch (final Throwable ex) {
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
        }

        throw new IllegalArgumentException("Cannot compress " + this + " stream");
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Compression)) {
            return false;
        }
        final Compression other = (Compression) object;
        return this.name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder(64);
        builder.append(this.name);
        builder.append(" (mimeTypes=");
        Joiner.on(", ").appendTo(builder, this.mimeTypes);
        builder.append("; ext=");
        Joiner.on(", ").appendTo(builder, this.fileExtensions);
        builder.append(")");
        return builder.toString();
    }

    /**
     * Returns the registered {@code Compression} format matching the extension in the file name
     * supplied, or the {@code fallback} specified if none matches.
     * 
     * @param fileName
     *            the file name, not null
     * @param fallback
     *            the {@code Compression} to return if none matches
     * @return the matching {@code Compression}, or the {@code fallback} one if none of the
     *         registered {@code Compression}s matches
     */
    @Nullable
    public static Compression forFileName(final String fileName,
            @Nullable final Compression fallback) {
        final int index = fileName.lastIndexOf('.');
        final String extension = (index < 0 ? fileName : fileName.substring(index + 1))
                .toLowerCase();
        for (final Compression compression : register) {
            final List<String> extensions = compression.fileExtensions;
            if (!extensions.isEmpty() && extensions.get(0).equals(extension)) {
                return compression;
            }
        }
        for (final Compression compression : register) {
            if (compression.fileExtensions.contains(extension)) {
                return compression;
            }
        }
        return fallback;
    }

    /**
     * Returns the registered {@code Compression} format matching the MIME type specified, or the
     * {@code fallback} specified if none matches.
     * 
     * @param mimeType
     *            the mime type, not null
     * @param fallback
     *            the {@code Compression} to return if none matches
     * @return the matching {@code Compression}, or the {@code fallback} one if none of the
     *         registered {@code Compression}s matches
     */
    @Nullable
    public static Compression forMIMEType(final String mimeType,
            @Nullable final Compression fallback) {
        final String actualMimeType = mimeType.toLowerCase();
        for (final Compression compression : register) {
            final List<String> mimeTypes = compression.mimeTypes;
            if (!mimeTypes.isEmpty() && mimeTypes.get(0).equals(actualMimeType)) {
                return compression;
            }
        }
        for (final Compression compression : register) {
            if (compression.mimeTypes.contains(actualMimeType)) {
                return compression;
            }
        }
        return fallback;
    }

    /**
     * Returns the {@code Compression} with the name specified. Matching is case-insensitive.
     * 
     * @param name
     *            the {@code Compression} name.
     * @return the {@code Compression} for the name specified, or null if none matches
     */
    @Nullable
    public static Compression valueOf(final String name) {
        final String actualName = name.trim().toUpperCase();
        for (final Compression compression : register) {
            if (compression.name.equals(actualName)) {
                return compression;
            }
        }
        Preconditions.checkNotNull(name);
        return null;
    }

    public static Set<Compression> values() {
        return register;
    }

    public synchronized static void register(final Compression compression) {
        Preconditions.checkNotNull(compression);
        final List<Compression> newRegister = Lists.newArrayList(register);
        newRegister.add(compression);
        register = ImmutableSet.copyOf(newRegister);
    }

    private static String lookupProgram(final String name) {
        synchronized (CMD_MAP) {
            String cmd = CMD_MAP.get(name);
            if (cmd == null) {
                cmd = System.getenv(name.toUpperCase() + "_CMD");
                if (cmd != null) {
                    LOGGER.info("Using '{}' for '{}'", cmd, name);
                } else {
                    cmd = name;
                }
                CMD_MAP.put(name, cmd);
            }
            return cmd;
        }
    }

    private static void invalidateProgram(final String name) {
        synchronized (CMD_MAP) {
            CMD_MAP.put(name, null);
        }
    }

    private static String[] tokenize(final String command) {
        final List<String> tokens = Lists.newArrayList();
        final int length = command.length();
        boolean escape = false;
        char quote = 0;
        int start = -1;
        for (int i = 0; i < length; ++i) {
            final char ch = command.charAt(i);
            if (escape) {
                escape = false;
            } else if (ch == '\\') {
                escape = true;
            } else if (quote != 0) {
                if (ch == quote) {
                    tokens.add(command.substring(start, i));
                    start = -1;
                    quote = 0;
                }
            } else if (start == -1) {
                if (ch == '\'' || ch == '"') {
                    start = i + 1;
                    quote = ch;
                } else if (!Character.isWhitespace(ch)) {
                    start = i;
                }
            } else if (Character.isWhitespace(ch)) {
                tokens.add(command.substring(start, i));
                start = -1;
            }
        }
        if (quote == 0 && start >= 0) {
            tokens.add(command.substring(start));
        }
        return tokens.toArray(new String[tokens.size()]);
    }

    private static String quote(final String string) {
        return '"' + string + '"';
    }

    private static void copyStream(final Executor executor, final InputStream in,
            final OutputStream out) {
        executor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    ByteStreams.copy(in, out);
                } catch (final IOException ex) {
                    LOGGER.error("Stream copy failed", ex);
                }
            }

        });
    }

    private static void logStream(final Executor executor, final String name,
            final InputStream stream, final boolean error) {
        executor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    final BufferedReader in = new BufferedReader(new InputStreamReader(stream));
                    String line;
                    while ((line = in.readLine()) != null) {
                        final String message = "[" + name + "] " + line;
                        if (error) {
                            LOGGER.error(message);
                        } else {
                            LOGGER.debug(message);
                        }
                    }
                } catch (final Throwable ex) {
                    // ignore
                }
            }

        });
    }

    private static InputStream wrap(final InputStream stream, final Process process) {
        final Thread destroyHook = new DestroyHook(process);
        Runtime.getRuntime().addShutdownHook(destroyHook);
        return new BufferedInputStream(stream) {

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    process.destroy();
                    Runtime.getRuntime().removeShutdownHook(destroyHook);
                }
            }

        };
    }

    private static OutputStream wrap(final OutputStream stream, final Process process) {
        final Thread destroyHook = new DestroyHook(process);
        Runtime.getRuntime().addShutdownHook(destroyHook);
        return new BufferedOutputStream(stream) {

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    process.destroy();
                    Runtime.getRuntime().removeShutdownHook(destroyHook);
                }
            }

        };
    }

    private static class DestroyHook extends Thread {

        private final Process process;

        DestroyHook(final Process process) {
            this.process = Preconditions.checkNotNull(process);
        }

        @Override
        public void run() {
            this.process.destroy();
        }

    }

}
