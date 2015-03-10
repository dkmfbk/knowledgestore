package eu.fbk.knowledgestore.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

import ch.qos.logback.classic.Level;

import eu.fbk.knowledgestore.data.Data;

public final class CommandLine {

    private final List<String> args;

    private final List<String> options;

    private final Map<String, List<String>> optionValues;

    private CommandLine(final List<String> args, final Map<String, List<String>> optionValues) {

        final List<String> options = Lists.newArrayList();
        for (final String letterOrName : optionValues.keySet()) {
            if (letterOrName.length() > 1) {
                options.add(letterOrName);
            }
        }

        this.args = args;
        this.options = Ordering.natural().immutableSortedCopy(options);
        this.optionValues = optionValues;
    }

    public <T> List<T> getArgs(final Class<T> type) {
        return convert(this.args, type);
    }

    public <T> T getArg(final int index, final Class<T> type) {
        return convert(this.args.get(index), type);
    }

    public <T> T getArg(final int index, final Class<T> type, final T defaultValue) {
        try {
            return convert(this.args.get(index), type);
        } catch (final Throwable ex) {
            return defaultValue;
        }
    }

    public int getArgCount() {
        return this.args.size();
    }

    public List<String> getOptions() {
        return this.options;
    }

    public boolean hasOption(final String letterOrName) {
        return this.optionValues.containsKey(letterOrName);
    }

    public <T> List<T> getOptionValues(final String letterOrName, final Class<T> type) {
        final List<String> strings = Objects.firstNonNull(this.optionValues.get(letterOrName),
                ImmutableList.<String>of());
        return convert(strings, type);
    }

    @Nullable
    public <T> T getOptionValue(final String letterOrName, final Class<T> type) {
        final List<String> strings = this.optionValues.get(letterOrName);
        if (strings == null || strings.isEmpty()) {
            return null;
        }
        if (strings.size() > 1) {
            throw new Exception("Multiple values for option '" + letterOrName + "': "
                    + Joiner.on(", ").join(strings), null);
        }
        try {
            return convert(strings.get(0), type);
        } catch (final Throwable ex) {
            throw new Exception("'" + strings.get(0) + "' is not a valid " + type.getSimpleName(),
                    ex);
        }
    }

    @Nullable
    public <T> T getOptionValue(final String letterOrName, final Class<T> type,
            @Nullable final T defaultValue) {
        final List<String> strings = this.optionValues.get(letterOrName);
        if (strings == null || strings.isEmpty() || strings.size() > 1) {
            return defaultValue;
        }
        try {
            return convert(strings.get(0), type);
        } catch (final Throwable ex) {
            return defaultValue;
        }
    }

    public int getOptionCount() {
        return this.options.size();
    }

    private static <T> T convert(final String string, final Class<T> type) {
        try {
            return Data.convert(string, type);
        } catch (final Throwable ex) {
            throw new Exception("'" + string + "' is not a valid " + type.getSimpleName(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> convert(final List<String> strings, final Class<T> type) {
        if (type == String.class) {
            return (List<T>) strings;
        }
        final List<T> list = Lists.newArrayList();
        for (final String string : strings) {
            list.add(convert(string, type));
        }
        return ImmutableList.copyOf(list);
    }

    public static void fail(final Throwable throwable) {
        if (throwable instanceof Exception) {
            if (throwable.getMessage() == null) {
                System.exit(0);
            } else {
                System.err.println("SYNTAX ERROR: " + throwable.getMessage());
            }
            System.exit(-2);
        } else {
            System.err.println("EXECUTION FAILED: " + throwable.getMessage());
            throwable.printStackTrace();
            System.exit(-1);
        }
    }

    public static Parser parser() {
        return new Parser();
    }

    public static final class Parser {

        @Nullable
        private String name;

        @Nullable
        private String header;

        @Nullable
        private String footer;

        @Nullable
        private Logger logger;

        private final Options options;

        private final Set<String> mandatoryOptions;

        public Parser() {
            this.name = null;
            this.header = null;
            this.footer = null;
            this.options = new Options();
            this.mandatoryOptions = new HashSet<>();
        }

        public Parser withName(@Nullable final String name) {
            this.name = name;
            return this;
        }

        public Parser withHeader(@Nullable final String header) {
            this.header = header;
            return this;
        }

        public Parser withFooter(@Nullable final String footer) {
            this.footer = footer;
            return this;
        }

        public Parser withLogger(@Nullable final Logger logger) {
            this.logger = logger;
            return this;
        }

        public Parser withOption(@Nullable final String letter, final String name,
                final String description) {

            Preconditions.checkNotNull(name);
            Preconditions.checkArgument(name.length() > 1);
            Preconditions.checkNotNull(description);

            final Option option = new Option(letter, name, false, description);
            this.options.addOption(option);

            return this;
        }

        public Parser withOption(@Nullable final String letter, final String name,
                final String description, final String argName, @Nullable final Type argType,
                final boolean argRequired, final boolean multiValue, final boolean mandatory) {

            Preconditions.checkNotNull(name);
            Preconditions.checkArgument(name.length() > 1);
            Preconditions.checkNotNull(description);
            Preconditions.checkNotNull(argName);
            Preconditions.checkNotNull(argType);

            final Option option = new Option(letter, name, true, description);
            option.setArgName(argName);
            option.setOptionalArg(!argRequired);
            option.setArgs(multiValue ? Short.MAX_VALUE : 1);
            option.setType(argType);
            this.options.addOption(option);

            if (mandatory) {
                this.mandatoryOptions.add(name);
            }

            return this;
        }

        @SuppressWarnings("unchecked")
        public CommandLine parse(final String... args) {

            try {
                // Add additional options
                if (this.logger != null) {
                    this.options.addOption("V", "verbose", false, "enable verbose output");
                }
                this.options.addOption("v", "version", false,
                        "display version information and terminate");
                this.options.addOption("h", "help", false,
                        "display this help message and terminate");

                // Parse options
                org.apache.commons.cli.CommandLine cmd = null;
                try {
                    cmd = new GnuParser().parse(this.options, args);
                } catch (final Throwable ex) {
                    System.err.println("SYNTAX ERROR: " + ex.getMessage());
                    printHelp();
                    throw new Exception(null);
                }

                // Handle verbose mode
                if (cmd.hasOption('V')) {
                    try {
                        ((ch.qos.logback.classic.Logger) this.logger).setLevel(Level.DEBUG);
                    } catch (final Throwable ex) {
                        // ignore
                    }
                }

                // Handle version and help commands. Throw an exception to halt execution
                if (cmd.hasOption('v')) {
                    printVersion();
                    throw new Exception(null);

                } else if (cmd.hasOption('h')) {
                    printHelp();
                    throw new Exception(null);
                }

                // Check that mandatory options have been specified
                for (String name : mandatoryOptions) {
                    if (!cmd.hasOption(name)) {
                        System.err.println("SYNTAX ERROR: missing mandatory option " + name);
                        printHelp();
                        throw new Exception(null);
                    }
                }

                // Extract options and their arguments
                final Map<String, List<String>> optionValues = Maps.newHashMap();
                for (final Option option : cmd.getOptions()) {
                    final List<String> valueList = Lists.newArrayList();
                    final String[] values = cmd.getOptionValues(option.getLongOpt());
                    if (values != null) {
                        for (final String value : values) {
                            if (option.getType() instanceof Type) {
                                Type.validate(value, (Type) option.getType());
                            }
                            valueList.add(value);
                        }
                    }
                    final List<String> valueSet = ImmutableList.copyOf(valueList);
                    optionValues.put(option.getLongOpt(), valueSet);
                    if (option.getOpt() != null) {
                        optionValues.put(option.getOpt(), valueSet);
                    }
                }

                // Create and return the resulting CommandLine object
                return new CommandLine(ImmutableList.copyOf(cmd.getArgList()), optionValues);

            } catch (final Throwable ex) {
                throw new Exception(ex.getMessage(), ex);
            }
        }

        private void printVersion() {
            String version = "(development)";
            final URL url = CommandLine.class.getClassLoader().getResource(
                    "META-INF/maven/eu.fbk.nafview/nafview/pom.properties");
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
                    version = "(unknown)";
                }
            }
            final String name = Objects.firstNonNull(this.name, "Version");
            System.out.println(String.format("%s %s\nJava %s bit (%s) %s\n", name, version,
                    System.getProperty("sun.arch.data.model"), System.getProperty("java.vendor"),
                    System.getProperty("java.version")));
        }

        private void printHelp() {
            final HelpFormatter formatter = new HelpFormatter();
            final PrintWriter out = new PrintWriter(System.out);
            final String name = Objects.firstNonNull(this.name, "java");
            formatter.printUsage(out, 80, name, this.options);
            if (this.header != null) {
                out.println();
                formatter.printWrapped(out, 80, this.header);
            }
            out.println();
            formatter.printOptions(out, 80, this.options, 2, 2);
            if (this.footer != null) {
                out.println();
                out.println(this.footer);
                // formatter.printWrapped(out, 80, this.footer);
            }
            out.flush();
        }

    }

    public static final class Exception extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public Exception(final String message) {
            super(message);
        }

        public Exception(final String message, final Throwable cause) {
            super(message, cause);
        }

    }

    public enum Type {

        STRING,

        INTEGER,

        POSITIVE_INTEGER,

        NON_NEGATIVE_INTEGER,

        FLOAT,

        POSITIVE_FLOAT,

        NON_NEGATIVE_FLOAT,

        FILE,

        FILE_EXISTING,

        DIRECTORY,

        DIRECTORY_EXISTING;

        public boolean validate(final String string) {
            // Polymorphism not used for performance reasons
            return validate(string, this);
        }

        private static boolean validate(final String string, final Type type) {

            if (type == Type.INTEGER || type == Type.POSITIVE_INTEGER
                    || type == Type.NON_NEGATIVE_INTEGER) {
                try {
                    final long n = Long.parseLong(string);
                    if (type == Type.POSITIVE_INTEGER) {
                        return n > 0L;
                    } else if (type == Type.NON_NEGATIVE_INTEGER) {
                        return n >= 0L;
                    }
                } catch (final Throwable ex) {
                    return false;
                }

            } else if (type == Type.FLOAT || type == Type.POSITIVE_FLOAT
                    || type == Type.NON_NEGATIVE_FLOAT) {
                try {
                    final double n = Double.parseDouble(string);
                    if (type == Type.POSITIVE_FLOAT) {
                        return n > 0.0;
                    } else if (type == Type.NON_NEGATIVE_FLOAT) {
                        return n >= 0.0;
                    }
                } catch (final Throwable ex) {
                    return false;
                }

            } else if (type == FILE) {
                final File file = new File(string);
                return !file.exists() || file.isFile();

            } else if (type == FILE_EXISTING) {
                final File file = new File(string);
                return file.exists() && file.isFile();

            } else if (type == DIRECTORY) {
                final File dir = new File(string);
                return !dir.exists() || dir.isDirectory();

            } else if (type == DIRECTORY_EXISTING) {
                final File dir = new File(string);
                return dir.exists() && dir.isDirectory();
            }

            return true;
        }

    }

}
