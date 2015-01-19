package eu.fbk.knowledgestore.data;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import eu.fbk.knowledgestore.internal.Util;
import eu.fbk.knowledgestore.internal.rdf.CompactValueFactory;

// TODO: RDF conversion
// TODO: bytes
// TODO: getterOf, getterOfUnique, converterTo

/**
 * Helper services for working with KnowledgeStore data.
 * <p>
 * This class provides a number of services for working with KnowledgeStore data, i.e., with
 * {@link Representation}, {@link Value}, {@link Statement}, {@link Record} instances and
 * instances of scalar types that can be converted to {@code Literal} values. The following
 * services are offered:
 * </p>
 * <ul>
 * <li><b>MIME type registry</b>. Method {@link #extensionToMimeType(String)} and
 * {@link #mimeTypeToExtensions(String)} allow to map from a file extension to the corresponding
 * MIME type and the other way round based on an internal database; in addition, method
 * {@link #isMimeTypeCompressible(String)} checks whether data of a MIME type can be effectively
 * compressed. MIME types and associated extensions have been imported from the Apache Web server
 * media type database (<a
 * href="http://svn.apache.org/viewvc/httpd/httpd/trunk/docs/conf/mime.types?view=markup"
 * >http://svn.apache.org/viewvc/httpd/httpd/trunk/docs/conf/mime.types?view=markup</a>). A list
 * of non-compressed media types have been obtained from here: <a href=
 * "http://pic.dhe.ibm.com/infocenter/storwize/unified_ic/index.jsp?topic=
 * %2Fcom.ibm.storwize.v7000.unified.140.doc%2Fifs_plancompdata.html"
 * >http://pic.dhe.ibm.com/infocenter/storwize/unified_ic/index.jsp?topic=%2Fcom.ibm.storwize.
 * v7000.unified.140.doc%2Fifs_plancompdata.html</a>. Some manual modification to the data has
 * been done as well, with to goal to enforce that a file association is mapped to at most a media
 * type (a few entries from the Apache DB had to be changed).</li>
 * <li><b>Factories for value objects</b>. Method {@link #getValueFactory()} returns a singleton,
 * memory-optimized {@code ValueFactory} for creating {@code Statement}s and {@code Value}s (
 * {@code URI}s, {@code BNode}s, {@code Literal}s); method {@link #getDatatypeFactory()} returns a
 * singleton factory for creating instances of XML Schema structured types, such as
 * {@link XMLGregorianCalendar} instances.</li>
 * <li><b>Total and partial ordering</b>. Methods {@link #getTotalComparator()} and
 * {@link #getPartialComparator()} returns two general-purpose comparators that impose,
 * respectively, a total and a partial order over objects of the data model. The total order allow
 * comparing any pair of {@code Value}s, {@code Statement}s, {@code Record}s and scalars
 * convertible to literal values; resorting to type comparison if the denoted value belong to
 * incomparable domains. The partial order compares only instances of compatible types (e.g.,
 * numbers with numbers), and can thus be better suited for the evaluation of conditions (while
 * the total order can help with the presentation of data).</li>
 * <li><b>Prefix-to-namespace mappings management support</b>. Method {@link #getNamespaceMap()}
 * returns a prefix-to-namespace map with common bindings as defined on the {@code prefix.cc} site
 * (as of end 2013); it can be used to achieve a reasonable presentation of URIs, e.g., for RDF
 * serialization. Methods {@link #newNamespaceMap()} and {@link #newNamespaceMap(Map, Map)} create
 * either an empty, optimized prefix-to-namespace map or a combined prefix-to-namespace map that
 * merges definitions of two input maps. Method {@link #namespaceToPrefix(String, Map)} performs a
 * reverse lookup in a prefix-to-namespace map, efficiently returning the prefix for a given
 * namespace.</li>
 * <li><b>General-purpose conversion</b>. Methods {@link #convert(Object, Class)} and
 * {@link #convert(Object, Class, Object)} attempts conversion from a data model object to another
 * data model class or scalar class compatible convertible to a literal object. More details about
 * the conversion are specified in the associated Javadoc documentation.</li>
 * <li><b>Value normalization</b>. Methods {@link #normalize(Object)} and
 * {@link #normalize(Object, Collection)} accepts any object of a data model class or of a scalar
 * class convertible to a scalar literal (e.g., an integer) and normalize it to an instance of the
 * three data model classes: {@code Value}, {@code Statement} and {@code Record}.</li>
 * <li><b>String rendering and parsing</b>. Methods {@link #toString(Object, Map)} and
 * {@link #toString(Object, Map, boolean)} generate a string representation of any data model
 * object, using the supplied prefix-to-namespace mappings and possibly including a full listing
 * of record properties. Method {@link #parseValue(String, Map)} allow parsing a {@code Value} out
 * of a Turtle / TriG string, such as the ones produced by {@code toString(...)} methods.</li>
 * </ul>
 * 
 * <p style="color:red">
 * TODO: this class needs urgent refactoring
 * </p>
 */
public final class Data {

    private static final Ordering<Object> TOTAL_ORDERING = new TotalOrdering();

    private static final Comparator<Object> PARTIAL_ORDERING = new PartialOrdering(TOTAL_ORDERING);

    private static final Map<String, String> COMMON_NAMESPACES;

    private static final Map<String, String> COMMON_PREFIXES;

    private static final Set<String> UNCOMPRESSIBLE_MIME_TYPES;

    private static final Map<String, String> EXTENSIONS_TO_MIME_TYPES;

    private static final Map<String, List<String>> MIME_TYPES_TO_EXTENSIONS;

    private static final Map<String, URI> LANGUAGE_CODES_TO_URIS;

    private static final Map<URI, String> LANGUAGE_URIS_TO_CODES;

    private static final DatatypeFactory DATATYPE_FACTORY;

    private static final ValueFactory VALUE_FACTORY;

    private static ListeningScheduledExecutorService executor;

    private static AtomicBoolean executorPrivate = new AtomicBoolean();

    static {
        VALUE_FACTORY = CompactValueFactory.getInstance();
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
        final Map<String, URI> codesToURIs = Maps.newHashMap();
        final Map<URI, String> urisToCodes = Maps.newHashMap();
        for (final String language : Locale.getISOLanguages()) {
            final Locale locale = new Locale(language);
            final URI uri = Data.getValueFactory().createURI("http://lexvo.org/id/iso639-3/",
                    locale.getISO3Language());
            codesToURIs.put(language, uri);
            urisToCodes.put(uri, language);
        }
        LANGUAGE_CODES_TO_URIS = ImmutableMap.copyOf(codesToURIs);
        LANGUAGE_URIS_TO_CODES = ImmutableMap.copyOf(urisToCodes);
    }

    static {
        try {
            final ImmutableMap.Builder<String, String> nsToPrefixBuilder;
            final ImmutableMap.Builder<String, String> prefixToNsBuilder;

            nsToPrefixBuilder = ImmutableMap.builder();
            prefixToNsBuilder = ImmutableMap.builder();

            for (final String line : Resources.readLines(Data.class.getResource("prefixes"),
                    Charsets.UTF_8)) {
                final Iterator<String> i = Splitter.on(' ').omitEmptyStrings().split(line)
                        .iterator();
                final String uri = i.next();
                final String prefix = i.next();
                nsToPrefixBuilder.put(uri, prefix);
                prefixToNsBuilder.put(prefix, uri);
                while (i.hasNext()) {
                    prefixToNsBuilder.put(i.next(), uri);
                }
            }

            COMMON_NAMESPACES = prefixToNsBuilder.build();
            COMMON_PREFIXES = nsToPrefixBuilder.build();

            final ImmutableSet.Builder<String> uncompressibleMtsBuilder = ImmutableSet.builder();
            final ImmutableMap.Builder<String, String> extToMtIndexBuilder = ImmutableMap
                    .builder();
            final ImmutableMap.Builder<String, List<String>> mtToExtsIndexBuilder = ImmutableMap
                    .builder();

            final URL resource = Data.class.getResource("mimetypes");
            for (final String line : Resources.readLines(resource, Charsets.UTF_8)) {
                if (!line.isEmpty() && line.charAt(0) != '#') {
                    final Iterator<String> iterator = Splitter.on(' ').trimResults()
                            .omitEmptyStrings().split(line).iterator();
                    final String mimeType = iterator.next();
                    final ImmutableList.Builder<String> extBuilder = ImmutableList.builder();
                    while (iterator.hasNext()) {
                        final String token = iterator.next();
                        if ("*".equals(token)) {
                            uncompressibleMtsBuilder.add(mimeType);
                        } else {
                            extBuilder.add(token);
                            extToMtIndexBuilder.put(token, mimeType);
                        }
                    }
                    mtToExtsIndexBuilder.put(mimeType, extBuilder.build());
                }
            }

            UNCOMPRESSIBLE_MIME_TYPES = uncompressibleMtsBuilder.build();
            EXTENSIONS_TO_MIME_TYPES = extToMtIndexBuilder.build();
            MIME_TYPES_TO_EXTENSIONS = mtToExtsIndexBuilder.build();

        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    /**
     * Returns the executor shared by KnowledgeStore components. If no executor is setup using
     * {@link #setExecutor(ScheduledExecutorService)}, an executor is automatically created using
     * the thread number and naming given by system properties
     * {@code eu.fbk.knowledgestore.threadCount} and {@code eu.fbk.knowledgestore.threadNumber}.
     * 
     * @return the shared executor
     */
    public static ListeningScheduledExecutorService getExecutor() {
        synchronized (executorPrivate) {
            if (executor == null) {
                final String threadName = Objects.firstNonNull(
                        System.getProperty("eu.fbk.knowledgestore.threadName"), "worker-%02d");
                int threadCount = 32;
                try {
                    threadCount = Integer.parseInt(System
                            .getProperty("eu.fbk.knowledgestore.threadCount"));
                } catch (final Throwable ex) {
                    // ignore
                }
                executor = Util.newScheduler(threadCount, threadName, true);
                executorPrivate.set(true);
            }
            return executor;
        }
    }

    /**
     * Setup the executor shared by KnowledgeStore components. If another executor was previously
     * in use, it will not be used anymore; in case it was the executor automatically created by
     * the system, it will be shutdown.
     * 
     * @param newExecutor
     *            the new executor
     */
    public static void setExecutor(final ScheduledExecutorService newExecutor) {
        Preconditions.checkNotNull(newExecutor);
        ScheduledExecutorService executorToShutdown = null;
        synchronized (executorPrivate) {
            if (executor != null && executorPrivate.get()) {
                executorToShutdown = executor;
            }
            executor = Util.decorate(newExecutor);
            executorPrivate.set(false);
        }
        if (executorToShutdown != null) {
            executorToShutdown.shutdown();
        }
    }

    /**
     * Returns an optimized {@code ValueFactory} for creating RDF {@code URI}s, {@code BNode}s,
     * {@code Literal}s and {@code Statement}s. Note that while any {@code ValueFactory} can be
     * used for this purpose (including the {@link ValueFactoryImpl} shipped with Sesame), the
     * factory returned by this method has been optimized to create objects that minimize the use
     * of memory, thus allowing to keep more objects / records in memory.
     * 
     * @return a singleton {@code ValueFactory}
     */
    public static ValueFactory getValueFactory() {
        return VALUE_FACTORY;
    }

    /**
     * Returns a {@code DatatypeFactory} for creating {@code XMLGregorianCalendar} instances and
     * instances of other XML schema structured types.
     * 
     * @return a singleton {@code DatatypeFactory}
     */
    public static DatatypeFactory getDatatypeFactory() {
        return DATATYPE_FACTORY;
    }

    /**
     * Returns a comparator imposing a total order over objects of the data model ({@code Value}s,
     * {@code Statement}s, {@code Record}s). Objects are organized in groups: booleans, strings,
     * numbers (longs, ints, ...), calendar dates, URIs, BNodes and records. Ordering is done
     * first based on the group. Then, among a group the ordering is the natural one defined for
     * its elements, where applicable (booleans, strings, numbers, dates), otherwise the following
     * extensions are adopted: (i) statements are sorted by context first, followed by subject,
     * predicate and object; (ii) identifiers are sorted based on their string representation.
     * Note that comparison of dates follows the XML Schema specification with the only exception
     * that incomparable dates according to this specification (due to unknown timezone) are
     * considered equal.
     * 
     * @return a singleton comparator imposing a total order over objects of the data model
     */
    public static Comparator<Object> getTotalComparator() {
        return TOTAL_ORDERING;
    }

    /**
     * Returns a comparator imposing a partial order over objects of the data model ({@code Value}
     * s, {@code Statement}s, {@code Record}s). This comparator behaves like the one of
     * {@link #getTotalComparator()} but in case objects belong to different groups (e.g., an
     * integer and a string) an exception is thrown rather than applying group sorting, thus
     * resulting in a more strict comparison that may be useful, e.g., for checking conditions.
     * 
     * @return a singleton comparator imposing a partial order over objects of the data model
     */
    public static Comparator<Object> getPartialComparator() {
        return PARTIAL_ORDERING;
    }

    /**
     * Returns a map of common prefix-to-namespace mappings, taken from a {@code prefix.cc} dump.
     * The returned map provides multiple prefixes for some namespace; it also support fast
     * reverse prefix lookup via {@link #namespaceToPrefix(String, Map)}.
     * 
     * @return a singleton map of common prefix-to-namespace mappings
     */
    public static Map<String, String> getNamespaceMap() {
        return COMMON_NAMESPACES;
    }

    /**
     * Creates a new, empty prefix-to-namespace map that supports fast reverse prefix lookup via
     * {@link #namespaceToPrefix(String, Map)}.
     * 
     * @return the created prefix-to-namespace map, empty
     */
    public static Map<String, String> newNamespaceMap() {
        return new NamespaceMap();
    }

    /**
     * Creates a new prefix-to-namespace map combining the mappings in the supplied maps. Mappings
     * in the {@code primaryNamespaceMap} take precedence, while the {@code secondaryNamespaceMap}
     * is accessed only if a mapping is not found in the primary map. Modification operations
     * target exclusively the {@code primaryNamespaceMap}.
     * 
     * @param primaryNamespaceMap
     *            the primary prefix-to-namespace map, not null
     * @param secondaryNamespaceMap
     *            the secondary prefix-to-namespace map, not null
     * @return the created, combined prefix-to-namespace map
     */
    public static Map<String, String> newNamespaceMap(
            final Map<String, String> primaryNamespaceMap,
            final Map<String, String> secondaryNamespaceMap) {

        Preconditions.checkNotNull(primaryNamespaceMap);
        Preconditions.checkNotNull(secondaryNamespaceMap);

        if (primaryNamespaceMap == secondaryNamespaceMap) {
            return primaryNamespaceMap;
        } else {
            return new NamespaceCombinedMap(primaryNamespaceMap, secondaryNamespaceMap);
        }
    }

    /**
     * Performs a reverse lookup of the prefix corresponding to a namespace in a
     * prefix-to-namespace map. This method tries a number of strategy for efficiently performing
     * the reverse lookup, expliting features of the particular prefix-to-namespace map supplied
     * (e.g., whether it is a {@code BiMap} from Guava, a namespace map from
     * {@link #newNamespaceMap()} or the map of common prefix-to-namespace declarations); as a
     * result, calling this method may be significantly faster than manually looping over all the
     * prefix-to-namespace entries.
     * 
     * @param namespace
     *            the namespace the corresponding prefix should be looked up
     * @param namespaceMap
     *            the prefix-to-namespace map containing the searched mapping
     * @return the prefix corresponding to the namespace, or null if no mapping is defined
     */
    @Nullable
    public static String namespaceToPrefix(final String namespace,
            final Map<String, String> namespaceMap) {

        Preconditions.checkNotNull(namespace);

        if (namespaceMap == COMMON_NAMESPACES) {
            return COMMON_PREFIXES.get(namespace);

        } else if (namespaceMap instanceof NamespaceCombinedMap) {
            final NamespaceCombinedMap map = (NamespaceCombinedMap) namespaceMap;
            String prefix = namespaceToPrefix(namespace, map.primaryNamespaces);
            if (prefix == null) {
                prefix = namespaceToPrefix(namespace, map.secondaryNamespaces);
            }
            return prefix;

        } else if (namespaceMap instanceof NamespaceMap) {
            return ((NamespaceMap) namespaceMap).getPrefix(namespace);

        } else if (namespaceMap instanceof BiMap) {
            return ((BiMap<String, String>) namespaceMap).inverse().get(namespace);

        } else {
            Preconditions.checkNotNull(namespaceMap);
            for (final Map.Entry<String, String> entry : namespaceMap.entrySet()) {
                if (entry.getValue().equals(namespace)) {
                    return entry.getKey();
                }
            }
            return null;
        }
    }

    /**
     * Checks whether the specific MIME type can be compressed.
     * 
     * @param mimeType
     *            the MIME type
     * @return true if compression can reduce size of data belonging to the specified MIME type,
     *         or if there is no knowledge about compressibility of the specified MIME type; false
     *         if it is known that compression cannot help (e.g., because the media type is
     *         already compressed).
     */
    public static boolean isMimeTypeCompressible(final String mimeType) {
        Preconditions.checkNotNull(mimeType);
        final int index = mimeType.indexOf(';');
        final String key = (index < 0 ? mimeType : mimeType.substring(0, index)).toLowerCase();
        return !UNCOMPRESSIBLE_MIME_TYPES.contains(key);
    }

    /**
     * Returns the MIME type for the file extension specified, if known. If the parameter contains
     * a full file name, its extension is extracted and used for the lookup.
     * 
     * @param fileNameOrExtension
     *            the file extension or file name (from which the extension is extracted)
     * @return the corresponding MIME type; null if the file extension specified is not contained
     *         in the internal database
     */
    public static String extensionToMimeType(final String fileNameOrExtension) {
        Preconditions.checkNotNull(fileNameOrExtension);
        final int index = fileNameOrExtension.lastIndexOf('.');
        final String extension = index < 0 ? fileNameOrExtension : fileNameOrExtension
                .substring(index + 1);
        return EXTENSIONS_TO_MIME_TYPES.get(extension);
    }

    /**
     * Returns the file extensions commonly associated to the specified MIME type. Extensions are
     * reported without a leading {@code '.'} (e.g., {@code txt}). In case multiple extensions are
     * returned, it is safe to consider the first one as the most common and preferred.
     * 
     * @param mimeType
     *            the MIME type
     * @return a list with the extensions mapped to the MIME type, if known; an empty list
     *         otherwise
     */
    public static List<String> mimeTypeToExtensions(final String mimeType) {
        Preconditions.checkNotNull(mimeType);
        final int index = mimeType.indexOf(';');
        final String key = (index < 0 ? mimeType : mimeType.substring(0, index)).toLowerCase();
        final List<String> result = MIME_TYPES_TO_EXTENSIONS.get(key);
        return result != null ? result : ImmutableList.<String>of();
    }

    /**
     * Returns the language URI for the ISO 639 code (2-letters, 3-letters) specified. The
     * returned URI is in the form {@code http://lexvo.org/id/iso639-3/XYZ}.
     * 
     * @param code
     *            the ISO 639 language code, possibly null
     * @return the corresponding URI, or null if the input is null
     * @throws IllegalArgumentException
     *             in case the supplied string is not a valid ISO 639 2-letters or 3-letters code
     */
    @Nullable
    public static URI languageCodeToURI(@Nullable final String code)
            throws IllegalArgumentException {
        if (code == null) {
            return null;
        }
        final int length = code.length();
        if (length == 2) {
            final URI uri = LANGUAGE_CODES_TO_URIS.get(code);
            if (uri != null) {
                return uri;
            }
        } else if (length == 3) {
            final URI uri = Data.getValueFactory().createURI(
                    "http://lexvo.org/id/iso639-3/" + code);
            if (LANGUAGE_URIS_TO_CODES.containsKey(uri)) {
                return uri;
            }
        }
        throw new IllegalArgumentException("Invalid language code: " + code);
    }

    /**
     * Returns the 2-letter ISO 639 code for the language URI supplied. The URI must be in the
     * form {@code http://lexvo.org/id/iso639-3/XYZ}.
     * 
     * @param uri
     *            the language URI, possibly null
     * @return the corresponding ISO 639 2-letter code, or null if the input URI is null
     * @throws IllegalArgumentException
     *             if the supplied URI is not valid
     */
    @Nullable
    public static String languageURIToCode(@Nullable final URI uri)
            throws IllegalArgumentException {
        if (uri == null) {
            return null;
        }
        final String code = LANGUAGE_URIS_TO_CODES.get(uri);
        if (code != null) {
            return code;
        }
        throw new IllegalArgumentException("Invalid language URI: " + uri);
    }

    /**
     * Utility method to compute an hash string from a vararg list of objects. The returned string
     * is 16 characters long, starts with {@code A-Za-z} and contains only characters
     * {@code A-Za-z0-9}.
     * 
     * @param objects
     *            the objects to compute the hash from
     * @return the computed hash string
     */
    public static String hash(final Object... objects) {
        final Hasher hasher = Hashing.md5().newHasher();
        for (final Object object : objects) {
            if (object instanceof CharSequence) {
                hasher.putString((CharSequence) object, Charsets.UTF_16LE);
            } else if (object instanceof byte[]) {
                hasher.putBytes((byte[]) object);
            } else if (object instanceof Character) {
                hasher.putChar((Character) object);
            } else if (object instanceof Boolean) {
                hasher.putBoolean((Boolean) object);
            } else if (object instanceof Integer) {
                hasher.putInt(((Integer) object).intValue());
            } else if (object instanceof Long) {
                hasher.putLong(((Long) object).longValue());
            } else if (object instanceof Double) {
                hasher.putDouble(((Double) object).doubleValue());
            } else if (object instanceof Float) {
                hasher.putFloat(((Float) object).floatValue());
            } else if (object instanceof Byte) {
                hasher.putByte(((Byte) object).byteValue());
            } else {
                hasher.putString(object.toString(), Charsets.UTF_16LE);
            }
        }
        final byte[] bytes = hasher.hash().asBytes();
        final StringBuilder builder = new StringBuilder(16);
        int max = 52;
        for (int i = 0; i < bytes.length; ++i) {
            final int n = (bytes[i] & 0x7F) % max;
            if (n < 26) {
                builder.append((char) (65 + n));
            } else if (n < 52) {
                builder.append((char) (71 + n));
            } else {
                builder.append((char) (n - 4));
            }
            max = 62;
        }
        return builder.toString();
    }

    /**
     * General conversion facility. This method attempts to convert a supplied {@code object} to
     * an instance of the class specified. If the input is null, null is returned. If conversion
     * is unsupported or fails, an exception is thrown. The following table lists the supported
     * conversions: <blockquote>
     * <table border="1">
     * <thead>
     * <tr>
     * <th>From classes (and sub-classes)</th>
     * <th>To classes (and super-classes)</th>
     * </tr>
     * </thead><tbody>
     * <tr>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean})</td>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string})</td>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string}), {@code URI} (as uri
     * string), {@code BNode} (as BNode ID), {@link Integer}, {@link Long}, {@link Double},
     * {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal}, {@link BigInteger},
     * {@link AtomicInteger}, {@link AtomicLong}, {@link Boolean}, {@link XMLGregorianCalendar},
     * {@link GregorianCalendar}, {@link Date} (via parsing), {@link Character} (length >= 1)</td>
     * </tr>
     * <tr>
     * <td>{@link Number}, {@link Literal} (any numeric {@code xsd:} type)</td>
     * <td>{@link Literal} (top-level numeric {@code xsd:} type), {@link Integer}, {@link Long},
     * {@link Double}, {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal},
     * {@link BigInteger}, {@link AtomicInteger}, {@link AtomicLong}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}, {@code xsd:date})</td>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link URI}</td>
     * <td>{@link URI}, {@link Record} (ID assigned), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link BNode}</td>
     * <td>{@link BNode}, {@link URI} (skolemization), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement}</td>
     * <td>{@link Statement}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Record}</td>
     * <td>{@link Record}, {@link URI} (ID extracted), {@link String}</td>
     * </tr>
     * </tbody>
     * </table>
     * </blockquote>
     * 
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or null if {@code object} was null
     * @throws IllegalArgumentException
     *             in case conversion fails or is unsupported for the {@code object} and class
     *             specified
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz)
            throws IllegalArgumentException {
        if (object == null) {
            Preconditions.checkNotNull(clazz);
            return null;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        final T result = (T) convertObject(object, clazz);
        if (result != null) {
            return result;
        }
        throw new IllegalArgumentException("Unsupported conversion of " + object + " to " + clazz);
    }

    /**
     * General conversion facility, with fall back to default value. This method operates as
     * {@link #convert(Object, Class)}, but in case the input is null or conversion is not
     * supported returns the specified default value.
     * 
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param defaultValue
     *            the default value to fall back to
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or the default value if {@code object} was null,
     *         conversion failed or is unsupported
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz,
            @Nullable final T defaultValue) {
        if (object == null) {
            Preconditions.checkNotNull(clazz);
            return defaultValue;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        try {
            final T result = (T) convertObject(object, clazz);
            return result != null ? result : defaultValue;
        } catch (final RuntimeException ex) {
            return defaultValue;
        }
    }

    @Nullable
    private static Object convertObject(final Object object, final Class<?> clazz) {
        if (object instanceof Literal) {
            return convertLiteral((Literal) object, clazz);
        } else if (object instanceof URI) {
            return convertURI((URI) object, clazz);
        } else if (object instanceof String) {
            return convertString((String) object, clazz);
        } else if (object instanceof Number) {
            return convertNumber((Number) object, clazz);
        } else if (object instanceof Boolean) {
            return convertBoolean((Boolean) object, clazz);
        } else if (object instanceof XMLGregorianCalendar) {
            return convertCalendar((XMLGregorianCalendar) object, clazz);
        } else if (object instanceof BNode) {
            return convertBNode((BNode) object, clazz);
        } else if (object instanceof Statement) {
            return convertStatement((Statement) object, clazz);
        } else if (object instanceof Record) {
            return convertRecord((Record) object, clazz);
        } else if (object instanceof GregorianCalendar) {
            final XMLGregorianCalendar calendar = getDatatypeFactory().newXMLGregorianCalendar(
                    (GregorianCalendar) object);
            return clazz == XMLGregorianCalendar.class ? calendar : convertCalendar(calendar,
                    clazz);
        } else if (object instanceof Date) {
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime((Date) object);
            final XMLGregorianCalendar xmlCalendar = getDatatypeFactory().newXMLGregorianCalendar(
                    calendar);
            return clazz == XMLGregorianCalendar.class ? xmlCalendar : convertCalendar(
                    xmlCalendar, clazz);
        } else if (object instanceof Enum<?>) {
            return convertEnum((Enum<?>) object, clazz);
        }
        return null;
    }

    @Nullable
    private static Object convertStatement(final Statement statement, final Class<?> clazz) {
        if (clazz.isAssignableFrom(String.class)) {
            return statement.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertLiteral(final Literal literal, final Class<?> clazz) {
        final URI datatype = literal.getDatatype();
        if (datatype == null || datatype.equals(XMLSchema.STRING)) {
            return convertString(literal.getLabel(), clazz);
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return convertBoolean(literal.booleanValue(), clazz);
        } else if (datatype.equals(XMLSchema.DATE) || datatype.equals(XMLSchema.DATETIME)) {
            return convertCalendar(literal.calendarValue(), clazz);
        } else if (datatype.equals(XMLSchema.INT)) {
            return convertNumber(literal.intValue(), clazz);
        } else if (datatype.equals(XMLSchema.LONG)) {
            return convertNumber(literal.longValue(), clazz);
        } else if (datatype.equals(XMLSchema.DOUBLE)) {
            return convertNumber(literal.doubleValue(), clazz);
        } else if (datatype.equals(XMLSchema.FLOAT)) {
            return convertNumber(literal.floatValue(), clazz);
        } else if (datatype.equals(XMLSchema.SHORT)) {
            return convertNumber(literal.shortValue(), clazz);
        } else if (datatype.equals(XMLSchema.BYTE)) {
            return convertNumber(literal.byteValue(), clazz);
        } else if (datatype.equals(XMLSchema.DECIMAL)) {
            return convertNumber(literal.decimalValue(), clazz);
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            return convertNumber(literal.integerValue(), clazz);
        } else if (datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.NON_POSITIVE_INTEGER)
                || datatype.equals(XMLSchema.NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.POSITIVE_INTEGER)) {
            return convertNumber(literal.integerValue(), clazz); // infrequent integer cases
        } else if (datatype.equals(XMLSchema.NORMALIZEDSTRING) || datatype.equals(XMLSchema.TOKEN)
                || datatype.equals(XMLSchema.NMTOKEN) || datatype.equals(XMLSchema.LANGUAGE)
                || datatype.equals(XMLSchema.NAME) || datatype.equals(XMLSchema.NCNAME)) {
            return convertString(literal.getLabel(), clazz); // infrequent string cases
        }
        return null;
    }

    @Nullable
    private static Object convertBoolean(final Boolean bool, final Class<?> clazz) {
        if (clazz == Boolean.class || clazz == boolean.class) {
            return bool;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return getValueFactory().createLiteral(bool);
        } else if (clazz.isAssignableFrom(String.class)) {
            return bool.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertString(final String string, final Class<?> clazz) {
        if (clazz.isInstance(string)) {
            return string;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return getValueFactory().createLiteral(string, XMLSchema.STRING);
        } else if (clazz.isAssignableFrom(URI.class)) {
            return getValueFactory().createURI(string);
        } else if (clazz.isAssignableFrom(BNode.class)) {
            return getValueFactory().createBNode(
                    string.startsWith("_:") ? string.substring(2) : string);
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return Boolean.valueOf(string);
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf(string);
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(string);
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(string);
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(string);
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf(string);
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf(string);
        } else if (clazz == BigDecimal.class) {
            return new BigDecimal(string);
        } else if (clazz == BigInteger.class) {
            return new BigInteger(string);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(Integer.parseInt(string));
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(Long.parseLong(string));
        } else if (clazz == Date.class) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return getDatatypeFactory().newXMLGregorianCalendar(fixed).toGregorianCalendar()
                    .getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return getDatatypeFactory().newXMLGregorianCalendar(fixed).toGregorianCalendar();
        } else if (clazz.isAssignableFrom(XMLGregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return getDatatypeFactory().newXMLGregorianCalendar(fixed);
        } else if (clazz == Character.class || clazz == char.class) {
            return string.isEmpty() ? null : string.charAt(0);
        } else if (clazz.isEnum()) {
            for (final Object constant : clazz.getEnumConstants()) {
                if (string.equalsIgnoreCase(((Enum<?>) constant).name())) {
                    return constant;
                }
            }
            throw new IllegalArgumentException("Illegal " + clazz.getSimpleName() + " constant: "
                    + string);
        }
        return null;
    }

    @Nullable
    private static Object convertNumber(final Number number, final Class<?> clazz) {
        if (clazz.isAssignableFrom(Literal.class)) {
            // TODO: perhaps datatype should be based on denoted value, rather than class (e.g.
            // 3.0f -> 3 xsd:byte)
            if (number instanceof Integer || number instanceof AtomicInteger) {
                return getValueFactory().createLiteral(number.intValue());
            } else if (number instanceof Long || number instanceof AtomicLong) {
                return getValueFactory().createLiteral(number.longValue());
            } else if (number instanceof Double) {
                return getValueFactory().createLiteral(number.doubleValue());
            } else if (number instanceof Float) {
                return getValueFactory().createLiteral(number.floatValue());
            } else if (number instanceof Short) {
                return getValueFactory().createLiteral(number.shortValue());
            } else if (number instanceof Byte) {
                return getValueFactory().createLiteral(number.byteValue());
            } else if (number instanceof BigDecimal) {
                return getValueFactory().createLiteral(number.toString(), XMLSchema.DECIMAL);
            } else if (number instanceof BigInteger) {
                return getValueFactory().createLiteral(number.toString(), XMLSchema.INTEGER);
            }
        } else if (clazz.isAssignableFrom(String.class)) {
            return number.toString();
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf(number.intValue());
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(number.longValue());
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(number.doubleValue());
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(number.floatValue());
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf(number.shortValue());
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf(number.byteValue());
        } else if (clazz == BigDecimal.class) {
            return toBigDecimal(number);
        } else if (clazz == BigInteger.class) {
            return toBigInteger(number);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(number.intValue());
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(number.longValue());
        }
        return null;
    }

    @Nullable
    private static Object convertCalendar(final XMLGregorianCalendar calendar, //
            final Class<?> clazz) {
        if (clazz.isInstance(calendar)) {
            return calendar;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return getValueFactory().createLiteral(calendar);
        } else if (clazz.isAssignableFrom(String.class)) {
            return calendar.toXMLFormat();
        } else if (clazz == Date.class) {
            return calendar.toGregorianCalendar().getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            return calendar.toGregorianCalendar();
        }
        return null;
    }

    @Nullable
    private static Object convertURI(final URI uri, final Class<?> clazz) {
        if (clazz.isInstance(uri)) {
            return uri;
        } else if (clazz.isAssignableFrom(String.class)) {
            return uri.stringValue();
        } else if (clazz == Record.class) {
            return Record.create(uri);
        }
        return null;
    }

    @Nullable
    private static Object convertBNode(final BNode bnode, final Class<?> clazz) {
        if (clazz.isInstance(bnode)) {
            return bnode;
        } else if (clazz.isAssignableFrom(URI.class)) {
            return getValueFactory().createURI("bnode:" + bnode.getID());
        } else if (clazz.isAssignableFrom(String.class)) {
            return "_:" + bnode.getID();
        }
        return null;
    }

    @Nullable
    private static Object convertRecord(final Record record, final Class<?> clazz) {
        if (clazz.isInstance(record)) {
            return record;
        } else if (clazz.isAssignableFrom(URI.class)) {
            return record.getID();
        } else if (clazz.isAssignableFrom(String.class)) {
            return record.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertEnum(final Enum<?> constant, final Class<?> clazz) {
        if (clazz.isInstance(constant)) {
            return constant;
        } else if (clazz.isAssignableFrom(String.class)) {
            return constant.name();
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return getValueFactory().createLiteral(constant.name(), XMLSchema.STRING);
        }
        return null;
    }

    private static BigDecimal toBigDecimal(final Number number) {
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        } else if (number instanceof Double || number instanceof Float) {
            final double value = number.doubleValue();
            return Double.isInfinite(value) || Double.isNaN(value) ? null : new BigDecimal(value);
        } else {
            return new BigDecimal(number.longValue());
        }
    }

    private static BigInteger toBigInteger(final Number number) {
        if (number instanceof BigInteger) {
            return (BigInteger) number;
        } else if (number instanceof BigDecimal) {
            return ((BigDecimal) number).toBigInteger();
        } else if (number instanceof Double || number instanceof Float) {
            return new BigDecimal(number.doubleValue()).toBigInteger();
        } else {
            return BigInteger.valueOf(number.longValue());
        }
    }

    /**
     * Normalizes the supplied object to an object of the data model. The method operates as
     * follows:
     * <ul>
     * <li>if the input is null, null is returned;</li>
     * <li>if the input is already an object of the data model ({@link Record}, {@link Value},
     * {@link Statement}), it is returned unchanged;</li>
     * <li>if the input is an iterable or array, its unique element is converted if length is 1;
     * null is returned if length is 0; {@link IllegalArgumentException} is thrown otherwise;</li>
     * <li>in all the other cases, conversion to {@code Value} is performed.</li>
     * </ul>
     * 
     * @param object
     *            the object to normalize, possibly an array or iterable
     * @return the corresponding object of the data model
     * @throws IllegalArgumentException
     *             in case the supplied object is an array or iterable with more than one element,
     *             or if conversion was required but failed or was unsupported
     */
    @Nullable
    public static Object normalize(@Nullable final Object object) throws IllegalArgumentException {
        if (object == null || object instanceof Record || object instanceof Value
                || object instanceof Statement) {
            return object;
        }
        if (object.getClass().isArray()) {
            final int length = Array.getLength(object);
            if (length == 0) {
                return null;
            }
            if (length == 1) {
                return normalize(Array.get(object, 0));
            }
            throw new IllegalArgumentException(
                    "Cannot extract a unique node from array of length " + length);
        }
        if (object instanceof Iterable<?>) {
            Object result = null;
            for (final Object element : (Iterable<?>) object) {
                if (result != null) {
                    throw new IllegalArgumentException(
                            "cannot extract a unique node from iterable " + object);
                }
                result = normalize(element);
            }
            return result;
        }
        return convert(object, Value.class);
    }

    /**
     * Normalizes the supplied object to zero or more objects of the data model, which are added
     * to the collection specified. The method operates as follows:
     * <ul>
     * <li>if the input is null, no nodes are produced and false is returned</li>
     * <li>if the input is already an object of the data model ({@link Record}, {@link Value},
     * {@link Statement}), it is stored unchanged in the supplied collection;</li>
     * <li>if the input is an iterable or array, its elements are converted recursively (i.e.,
     * {@code normalize()} is called for each of them, using the same collection supplied);</li>
     * <li>in all the other cases, conversion to {@code Value} is performed.</li>
     * </ul>
     * 
     * @param object
     *            the object to normalize, possibly an array or iterable
     * @param collection
     *            a collection where to add the resulting nodes
     * @return true, if the collection changed as a result of the call
     * @throws IllegalArgumentException
     *             in case conversion was necessary but failed or was unsupported
     */
    public static boolean normalize(final Object object, final Collection<Object> collection)
            throws IllegalArgumentException {
        if (object == null) {
            return false;
        } else if (object.getClass().isArray()) {
            final int length = Array.getLength(object);
            if (length == 0) {
                return false;
            } else if (length == 1) {
                return normalize(Array.get(object, 0), collection);
            } else if (object instanceof Object[]) {
                return normalize(Arrays.asList((Object[]) object), collection);
            } else if (object instanceof int[]) {
                return normalize(Ints.asList((int[]) object), collection);
            } else if (object instanceof long[]) {
                return normalize(Longs.asList((long[]) object), collection);
            } else if (object instanceof double[]) {
                return normalize(Doubles.asList((double[]) object), collection);
            } else if (object instanceof float[]) {
                return normalize(Floats.asList((float[]) object), collection);
            } else if (object instanceof short[]) {
                return normalize(Shorts.asList((short[]) object), collection);
            } else if (object instanceof boolean[]) {
                return normalize(Booleans.asList((boolean[]) object), collection);
            } else if (object instanceof char[]) {
                return normalize(Chars.asList((char[]) object), collection);
            } else {
                throw new IllegalArgumentException("Unsupported primitive array type: "
                        + object.getClass());
            }
        } else if (object instanceof Iterable<?>) {
            boolean changed = false;
            for (final Object element : (Iterable<?>) object) {
                if (normalize(element, collection)) {
                    changed = true;
                }
            }
            return changed;
        } else {
            return collection.add(normalize(object));
        }
    }

    /**
     * Parses an RDF value out of a string. The string can be in the Turtle / N3 / TriG format,
     * i.e., {@code "literal", "literal"^^^datatype, "literal"@lang", <uri>, _:bnode} (strings
     * produced by {@link #toString(Object, Map, boolean)} obey this format).
     * 
     * @param string
     *            the string to parse, possibly null
     * @param namespaces
     *            the optional prefix-to-namespace mappings to use for parsing the string,
     *            possibly null
     * @return the parsed value, or null if a null string was passed as input
     * @throws ParseException
     *             in case parsing fails
     */
    @Nullable
    public static Value parseValue(@Nullable final String string,
            @Nullable final Map<String, String> namespaces) throws ParseException {

        if (string == null) {
            return null;
        }

        try {
            final int length = string.length();
            if (string.startsWith("\"") || string.startsWith("'")) {
                if (string.charAt(length - 1) == '"' || string.charAt(length - 1) == '\'') {
                    return getValueFactory().createLiteral(string.substring(1, length - 1));
                }
                int index = string.lastIndexOf("@");
                if (index == length - 3) {
                    final String language = string.substring(index + 1);
                    if (Character.isLetter(language.charAt(0))
                            && Character.isLetter(language.charAt(1))) {
                        return getValueFactory().createLiteral(string.substring(1, index - 1),
                                language);
                    }
                }
                index = string.lastIndexOf("^^");
                if (index > 0) {
                    final String datatype = string.substring(index + 2);
                    try {
                        final URI datatypeURI = (URI) parseValue(datatype, namespaces);
                        return getValueFactory().createLiteral(string.substring(1, index - 1),
                                datatypeURI);
                    } catch (final Throwable ex) {
                        // ignore
                    }
                }
                throw new ParseException(string, "Invalid literal");

            } else if (string.startsWith("_:")) {
                return getValueFactory().createBNode(string.substring(2));

            } else if (string.startsWith("<")) {
                return getValueFactory().createURI(string.substring(1, length - 1));

            } else if (namespaces != null) {
                final int index = string.indexOf(':');
                if (index >= 0) {
                    final String prefix = string.substring(0, index);
                    final String localName = string.substring(index + 1);
                    final String namespace = namespaces.get(prefix);
                    if (namespace != null) {
                        return getValueFactory().createURI(namespace, localName);
                    }
                }
            }
            throw new ParseException(string, "Unparseable value");

        } catch (final RuntimeException ex) {
            throw ex instanceof ParseException ? (ParseException) ex : new ParseException(string,
                    ex.getMessage(), ex);
        }
    }

    /**
     * Returns a string representation of the supplied data model object, optionally using the
     * supplied namespaces and including record properties. Supported objects are {@link Value},
     * {@link Statement}, {@link Record} instances and instances of scalar types that can be
     * converted to {@code Value}s (via {@link #convert(Object, Class)}).
     * 
     * @param object
     *            the data model object, possibly null
     * @param namespaces
     *            the optional prefix-to-namespace mappings to use for generating the string,
     *            possibly null
     * @param includeProperties
     *            true if record properties should be included in the resulting string, in
     *            addition to the record ID
     * @return the produced string, or null if a null object was passed as input
     */
    @Nullable
    public static String toString(@Nullable final Object object,
            @Nullable final Map<String, String> namespaces, final boolean includeProperties) {

        if (object instanceof Record) {
            return ((Record) object).toString(namespaces, includeProperties);

        } else if (object instanceof Statement) {
            final Statement statement = (Statement) object;
            final Resource subj = statement.getSubject();
            final URI pred = statement.getPredicate();
            final Value obj = statement.getObject();
            final Resource ctx = statement.getContext();
            final StringBuilder builder = new StringBuilder();
            builder.append('(');
            toString(subj, namespaces, builder);
            builder.append(',').append(' ');
            toString(pred, namespaces, builder);
            builder.append(',').append(' ');
            toString(obj, namespaces, builder);
            builder.append(")");
            if (statement.getContext() != null) {
                builder.append(' ').append('[');
                toString(ctx, namespaces, builder);
                builder.append(']');
            }
            return builder.toString();

        } else if (object != null) {
            final Value value = convert(object, Value.class);
            final StringBuilder builder = new StringBuilder();
            toString(value, namespaces, builder);
            return builder.toString();
        }

        return null;
    }

    /**
     * Returns a string representation of the supplied data model object, optionally using the
     * supplied namespaces. This method is a shortcut for {@link #toString(Object, Map, boolean)}
     * when no record properties are desired in output.
     * 
     * @param object
     *            the data model object, possibly null
     * @param namespaces
     *            the optional prefix-to-namespace mappings to use for generating the string,
     *            possibly null
     * @return the produced string, or null if a null object was passed as input
     */
    public static String toString(final Object object,
            @Nullable final Map<String, String> namespaces) {
        return toString(object, namespaces, false);
    }

    private static void toString(final Value value,
            @Nullable final Map<String, String> namespaces, final StringBuilder builder) {

        if (value instanceof URI) {
            final URI uri = (URI) value;
            String prefix = null;
            if (namespaces != null) {
                prefix = namespaceToPrefix(uri.getNamespace(), namespaces);
            }
            if (prefix != null) {
                builder.append(prefix).append(':').append(uri.getLocalName());
            } else {
                builder.append('<').append(uri.stringValue()).append('>');
            }

        } else if (value instanceof BNode) {
            builder.append('_').append(':').append(((BNode) value).getID());

        } else {
            final Literal literal = (Literal) value;
            builder.append('\"').append(literal.getLabel()).append('\"');
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                builder.append('^').append('^');
                toString(datatype, namespaces, builder);
            } else {
                final String language = literal.getLanguage();
                if (language != null) {
                    builder.append('@').append(language);
                }
            }
        }
    }

    private Data() {
    }

    private static final class TotalOrdering extends Ordering<Object> {

        private static final int DT_BOOLEAN = 1;

        private static final int DT_STRING = 2;

        private static final int DT_LONG = 3;

        private static final int DT_DOUBLE = 4;

        private static final int DT_DECIMAL = 5;

        private static final int DT_CALENDAR = 6;

        @Override
        public int compare(final Object first, final Object second) {
            if (first == null) {
                return second == null ? 0 : -1;
            } else if (second == null) {
                return 1;
            } else if (first instanceof URI) {
                return compareURI((URI) first, second);
            } else if (first instanceof BNode) {
                return compareBNode((BNode) first, second);
            } else if (first instanceof Record) {
                return compareRecord((Record) first, second);
            } else if (first instanceof Statement) {
                return compareStatement((Statement) first, second);
            } else if (first instanceof Literal) {
                return compareLiteral((Literal) first, second);
            } else {
                return compareLiteral(convert(first, Literal.class), second);
            }
        }

        private int compareStatement(final Statement first, final Object second) {
            if (second instanceof Statement) {
                final Statement secondStmt = (Statement) second;
                int result = compare(first.getSubject(), secondStmt.getSubject());
                if (result != 0) {
                    return result;
                }
                result = compare(first.getPredicate(), secondStmt.getPredicate());
                if (result != 0) {
                    return result;
                }
                result = compare(first.getObject(), secondStmt.getObject());
                if (result != 0) {
                    return result;
                }
                result = compare(first.getContext(), secondStmt.getContext());
                if (result != 0) {
                    return result;
                }
            } else if (second instanceof Value || second instanceof Record) {
                return Integer.MIN_VALUE;
            }
            return Integer.MAX_VALUE;
        }

        private int compareLiteral(final Literal first, final Object second) {
            if (second instanceof Resource || second instanceof Record) {
                return Integer.MIN_VALUE;
            } else if (second instanceof Statement) {
                return Integer.MAX_VALUE;
            }
            final Literal secondLit = second instanceof Literal ? (Literal) second : convert(
                    second, Literal.class);
            final int firstGroup = classifyDatatype(first.getDatatype());
            final int secondGroup = classifyDatatype(secondLit.getDatatype());
            switch (firstGroup) {
            case DT_BOOLEAN:
                if (secondGroup == DT_BOOLEAN) {
                    return Booleans.compare(first.booleanValue(), secondLit.booleanValue());
                }
                break;
            case DT_STRING:
                if (secondGroup == DT_STRING) {
                    final int result = first.getLabel().compareTo(secondLit.getLabel());
                    if (result != 0) {
                        return result;
                    }
                    final String firstLang = first.getLanguage();
                    final String secondLang = secondLit.getLanguage();
                    if (firstLang == null) {
                        return secondLang == null ? 0 : -1;
                    } else {
                        return secondLang == null ? 1 : firstLang.compareTo(secondLang);
                    }
                }
                break;
            case DT_LONG:
                if (secondGroup == DT_LONG) {
                    return Longs.compare(first.longValue(), secondLit.longValue());
                } else if (secondGroup == DT_DOUBLE) {
                    return Doubles.compare(first.doubleValue(), secondLit.doubleValue());
                } else if (secondGroup == DT_DECIMAL) {
                    return first.decimalValue().compareTo(secondLit.decimalValue());
                }
                break;
            case DT_DOUBLE:
                if (secondGroup == DT_LONG //
                        || secondGroup == DT_DOUBLE) {
                    return Doubles.compare(first.doubleValue(), secondLit.doubleValue());
                } else if (secondGroup == DT_DECIMAL) {
                    return first.decimalValue().compareTo(secondLit.decimalValue());
                }
                break;
            case DT_DECIMAL:
                if (secondGroup == DT_LONG || secondGroup == DT_DOUBLE
                        || secondGroup == DT_DECIMAL) {
                    return first.decimalValue().compareTo(secondLit.decimalValue());
                }
                break;
            case DT_CALENDAR:
                if (secondGroup == DT_CALENDAR) {
                    final int result = first.calendarValue().compare(secondLit.calendarValue());
                    return result == DatatypeConstants.INDETERMINATE ? 0 : result;
                }
                break;
            default:
            }
            return firstGroup < secondGroup ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }

        private int compareURI(final URI first, final Object second) {
            if (second instanceof URI) {
                return first.stringValue().compareTo(((URI) second).stringValue());
            } else if (second instanceof BNode) {
                return 1;
            } else if (second instanceof Record) {
                return -1;
            }
            return Integer.MAX_VALUE;
        }

        private int compareBNode(final BNode first, final Object second) {
            if (second instanceof BNode) {
                return first.getID().compareTo(((BNode) second).getID());
            } else if (second instanceof URI || second instanceof Record) {
                return -1;
            }
            return Integer.MAX_VALUE;
        }

        private int compareRecord(final Record first, final Object second) {
            if (second instanceof Record) {
                return first.compareTo((Record) second);
            } else if (second instanceof URI || second instanceof BNode) {
                return 1;
            }
            return Integer.MAX_VALUE;
        }

        private static int classifyDatatype(final URI datatype) {
            if (datatype == null || datatype.equals(XMLSchema.STRING)) {
                return DT_STRING;
            } else if (datatype.equals(XMLSchema.BOOLEAN)) {
                return DT_BOOLEAN;
            } else if (datatype.equals(XMLSchema.INT) || datatype.equals(XMLSchema.LONG)
                    || datatype.equals(XMLSchema.SHORT) || datatype.equals(XMLSchema.BYTE)) {
                return DT_LONG;
            } else if (datatype.equals(XMLSchema.DOUBLE) || datatype.equals(XMLSchema.FLOAT)) {
                return DT_DOUBLE;
            } else if (datatype.equals(XMLSchema.DATE) || datatype.equals(XMLSchema.DATETIME)) {
                return DT_CALENDAR;
            } else if (datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.INTEGER)
                    || datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                    || datatype.equals(XMLSchema.POSITIVE_INTEGER)
                    || datatype.equals(XMLSchema.NEGATIVE_INTEGER)) {
                return DT_DECIMAL;
            } else if (datatype.equals(XMLSchema.NORMALIZEDSTRING)
                    || datatype.equals(XMLSchema.TOKEN) || datatype.equals(XMLSchema.NMTOKEN)
                    || datatype.equals(XMLSchema.LANGUAGE) || datatype.equals(XMLSchema.NAME)
                    || datatype.equals(XMLSchema.NCNAME)) {
                return DT_STRING;
            }
            throw new IllegalArgumentException("Comparison unsupported for literal datatype "
                    + datatype);
        }

    }

    private static final class PartialOrdering extends Ordering<Object> {

        private final Comparator<Object> totalComparator;

        PartialOrdering(final Comparator<Object> totalComparator) {
            this.totalComparator = Preconditions.checkNotNull(totalComparator);
        }

        @Override
        public int compare(final Object first, final Object second) {
            final int result = this.totalComparator.compare(first, second);
            if (result == Integer.MIN_VALUE || result == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Incomparable values: " + first + ", " + second);
            }
            return result;
        }

    }

    private static final class NamespaceMap extends AbstractMap<String, String> {

        private final Map<String, String> namespaces;

        private final Map<String, String> prefixes;

        private final EntrySet entries;

        NamespaceMap() {
            this.namespaces = Maps.newHashMap();
            this.prefixes = Maps.newHashMap();
            this.entries = new EntrySet();
        }

        @Override
        public int size() {
            return this.namespaces.size();
        }

        @Override
        public boolean isEmpty() {
            return this.namespaces.isEmpty();
        }

        @Override
        public boolean containsKey(final Object prefix) {
            return this.namespaces.containsKey(prefix);
        }

        @Override
        public boolean containsValue(final Object namespace) {
            return this.prefixes.containsKey(namespace);
        }

        @Override
        public String get(final Object prefix) {
            return this.namespaces.get(prefix);
        }

        public String getPrefix(final Object namespace) {
            return this.prefixes.get(namespace);
        }

        @Override
        public String put(final String prefix, final String namespace) {
            this.prefixes.put(namespace, prefix);
            return this.namespaces.put(prefix, namespace);
        }

        @Override
        public String remove(final Object prefix) {
            final String namespace = super.remove(prefix);
            removeInverse(namespace, prefix);
            return namespace;
        }

        private void removeInverse(@Nullable final String namespace, final Object prefix) {
            if (namespace == null) {
                return;
            }
            final String inversePrefix = this.prefixes.remove(namespace);
            if (!prefix.equals(inversePrefix)) {
                this.prefixes.put(namespace, inversePrefix);
            } else if (this.prefixes.size() != this.namespaces.size()) {
                for (final Map.Entry<String, String> entry : this.namespaces.entrySet()) {
                    if (entry.getValue().equals(namespace)) {
                        this.prefixes.put(entry.getValue(), entry.getKey());
                        break;
                    }
                }
            }
        }

        @Override
        public void clear() {
            this.namespaces.clear();
            this.prefixes.clear();
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            return this.entries;
        }

        final class EntrySet extends AbstractSet<Map.Entry<String, String>> {

            @Override
            public int size() {
                return NamespaceMap.this.namespaces.size();
            }

            @Override
            public Iterator<Entry<String, String>> iterator() {
                final Iterator<Entry<String, String>> iterator = NamespaceMap.this.namespaces
                        .entrySet().iterator();
                return new Iterator<Entry<String, String>>() {

                    private Entry<String, String> last;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, String> next() {
                        this.last = new EntryWrapper(iterator.next());
                        return this.last;
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                        removeInverse(this.last.getValue(), this.last.getKey());
                    }

                };
            }

            private class EntryWrapper implements Entry<String, String> {

                private final Entry<String, String> entry;

                EntryWrapper(final Entry<String, String> entry) {
                    this.entry = entry;
                }

                @Override
                public String getKey() {
                    return this.entry.getKey();
                }

                @Override
                public String getValue() {
                    return this.entry.getValue();
                }

                @Override
                public String setValue(final String namespace) {
                    final String oldNamespace = this.entry.getValue();
                    if (!Objects.equal(oldNamespace, namespace)) {
                        final String prefix = this.entry.getKey();
                        removeInverse(oldNamespace, prefix);
                        this.entry.setValue(namespace);
                        NamespaceMap.this.prefixes.put(namespace, prefix);
                    }
                    return oldNamespace;
                }

                @Override
                public boolean equals(final Object object) {
                    return this.entry.equals(object);
                }

                @Override
                public int hashCode() {
                    return this.entry.hashCode();
                }

                @Override
                public String toString() {
                    return this.entry.toString();
                }

            }
        }

    }

    private static final class NamespaceCombinedMap extends AbstractMap<String, String> {

        final Map<String, String> primaryNamespaces;

        final Map<String, String> secondaryNamespaces;

        NamespaceCombinedMap(final Map<String, String> primaryNamespaces,
                final Map<String, String> secondaryNamespaces) {

            this.primaryNamespaces = primaryNamespaces;
            this.secondaryNamespaces = secondaryNamespaces;
        }

        @Override
        public String get(final Object prefix) {
            String uri = this.primaryNamespaces.get(prefix);
            if (uri == null) {
                uri = this.secondaryNamespaces.get(prefix);
            }
            return uri;
        }

        @Override
        public String put(final String prefix, final String uri) {
            return this.primaryNamespaces.put(prefix, uri);
        }

        @Override
        public Set<Map.Entry<String, String>> entrySet() {
            return new EntrySet();
        }

        @Override
        public void clear() {
            this.primaryNamespaces.clear();
        }

        final class EntrySet extends AbstractSet<Map.Entry<String, String>> {

            @Override
            public int size() {
                return Sets.union(NamespaceCombinedMap.this.primaryNamespaces.keySet(), //
                        NamespaceCombinedMap.this.secondaryNamespaces.keySet()).size();
            }

            @Override
            public Iterator<Entry<String, String>> iterator() {

                final Set<String> additionalKeys = Sets.difference(
                        NamespaceCombinedMap.this.secondaryNamespaces.keySet(),
                        NamespaceCombinedMap.this.primaryNamespaces.keySet());

                Function<String, Entry<String, String>> transformer;
                transformer = new Function<String, Entry<String, String>>() {

                    @Override
                    public Entry<String, String> apply(final String prefix) {
                        return new AbstractMap.SimpleImmutableEntry<String, String>(prefix,
                                NamespaceCombinedMap.this.secondaryNamespaces.get(prefix));
                    }

                };

                return Iterators.concat(NamespaceCombinedMap.this.primaryNamespaces.entrySet()
                        .iterator(), ignoreRemove(Iterators.transform(additionalKeys.iterator(),
                        transformer)));
            }

            private <T> Iterator<T> ignoreRemove(final Iterator<T> iterator) {
                return new Iterator<T>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public T next() {
                        return iterator.next();
                    }

                    @Override
                    public void remove() {
                    }

                };
            }

        }

    }

}
