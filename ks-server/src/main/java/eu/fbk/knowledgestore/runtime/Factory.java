package eu.fbk.knowledgestore.runtime;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.thoughtworks.paranamer.AdaptiveParanamer;
import com.thoughtworks.paranamer.BytecodeReadingParanamer;
import com.thoughtworks.paranamer.CachingParanamer;
import com.thoughtworks.paranamer.DefaultParanamer;
import com.thoughtworks.paranamer.Paranamer;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;

public final class Factory {

    private static final String SCHEME = "java:";

    private static final Paranamer PARANAMER = new CachingParanamer(new AdaptiveParanamer(
            new DefaultParanamer(), new BytecodeReadingParanamer()));

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{[^\\}]+\\}");

    public static Map<URI, Object> instantiate(final Iterable<? extends Statement> model,
            final URI... ids) {

        final Map<Resource, URI> types = Maps.newLinkedHashMap();
        final Multimap<Resource, Statement> stmt = ArrayListMultimap.create();
        for (final Statement statement : model) {
            final Resource s = statement.getSubject();
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();
            if (p.equals(RDF.TYPE) && o instanceof URI && o.stringValue().startsWith(SCHEME)) {
                types.put(s, (URI) o);
            } else if (p.stringValue().startsWith(SCHEME)) {
                stmt.put(s, statement);
            }
        }

        if (ids != null && ids.length > 0) {
            final Set<Resource> subjs = Sets.<Resource>newHashSet(ids);
            int size;
            do {
                size = subjs.size();
                for (final Statement statement : stmt.values()) {
                    final Value o = statement.getObject();
                    if (o instanceof Resource) {
                        subjs.add((Resource) o);
                    }
                }
            } while (subjs.size() > size);
            types.keySet().retainAll(subjs);
        }

        final Map<Resource, Object> map = Maps.newHashMap();
        while (!types.isEmpty()) {
            final int size = types.size();
            for (final Resource s : Lists.newArrayList(types.keySet())) {
                final Collection<Statement> statements = stmt.get(s);
                boolean dependent = false;
                for (final Statement statement : statements) {
                    final Value o = statement.getObject();
                    if (o instanceof Resource && types.keySet().contains(o)) {
                        dependent = true;
                        break;
                    }
                }
                if (!dependent) {
                    final URI implementation = types.get(s);
                    final Multimap<String, Object> properties = ArrayListMultimap.create();
                    for (final Statement statement : statements) {
                        final URI p = statement.getPredicate();
                        final Value o = statement.getObject();
                        final Object obj = map.get(o);
                        properties.put(p.stringValue().substring(SCHEME.length()),
                                obj != null ? obj : o);
                    }
                    Preconditions.checkArgument(implementation != null,
                            "No implementation specified for %s", s);
                    map.put(s, instantiate(properties.asMap(), implementation, Object.class));
                    types.remove(s);
                }
            }
            Preconditions.checkArgument(types.size() < size, "Cannot instantiate " + stmt.keySet()
                    + " - detected circular dependencies");
        }

        final ImmutableMap.Builder<URI, Object> builder = ImmutableMap.builder();
        for (final Map.Entry<Resource, Object> entry : map.entrySet()) {
            final Resource s = entry.getKey();
            final Object obj = entry.getValue();
            if (s instanceof URI
                    && (ids == null || ids.length == 0 || Arrays.asList(ids).contains(s))) {
                builder.put((URI) s, obj);
            }
        }
        return builder.build();
    }

    public static <T> T instantiate(final Iterable<? extends Statement> model, final URI id,
            final Class<T> type) {
        return type.cast(instantiate(model, id).get(id));
    }

    public static <T> T instantiate(final Map<String, ? extends Object> properties,
            final URI implementation, final Class<T> type) {

        final String uriString = implementation.stringValue();
        Preconditions.checkArgument(uriString.startsWith("java:"));

        final int index = uriString.indexOf("#");
        final String className = uriString.substring(5, index > 0 ? index : uriString.length());
        final String methodName = index < 0 ? null : uriString.substring(index + 1);

        final Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (final ClassNotFoundException ex) {
            throw new IllegalArgumentException("No class for " + implementation);
        }

        // Transform input property names to lower case
        final Map<String, Object> props = Maps.newLinkedHashMap();
        for (final Map.Entry<String, ? extends Object> entry : properties.entrySet()) {
            props.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        if (clazz == Record.class) {
            Preconditions.checkArgument(type.isAssignableFrom(Record.class));
            final Record record = Record.create();
            for (final Map.Entry<String, ? extends Object> entry : properties.entrySet()) {
                record.set(Data.getValueFactory().createURI("java:" + entry.getKey()),
                        entry.getValue());
            }
            return type.cast(record);
        }

        final Map<String, Method> setters = Maps.newHashMap();
        for (final Method m : clazz.getMethods()) {
            final String name = m.getName();
            if (!Modifier.isStatic(m.getModifiers()) && m.getParameterTypes().length == 1
                    && name.startsWith("set")) {
                setters.put(name.substring(3).toLowerCase(), m);
            }
        }

        final Set<String> constructionProps = Sets.newHashSet(props.keySet());
        constructionProps.removeAll(setters.keySet());

        if (methodName == null) {
            for (final Constructor<?> c : clazz.getConstructors()) {
                final Set<String> args = Sets.newHashSet(signature(c));
                boolean acceptMap = false;
                for (int i = 0; i < c.getParameterTypes().length; ++i) {
                    acceptMap = acceptMap || c.getParameterTypes()[i].isAssignableFrom(Map.class)
                            || c.getParameterTypes()[i].isAssignableFrom(Properties.class);
                }
                if (args.containsAll(constructionProps) || acceptMap) {
                    return type.cast(callSetters(callConstructor(c, props), setters, props));
                }
            }
            throw new IllegalArgumentException("No suitable constructor for " + implementation
                    + " supporting properties " + Joiner.on(", ").join(props.keySet()));
        }

        for (final Method m : clazz.getMethods()) {
            if (!m.getName().equals(methodName) || !Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            final Set<String> args = Sets.newHashSet(signature(m));
            if (methodName.equals("builder")) {
                final Class<?> builderClazz = m.getReturnType();
                Method build = null;
                final Map<String, Method> builderSetters = Maps.newHashMap();
                for (final Method setter : builderClazz.getMethods()) {
                    String name = setter.getName();
                    if (name.equals("build")) {
                        build = setter;
                    }
                    if (!Modifier.isStatic(setter.getModifiers())
                            && setter.getReturnType() == builderClazz
                            && setter.getParameterTypes().length == 1) {
                        if (name.startsWith("set")) {
                            name = name.substring(3);
                        } else if (name.startsWith("with")) {
                            name = name.substring(4);
                        }
                        builderSetters.put(name.toLowerCase(), setter);
                    }
                }
                args.addAll(builderSetters.keySet());
                args.addAll(Arrays.asList(signature(build)));
                if (build != null && args.containsAll(props.keySet())) {
                    return type.cast(callSetters(callBuilder(m, builderSetters, build, props),
                            setters, props));
                }
            } else if (args.containsAll(props.keySet())) {
                return type.cast(callSetters(callMethod(m, null, props), setters, props));
            }
        }

        throw new IllegalArgumentException("No suitable '" + methodName + "' method for "
                + implementation + " supporting properties "
                + Joiner.on(", ").join(props.keySet()));
    }

    private static String[] signature(final AccessibleObject member) {
        String[] names = PARANAMER.lookupParameterNames(member, false);
        if (names != null) {
            names = names.clone();
            for (int i = 0; i < names.length; ++i) {
                names[i] = names[i].trim().toLowerCase();
            }
        } else if (member instanceof Constructor) {
            names = new String[((Constructor<?>) member).getParameterTypes().length];
        } else if (member instanceof Method) {
            names = new String[((Method) member).getParameterTypes().length];
        } else {
            throw new Error("Unexpected member " + member);
        }
        return names;
    }

    private static Object callConstructor(final Constructor<?> constructor,
            final Map<String, Object> properties) {

        // Prepare the argument list
        final Type[] argTypes = constructor.getGenericParameterTypes();
        final String[] argNames = signature(constructor);
        final Object[] argValues = convertArgs(properties, argNames, argTypes);

        try {
            // Invoke the constructor, reporting detailed error information on failure
            return constructor.newInstance(argValues);
        } catch (final Throwable ex) {
            throw new RuntimeException("Invocation of constructor '" + constructor
                    + "' with parameters " + Arrays.asList(argValues) + " failed", ex);
        }
    }

    private static Object callMethod(final Method method, final Object object,
            final Map<String, Object> properties) {

        // Prepare the argument list
        final Type[] argTypes = method.getGenericParameterTypes();
        final String[] argNames = signature(method);
        final Object[] argValues = convertArgs(properties, argNames, argTypes);

        try {
            // Invoke the method, reporting detailed error information on failure
            return method.invoke(object, argValues);
        } catch (final Throwable ex) {
            throw new RuntimeException("Invocation of method '" + method + "' on object " + object
                    + " with parameters " + Arrays.asList(argValues) + " failed", ex);
        }
    }

    private static Object callBuilder(final Method builder, final Map<String, Method> setters,
            final Method build, final Map<String, Object> properties) {

        // Instantiate the builder object
        final Object obj = callMethod(builder, null, properties);

        // Set properties on the builder
        callSetters(obj, setters, properties);

        // Instantiate the object
        return callMethod(build, obj, properties);
    }

    private static Object callSetters(final Object object, final Map<String, Method> setters,
            final Map<String, Object> properties) {

        for (final Map.Entry<String, Method> entry : setters.entrySet()) {
            final String name = entry.getKey();
            final Method setter = entry.getValue();
            final Object value = properties.get(name);
            if (value != null || properties.containsKey(name)) {
                callMethod(setter, object, ImmutableMap.of(name, value));
            }
        }

        return object; // for call chaining
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Object[] convertArgs(final Map<String, Object> properties,
            final String[] names, final Type[] types) {
        assert names.length == types.length;
        final int length = names.length;
        final Object[] result = new Object[length];
        for (int i = 0; i < length; ++i) {
            final String name = names[i];
            final Type type = types[i];
            final TypeToken token = TypeToken.of(type);
            if (token.getRawType().isAssignableFrom(Map.class)) {
                final Type valueType = ((ParameterizedType) token.getSupertype(Map.class)
                        .getType()).getActualTypeArguments()[1];
                final Map<String, Object> map = Maps.newHashMapWithExpectedSize(properties.size());
                for (final Map.Entry<String, Object> entry : properties.entrySet()) {
                    map.put(entry.getKey(), convertMany(entry.getValue(), valueType));
                }
                result[i] = map;
            } else if (token.getRawType().isAssignableFrom(Properties.class)) {
                final Properties props = new Properties();
                for (final Map.Entry<String, Object> entry : properties.entrySet()) {
                    try {
                        props.setProperty(entry.getKey(),
                                (String) convertMany(entry.getValue(), String.class));
                    } catch (final Throwable ex) {
                        // ignore conversion error
                    }
                }
                result[i] = props;
            } else {
                result[i] = convertMany(properties.get(name), type);
            }
        }
        return result;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Object convertMany(@Nullable final Object value, final Type type) {

        final TypeToken token = TypeToken.of(type);
        final Class<?> clazz = token.getRawType();

        Type elementType = type;
        if (Iterable.class.isAssignableFrom(clazz)) {
            elementType = ((ParameterizedType) token.getSupertype(Iterable.class).getType())
                    .getActualTypeArguments()[0];
        } else if (clazz.isArray()) {
            elementType = token.getComponentType().getType();
        }
        final Class<?> elementClass = TypeToken.of(elementType).getRawType();

        final List<Object> values = Lists.newArrayList();
        if (value != null) {
            if (value instanceof Iterable<?>) {
                for (final Object v : (Iterable<?>) value) {
                    values.add(convertSingle(expand(v), elementClass));
                }
            } else if (value.getClass().isArray()) {
                final int length = Array.getLength(value);
                for (int i = 0; i < length; ++i) {
                    values.add(convertSingle(expand(Array.get(value, i)), elementClass));
                }
            } else {
                values.add(convertSingle(expand(value), elementClass));
            }
        }

        if (clazz.isAssignableFrom(List.class)) {
            return values;
        } else if (clazz.isAssignableFrom(Set.class)) {
            return Sets.newHashSet(values);
        } else if (clazz.isArray()) {
            return Iterables.toArray(values, (Class<Object>) clazz.getComponentType());
        } else if (!values.isEmpty()) {
            return values.get(0);
        } else if (clazz == long.class) {
            return 0L;
        } else if (clazz == int.class) {
            return 0;
        } else if (clazz == short.class) {
            return (short) 0;
        } else if (elementClass == byte.class) {
            return (byte) 0;
        } else if (elementClass == char.class) {
            return (char) 0;
        } else if (elementClass == boolean.class) {
            return false;
        }
        return null;
    }

    private static Object convertSingle(final Object object, final Class<?> type) {
        if (type == XPath.class) {
            return object instanceof XPath || object == null ? (XPath) object : //
                    XPath.parse((String) convertSingle(object, String.class));
        } else {
            return Data.convert(object, type);
        }
    }

    private static Object expand(final Object object) {

        String string;
        if (object instanceof String) {
            string = (String) object;
        } else if (object instanceof Literal) {
            string = ((Literal) object).getLabel();
        } else {
            return object;
        }

        final StringBuilder builder = new StringBuilder();
        int index = 0;
        final Matcher matcher = PLACEHOLDER.matcher(string);
        while (matcher.find()) {
            builder.append(string.substring(index, matcher.start()));
            final String property = string.substring(matcher.start() + 2, matcher.end() - 1);
            String value = System.getProperty(property);
            if (value != null) {
                if (property.equals("user.dir") || property.equals("user.home")
                        || property.equals("java.home") || property.equals("java.io.tmpdir")
                        || property.equals("java.ext.dirs")) {
                    value = value.replace('\\', '/');
                }
                builder.append(value);
            }
            index = matcher.end();
        }
        builder.append(string.substring(index));
        final String expanded = builder.toString();

        if (object instanceof String) {
            return expanded;
        } else {
            final Literal l = (Literal) object;
            final URI dt = l.getDatatype();
            final String lang = l.getLanguage();
            return dt != null ? Data.getValueFactory().createLiteral(expanded, dt) //
                    : Data.getValueFactory().createLiteral(expanded, lang);
        }
    }

}
