package eu.fbk.knowledgestore.tool;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

final class TestUtil {

    // Properties manipulation

    public static <T> T read(final Properties properties, final String name, final Class<T> type) {
        final T value = read(properties, name, type, null);
        if (value == null) {
            throw new IllegalArgumentException("No value for mandatory property '" + name + "'");
        }
        return value;
    }

    public static <T> T read(final Properties properties, final String name, final Class<T> type,
            final T defaultValue) {
        final String value = properties.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Data.convert(value, type);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Invalid value for property '" + name + "': "
                    + value + " (" + ex.getMessage() + ")");
        }
    }

    public static void expand(final Properties targetProperties, final Properties defaultProperties) {
        for (final Map.Entry<Object, Object> entry : defaultProperties.entrySet()) {
            final String name = (String) entry.getKey();
            if (!targetProperties.containsKey(name)) {
                final String value = (String) entry.getValue();
                targetProperties.setProperty(name, value);
            }
        }
    }

    public static Map<String, Properties> split(final Properties properties) {
        final Map<String, Properties> map = Maps.newLinkedHashMap();
        for (final Object key : properties.keySet()) {
            final String keyString = key.toString();
            final int index = keyString.indexOf(".");
            if (index > 0) {
                final String name = keyString.substring(0, index);
                final String property = keyString.substring(index + 1);
                final String value = properties.getProperty(keyString);
                Properties subProperties = map.get(name);
                if (subProperties == null) {
                    subProperties = new Properties();
                    map.put(name, subProperties);
                }
                subProperties.setProperty(property, value);
            }
        }
        return map;
    }

    // TSV and RDF manipulation

    public static BindingSet decode(final List<String> variables, final String line) {
        final String[] tokens = line.split("\t");
        Preconditions.checkArgument(tokens.length == variables.size(), "Wrong number of values ("
                + tokens.length + " found, " + variables.size() + " expected) in line: " + line);
        try {
            final MapBindingSet bindings = new MapBindingSet();
            for (int i = 0; i < tokens.length; ++i) {
                String token = tokens[i];
                if (!Strings.isNullOrEmpty(token)) {
                    final char ch = token.charAt(0);
                    token = ch == '\'' || ch == '"' || ch == '<' || ch == '_' ? token : "\""
                            + token + "\"";
                    final Value value = Statements.parseValue(token, Namespaces.DEFAULT);
                    bindings.addBinding(variables.get(i), value);
                }
            }
            return bindings;
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Could not parse variable values.\nVariables: "
                    + variables + "\nLine: " + line, ex);
        }
    }

    public static String encode(final List<String> variables, final BindingSet bindings) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < variables.size(); ++i) {
            if (i > 0) {
                builder.append('\t');
            }
            final Value value = bindings.getValue(variables.get(i));
            builder.append(format(value));
        }
        return builder.toString();
    }

    public static String format(final Iterable<String> variables, final BindingSet bindings,
            final String separator) {
        final StringBuilder builder = new StringBuilder();
        for (final String variable : variables) {
            final Value value = bindings.getValue(variable);
            if (value != null) {
                builder.append(builder.length() == 0 ? "" : separator);
                builder.append(variable);
                builder.append('=');
                builder.append(format(value));
            }
        }
        return builder.toString();
    }

    public static String format(final Value value) {
        // Emit literal without lang / datatype for easier consumption in analysis tools
        if (value instanceof Value) { // was instanceof Resource
            return Statements.formatValue(value, null);
        } else if (value != null) {
            return value.stringValue();
        } else {
            return "";
        }
    }

}
