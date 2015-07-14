package eu.fbk.knowledgestore.internal.rdf;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

public abstract class CompactBindingSet implements BindingSet, Serializable {

    private static final long serialVersionUID = 1L;

    private final VariableList variables;

    @Nullable
    private Set<String> names;

    private int hash;

    CompactBindingSet(final VariableList variableNames) {
        this.variables = variableNames;
        this.names = null;
        this.hash = 0;
    }

    abstract Value get(int index);

    @Override
    public final int size() {
        return getBindingNames().size();
    }

    @Override
    public final Iterator<Binding> iterator() {
        return new BindingIterator(this);
    }

    @Override
    public final Set<String> getBindingNames() {
        if (this.names == null) {
            int count = 0;
            final int size = this.variables.size();
            for (int i = 0; i < size; ++i) {
                if (get(i) != null) {
                    ++count;
                }
            }
            final String[] array = new String[count];
            int index = 0;
            for (int i = 0; i < size; ++i) {
                if (get(i) != null) {
                    array[index++] = this.variables.get(i);
                }
            }
            this.names = new VariableSet(array);
        }
        return this.names;
    }

    @Override
    public final boolean hasBinding(final String name) {
        return get(this.variables.indexOf(name)) != null;
    }

    @Override
    @Nullable
    public final Binding getBinding(final String name) {
        final Value value = get(this.variables.indexOf(name));
        return value == null ? null : new BindingImpl(name, value);
    }

    @Override
    @Nullable
    public final Value getValue(final String name) {
        return get(this.variables.indexOf(name));
    }

    @Override
    public final boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof BindingSet)) {
            return false;
        }
        final BindingSet other = (BindingSet) object;
        final int thisSize = this.variables.size();
        if (other instanceof CompactBindingSet
                && ((CompactBindingSet) other).variables == this.variables) {
            final CompactBindingSet bs = (CompactBindingSet) other;
            for (int i = 0; i < thisSize; ++i) {
                if (!Objects.equal(get(i), bs.get(i))) {
                    return false;
                }
            }
        } else {
            final Set<String> thisNames = getBindingNames();
            final Set<String> otherNames = other.getBindingNames();
            if (thisNames.size() != otherNames.size()) {
                return false;
            }
            final int size = this.variables.size();
            for (int i = 0; i < size; ++i) {
                if (!Objects.equal(get(i), other.getValue(this.variables.get(i)))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int hashCode() {
        if (this.hash == 0) {
            int hash = 0;
            final int size = this.variables.size();
            for (int i = 0; i < size; ++i) {
                final Value value = get(i);
                if (value != null) {
                    hash ^= this.variables.get(i).hashCode() ^ value.hashCode();
                }
            }
            this.hash = hash;
        }
        return this.hash;
    }

    /**
     * {@inheritDoc} The returned string follows the format {@code [name=value;name=value...]} (as
     * generally done in implementations of {@code BindingSet}).
     */
    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder(32 * size());
        builder.append('[');
        String separator = "";
        final int size = this.variables.size();
        for (int i = 0; i < size; ++i) {
            final Value value = get(i);
            if (value != null) {
                final String variable = this.variables.get(i);
                builder.append(separator);
                builder.append(variable);
                builder.append("=");
                builder.append(value);
                separator = ";";
            }
        }
        builder.append(']');
        return builder.toString();
    }

    public static Builder builder(final Iterable<? extends String> variables) {
        return new Builder(variables instanceof VariableList ? (VariableList) variables
                : new VariableList(variables));
    }

    public static final class Builder {

        private final VariableList variables;

        private Value[] values;

        Builder(final VariableList variables) {
            this.variables = variables;
            this.values = new Value[variables.size()];
        }

        public Builder set(final int index, @Nullable final Value value)
                throws IndexOutOfBoundsException {
            checkIndex(this.variables, index);
            this.values[index] = CompactValueFactory.getInstance().normalize(value);
            return this;
        }

        public Builder set(final String variable, @Nullable final Value value)
                throws NoSuchElementException {
            final int index = indexOfVariable(this.variables, variable);
            this.values[index] = CompactValueFactory.getInstance().normalize(value);
            return this;
        }

        public Builder setAll(final Value... values) throws IllegalArgumentException {
            final int size = values.length;
            checkSize(this.variables, size);
            for (int i = 0; i < values.length; ++i) {
                this.values[i] = CompactValueFactory.getInstance().normalize(values[i]);
            }
            return this;
        }

        public Builder setAll(final Iterable<? extends Value> values)
                throws IllegalArgumentException {
            final int size = Iterables.size(values);
            checkSize(this.variables, size);
            int index = 0;
            for (final Value value : values) {
                this.values[index++] = CompactValueFactory.getInstance().normalize(value);
            }
            return this;
        }

        public Builder setAll(final Map<? extends Object, ? extends Value> values)
                throws NoSuchElementException, IndexOutOfBoundsException {
            Arrays.fill(this.values, null);
            for (final Map.Entry<? extends Object, ? extends Value> entry : values.entrySet()) {
                final Object key = entry.getKey();
                final Value value = entry.getValue();
                if (key instanceof Number) {
                    set(((Number) key).intValue(), value);
                } else {
                    set(key.toString(), value);
                }
            }
            return this;
        }

        public Builder setAll(final BindingSet bindings) {
            Arrays.fill(this.values, null);
            for (final String name : bindings.getBindingNames()) {
                set(name, bindings.getValue(name));
            }
            return this;
        }

        public CompactBindingSet build() {
            final int size = this.values.length;
            int singletonIndex = -1;
            Value singletonValue = null;
            for (int i = 0; i < size; ++i) {
                final Value value = this.values[i];
                if (value != null) {
                    if (singletonIndex == -1) {
                        singletonIndex = i;
                        singletonValue = value;
                    } else {
                        final CompactBindingSet solution = new ArrayCompactBindingSet(
                                this.variables, this.values);
                        this.values = new Value[size];
                        return solution;
                    }
                }
            }
            if (singletonIndex == -1) {
                return new EmptyCompactBindingSet(this.variables);
            }
            this.values[singletonIndex] = null;
            return new SingletonCompactBindingSet(this.variables, singletonIndex, singletonValue);
        }

        private static void checkIndex(final List<String> variables, final int index) {
            if (index < 0 || index >= variables.size()) {
                throw new IndexOutOfBoundsException("Invalid variable index " + index
                        + " (variables are: " + Joiner.on(", ").join(variables) + ")");
            }
        }

        private static void checkSize(final List<String> variables, final int size) {
            if (size != variables.size()) {
                throw new IllegalArgumentException("Expected " + variables.size()
                        + " values, got " + size + " (variables are: "
                        + Joiner.on(", ").join(variables) + ")");
            }
        }

        private static int indexOfVariable(final List<String> variables, final String variable) {
            final int index = variables.indexOf(variable);
            if (index < 0) {
                throw new NoSuchElementException("Unknown variable '" + variable
                        + "' (variables are: " + Joiner.on(", ").join(variables) + ")");
            }
            return index;
        }

    }

    private static final class BindingIterator extends UnmodifiableIterator<Binding> {

        private final CompactBindingSet bindings;

        private Binding next;

        private int index;

        BindingIterator(final CompactBindingSet bindings) {
            this.bindings = bindings;
            this.next = null;
            this.index = 0;
            advance();
        }

        private void advance() {
            final int size = this.bindings.variables.size();
            while (this.index < size) {
                final int index = this.index++;
                final Value value = this.bindings.get(index);
                if (value != null) {
                    final String variable = this.bindings.variables.get(index);
                    this.next = new BindingImpl(variable, value);
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Binding next() {
            final Binding result = this.next;
            if (result == null) {
                throw new NoSuchElementException();
            }
            this.next = null;
            advance();
            return result;
        }

    }

    private static class VariableSet extends AbstractSet<String> {

        private final String[] variables;

        VariableSet(final String... variables) {
            this.variables = variables;
        }

        @Override
        public Iterator<String> iterator() {
            return Iterators.forArray(this.variables);
        }

        @Override
        public int size() {
            return this.variables.length;
        }

        @Override
        public boolean contains(final Object object) {
            if (object instanceof String) {
                for (int i = 0; i < this.variables.length; ++i) {
                    if (this.variables[i].equals(object)) {
                        return true;
                    }
                }
            }
            return false;
        }

    }

    private static final class VariableList extends AbstractList<String> {

        private final String[] variables;

        private final String[] variableTable;

        private final int[] indexTable;

        public VariableList(final Iterable<? extends String> variables) {
            final int size = Iterables.size(variables);
            final int tableSize = size * 4 - 1;
            this.variables = new String[size];
            this.variableTable = new String[tableSize];
            this.indexTable = new int[tableSize];
            int index = 0;
            for (final String variable : variables) {
                int tableIndex = (variable.hashCode() & 0x7FFFFFFF) % tableSize;
                while (this.variableTable[tableIndex] != null) {
                    tableIndex = (tableIndex + 1) % tableSize;
                }
                this.variables[index] = variable;
                this.variableTable[tableIndex] = variable;
                this.indexTable[tableIndex] = index;
                ++index;
            }
        }

        @Override
        public int size() {
            return this.variables.length;
        }

        @Override
        public String get(final int index) {
            return this.variables[index];
        }

        @Override
        public boolean contains(final Object object) {
            return indexOf(object) != -1;
        }

        @Override
        public int indexOf(final Object object) {
            final int tableSize = this.variableTable.length;
            int tableIndex = (object.hashCode() & 0x7FFFFFFF) % tableSize;
            for (int i = 0; i < tableSize; ++i) {
                final String candidate = this.variableTable[tableIndex];
                if (candidate != null && candidate.equals(object)) {
                    return this.indexTable[tableIndex];
                }
                tableIndex = (tableIndex + 1) % tableSize;
            }
            return -1;
        }

        @Override
        public int lastIndexOf(final Object object) {
            return indexOf(object);
        }

    }

    private static final class EmptyCompactBindingSet extends CompactBindingSet {

        private static final long serialVersionUID = 1L;

        EmptyCompactBindingSet(final VariableList variables) {
            super(variables);
        }

        @Override
        @Nullable
        public Value get(final int index) {
            return null;
        }

    }

    private static final class SingletonCompactBindingSet extends CompactBindingSet {

        private static final long serialVersionUID = 1L;

        private final int index;

        private final Value value;

        SingletonCompactBindingSet(final VariableList variables, final int index, //
                final Value value) {
            super(variables);
            this.index = index;
            this.value = value;
        }

        @Override
        @Nullable
        public Value get(final int index) {
            return index == this.index ? this.value : null;
        }

    }

    private static final class ArrayCompactBindingSet extends CompactBindingSet {

        private static final long serialVersionUID = 1L;

        private final Value[] values;

        ArrayCompactBindingSet(final VariableList variables, final Value[] values) {
            super(variables);
            this.values = values;
        }

        @Override
        @Nullable
        public Value get(final int index) {
            return index >= 0 && index < this.values.length ? this.values[index] : null;
        }

    }

}
