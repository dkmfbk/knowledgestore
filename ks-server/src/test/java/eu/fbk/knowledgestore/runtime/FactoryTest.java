package eu.fbk.knowledgestore.runtime;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;

import eu.fbk.knowledgestore.internal.rdf.RDFUtil;

public class FactoryTest {

    private static final Person ALICE = Person.create("Alice", 84);

    private static final Person JACK = Person.create("Jack", 0, ALICE);

    private static final Person JOHN = Person.create("John", 30, JACK, ALICE);

    private static final String BASE = "java:eu.fbk.knowledgestore.runtime.FactoryTest$Person";

    @Test
    public void testConstructor() {
        Assert.assertEquals(ALICE, Factory.instantiate(//
                ImmutableMap.<String, Object>of("name", "Alice", "age", 84), //
                new URIImpl(FactoryTest.BASE), Object.class));
    }

    @Test
    public void testMethod() {
        Assert.assertEquals(ALICE, Factory.instantiate(//
                ImmutableMap.<String, Object>of("name", "Alice", "age", 84), //
                new URIImpl(FactoryTest.BASE + "#create"), Object.class));
    }

    @Test
    public void testBuilder() {
        Assert.assertEquals(ALICE, Factory.instantiate(//
                ImmutableMap.<String, Object>of("name", "Alice", "age", 84), //
                new URIImpl(FactoryTest.BASE + "#builder"), Object.class));
    }

    @Test
    public void testRDF() throws Throwable {
        final InputStream stream = FactoryTest.class.getResourceAsStream("FactoryTest.ttl");
        final List<Statement> statements = RDFUtil.readRDF(stream, RDFFormat.TURTLE, null, null,
                false).toList();
        stream.close();
        final Map<URI, Object> map = Factory.instantiate(statements);
        Assert.assertEquals(ALICE, map.get(new URIImpl("example:alice")));
        Assert.assertEquals(JACK, map.get(new URIImpl("example:jack")));
        Assert.assertEquals(JOHN, map.get(new URIImpl("example:john")));
    }

    public static class Person {

        private final String name;

        private final int age;

        private final List<Person> knows;

        public static Person create(final String name, final int age,
                @Nullable final Person... knows) {
            return new Person(name, age, 0, knows == null ? null : Arrays.asList(knows));
        }

        public Person(final String name, final int age, final int unused,
                @Nullable final Iterable<? extends Person> knows) {
            this.name = name;
            this.age = age;
            this.knows = knows == null ? ImmutableList.<Person>of() : ImmutableList.copyOf(knows);
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Person)) {
                return false;
            }
            final Person other = (Person) object;
            return Objects.equal(this.name, other.name) && this.age == other.age
                    && Objects.equal(this.knows, other.knows);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.name, this.age, this.knows);
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.name).append(", ").append(this.age).append(" years old");
            if (!this.knows.isEmpty()) {
                builder.append(", knows ");
                String separator = "";
                for (final Person p : this.knows) {
                    builder.append(separator).append(p.name);
                    separator = ", ";
                }
            }
            return builder.toString();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            @Nullable
            private String name;

            private int age;

            @Nullable
            private Iterable<? extends Person> knows;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder withAge(final int age) {
                this.age = age;
                return this;
            }

            public Builder setUnused(final int unused) {
                return this;
            }

            public Builder setKnows(@Nullable final Iterable<? extends Person> knows) {
                this.knows = knows;
                return this;
            }

            public Person build() {
                return new Person(this.name, this.age, 0, this.knows);
            }

        }

    }

}
