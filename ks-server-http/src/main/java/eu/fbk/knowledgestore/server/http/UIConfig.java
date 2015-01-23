package eu.fbk.knowledgestore.server.http;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.vocabulary.KS;

public final class UIConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_RESULT_LIMIT = 1000;

    private static final Set<URI> DEFAULT_RESOURCE_OVERVIEW_PROPERTIES = ImmutableSet.of(
            DCTERMS.CREATED, DCTERMS.TITLE);

    private static final Set<URI> DEFAULT_MENTION_OVERVIEW_PROPERTIES = ImmutableSet.of( //
            RDF.TYPE, KS.REFERS_TO);

    private static final URI DEFAULT_DENOTED_BY_PROPERTY = new URIImpl(Data.getNamespaceMap().get(
            "gaf")
            + "denotedBy");

    private static final boolean DEFAULT_REFERS_TO_FUNCTIONAL = true;

    private final int resultLimit;

    private final List<URI> resourceOverviewProperties;

    private final List<URI> mentionOverviewProperties;

    private final List<Category> mentionCategories;

    private final List<Example> lookupExamples;

    private final List<Example> sparqlExamples;

    private final URI denotedByProperty;

    private final boolean refersToFunctional;

    private UIConfig(final Builder builder) {
        this.resultLimit = Objects.firstNonNull(builder.resultLimit, DEFAULT_RESULT_LIMIT);
        this.resourceOverviewProperties = ImmutableList.copyOf(Objects.firstNonNull(
                builder.resourceOverviewProperties, DEFAULT_RESOURCE_OVERVIEW_PROPERTIES));
        this.mentionOverviewProperties = ImmutableList.copyOf(Objects.firstNonNull(
                builder.mentionOverviewProperties, DEFAULT_MENTION_OVERVIEW_PROPERTIES));
        this.mentionCategories = builder.mentionCategories == null ? ImmutableList.<Category>of()
                : ImmutableList.copyOf(builder.mentionCategories);
        this.lookupExamples = builder.lookupExamples == null ? ImmutableList.<Example>of()
                : ImmutableList.copyOf(builder.lookupExamples);
        this.sparqlExamples = builder.sparqlExamples == null ? ImmutableList.<Example>of()
                : ImmutableList.copyOf(builder.sparqlExamples);
        this.denotedByProperty = builder.denotedByProperty == null ? DEFAULT_DENOTED_BY_PROPERTY
                : builder.denotedByProperty;
        this.refersToFunctional = builder.refersToFunctional == null ? DEFAULT_REFERS_TO_FUNCTIONAL
                : builder.refersToFunctional;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getResultLimit() {
        return this.resultLimit;
    }

    public List<URI> getResourceOverviewProperties() {
        return this.resourceOverviewProperties;
    }

    public List<URI> getMentionOverviewProperties() {
        return this.mentionOverviewProperties;
    }

    public List<Category> getMentionCategories() {
        return this.mentionCategories;
    }

    public List<Example> getLookupExamples() {
        return this.lookupExamples;
    }

    public List<Example> getSparqlExamples() {
        return this.sparqlExamples;
    }

    public URI getDenotedByProperty() {
        return this.denotedByProperty;
    }

    public boolean isRefersToFunctional() {
        return this.refersToFunctional;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof UIConfig)) {
            return false;
        }
        final UIConfig other = (UIConfig) object;
        return this.resultLimit == other.resultLimit
                && this.resourceOverviewProperties.equals(other.resourceOverviewProperties)
                && this.mentionOverviewProperties.equals(other.mentionOverviewProperties)
                && this.mentionCategories.equals(other.mentionCategories)
                && this.lookupExamples.equals(other.lookupExamples)
                && this.sparqlExamples.equals(other.sparqlExamples);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.resultLimit, this.resourceOverviewProperties,
                this.mentionOverviewProperties, this.mentionCategories, this.lookupExamples,
                this.sparqlExamples);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("resultLimit", this.resultLimit)
                .add("resourceOverviewProperties", this.resourceOverviewProperties)
                .add("mentionOverviewProperties", this.mentionOverviewProperties)
                .add("mentionCategories", this.mentionCategories)
                .add("lookupExamples", this.lookupExamples)
                .add("sparqlExamples", this.sparqlExamples).toString();
    }

    public static final class Category implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String label;

        private final String style;

        private final XPath condition;

        public Category(final String label, final String style, final XPath condition) {
            this.label = Preconditions.checkNotNull(label);
            this.style = Preconditions.checkNotNull(style);
            this.condition = Preconditions.checkNotNull(condition);
        }

        public String getLabel() {
            return this.label;
        }

        public String getStyle() {
            return this.style;
        }

        public XPath getCondition() {
            return this.condition;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Category)) {
                return false;
            }
            final Category other = (Category) object;
            return this.label.equals(other.label) //
                    && this.style.equals(other.style) //
                    && this.condition.equals(other.condition);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.label, this.style, this.condition);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this) //
                    .add("label", this.label) //
                    .add("style", this.style) //
                    .add("condition", this.condition) //
                    .toString();
        }

    }

    public static final class Example implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String label;

        private final String value;

        public Example(final String label, final String value) {
            this.label = Preconditions.checkNotNull(label);
            this.value = normalizeMultilineString(Preconditions.checkNotNull(value));
        }

        public String getLabel() {
            return this.label;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Example)) {
                return false;
            }
            final Example other = (Example) object;
            return this.label.equals(other.label) && this.value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.label, this.value);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this) //
                    .add("label", this.label) //
                    .add("value", this.value) //
                    .toString();
        }

        private static final String normalizeMultilineString(final String string) {

            final List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(string));

            int start = Integer.MAX_VALUE;
            for (final String line : lines) {
                int index = 0;
                while (index < line.length() && Character.isSpaceChar(line.charAt(index))) {
                    ++index;
                }
                if (index < line.length()) {
                    start = Math.min(start, index);
                }
            }

            String separator = "";
            final StringBuilder builder = new StringBuilder();
            for (final String line : lines) {
                if (!line.trim().isEmpty()) {
                    builder.append(separator).append(
                            line.substring(Math.min(line.length(), start)));
                    separator = "\n";
                }
            }

            return builder.toString();
        }

    }

    public static final class Builder {

        @Nullable
        private Integer resultLimit;

        @Nullable
        private Iterable<? extends URI> resourceOverviewProperties;

        @Nullable
        private Iterable<? extends URI> mentionOverviewProperties;

        @Nullable
        private Iterable<? extends Category> mentionCategories;

        @Nullable
        private Iterable<? extends Example> lookupExamples;

        @Nullable
        private Iterable<? extends Example> sparqlExamples;

        @Nullable
        private URI denotedByProperty;

        @Nullable
        private Boolean refersToFunctional = true;

        public Builder resultLimit(@Nullable final Integer resultLimit) {
            this.resultLimit = resultLimit;
            return this;
        }

        public Builder resourceOverviewProperties(
                @Nullable final Iterable<? extends URI> resourceOverviewProperties) {
            this.resourceOverviewProperties = resourceOverviewProperties;
            return this;
        }

        public Builder mentionOverviewProperties(
                @Nullable final Iterable<? extends URI> mentionOverviewProperties) {
            this.mentionOverviewProperties = mentionOverviewProperties;
            return this;
        }

        public Builder mentionCategories(
                @Nullable final Iterable<? extends Category> mentionCategories) {
            this.mentionCategories = mentionCategories;
            return this;
        }

        public Builder lookupExamples(@Nullable final Iterable<? extends Example> lookupExamples) {
            this.lookupExamples = lookupExamples;
            return this;
        }

        public Builder sparqlExamples(@Nullable final Iterable<? extends Example> sparqlExamples) {
            this.sparqlExamples = sparqlExamples;
            return this;
        }

        public Builder denotedByProperty(@Nullable final URI denotedByProperty) {
            this.denotedByProperty = denotedByProperty;
            return this;
        }

        public Builder refersToFunctional(@Nullable final Boolean refersToFunctional) {
            this.refersToFunctional = refersToFunctional;
            return this;
        }

        public UIConfig build() {
            return new UIConfig(this);
        }

    }

}
