package eu.fbk.knowledgestore.triplestore;

import java.io.ObjectStreamException;
import java.io.Serializable;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;

import eu.fbk.knowledgestore.data.ParseException;

/**
 * A SPARQL SELECT query.
 * <p>
 * This class models the specification of a SPARQL SELECT query, combining in a single object both
 * its query string representation (property {@link #getString()}) and its Sesame algebraic
 * representation (properties {@link #getExpression()}, {@link #getDataset()}).
 * </p>
 * <p>
 * A <tt>SelectQuery</tt> can be created either providing its string representation (method
 * {@link #from(String)}) or its algebraic expression together with the query dataset (method
 * {@link #from(TupleExpr, Dataset)}). In both case, the dual representation is automatically
 * derived, either via parsing or rendering to SPARQL language. The two <tt>from</tt> factory
 * methods provide for the caching and reuse of already created objects, thus reducing parsing
 * overhead. A {@link MalformedQueryException} is thrown in case the supplied representation
 * (either the query string or the algebraic expression) does not denote a valid SPARQL SELECT
 * query.
 * </p>
 * <p>
 * Serialization is supported, with deserialization attempting to reuse existing objects from the
 * cache. Note that only the string representation is serialized, with the algebraic expression
 * obtained at deserialization time via parsing.
 * </p>
 * <p>
 * Instances have to considered to be immutable: while it is not possible to (efficiently) forbid
 * modifying the query tuple expression ({@link #getExpression()}), THE ALGEBRAIC EXPRESSION MUST
 * NOT BE MODIFIED, as this will interfere with caching.
 * </p>
 */
public final class SelectQuery implements Serializable {

    /** Version identification code for serialization. */
    private static final long serialVersionUID = -3361485014094610488L;

    /**
     * A cache keeping track of created instances, which may be reclaimed by the GC. Instances are
     * indexed by their query string.
     */
    private static final Cache<String, SelectQuery> CACHE = CacheBuilder.newBuilder().softValues()
            .build();

    /** The query string. */
    private final String string;

    /** The algebraic tuple expression. */
    private final TupleExpr expression;

    /** The optional dataset associated to the query. */
    @Nullable
    private final Dataset dataset;

    /**
     * Returns a <tt>SelectQuery</tt> for the specified SPARQL SELECT query string.
     * 
     * @param string
     *            the query string, in SPARQL and without relative URIs
     * @return the corresponding <tt>SelectQuery</tt>
     * @throws ParseException
     *             in case the string does not denote a valid SPARQL SELECT query
     */
    public static SelectQuery from(final String string) throws ParseException {

        Preconditions.checkNotNull(string);

        SelectQuery query = CACHE.getIfPresent(string);
        if (query == null) {
            final ParsedTupleQuery parsedQuery;
            try {
                parsedQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, string, null);
            } catch (final IllegalArgumentException ex) {
                throw new ParseException(string, "SPARQL query not in SELECT form", ex);
            } catch (final MalformedQueryException ex) {
                throw new ParseException(string, "Invalid SPARQL query: " + ex.getMessage(), ex);
            }
            query = new SelectQuery(string, parsedQuery.getTupleExpr(), parsedQuery.getDataset());
            CACHE.put(string, query);
        }
        return query;
    }

    /**
     * Returns an <tt>SelectQuery</tt> for the algebraic expression and optional dataset
     * specified.
     * 
     * @param expression
     *            the algebraic expression for the query
     * @param dataset
     *            the dataset optionally associated to the query
     * @return the corresponding <tt>SelectQuery</tt> object
     * @throws ParseException
     *             in case the supplied algebraic expression does not denote a valid SPARQL SELECT
     *             query
     */
    public static SelectQuery from(final TupleExpr expression, @Nullable final Dataset dataset)
            throws ParseException {

        Preconditions.checkNotNull(expression);

        try {
            // Sesame rendering facilities are definitely broken, so we use our own
            final String string = new SPARQLRenderer(null, true).render(expression, dataset);
            SelectQuery query = CACHE.getIfPresent(string);
            if (query == null) {
                query = new SelectQuery(string, expression, dataset);
                CACHE.put(string, query);
            }
            return query;

        } catch (final Exception ex) {
            throw new ParseException(expression.toString(),
                    "The supplied algebraic expression does not denote a valid SPARQL query", ex);
        }
    }

    /**
     * Private constructor, accepting parameters for all the object properties.
     * 
     * @param string
     *            the query string
     * @param expression
     *            the algebraic expression
     * @param dataset
     *            the query dataset, null if unspecified
     */
    private SelectQuery(final String string, final TupleExpr expression,
            @Nullable final Dataset dataset) {
        this.string = string;
        this.expression = expression;
        this.dataset = dataset;
    }

    /**
     * Returns the query string. The is possibly automatically rendered from a supplied algebraic
     * expression.
     * 
     * @return the query string
     */
    public String getString() {
        return this.string;
    }

    /**
     * Returns the algebraic expression for the query - DON'T MODIFY THE RESULT. As a query
     * expression must be cached for performance reasons, modifying it would affect all subsequent
     * operations on the same <tt>SelectQuery</tt> object, so CLONE THE EXPRESSION BEFORE
     * MODIFYING IT.
     * 
     * @return the algebraic expression for this query
     */
    public TupleExpr getExpression() {
        return this.expression;
    }

    /**
     * Returns the dataset expressed by the FROM and FROM NAMED clauses of the query, or
     * <tt>null</tt> if there are no such clauses.
     * 
     * @return the dataset, possibly null
     */
    @Nullable
    public Dataset getDataset() {
        return this.dataset;
    }

    /**
     * Replaces the dataset of this query with the one specified, returning the resulting
     * <tt>SelectQuery</tt> object.
     * 
     * @param dataset
     *            the new dataset; as usual, <tt>null</tt> denotes the default dataset (all the
     *            graphs)
     * @return the resulting <tt>SelectQuery</tt> object (possibly <tt>this</tt> if no change is
     *         required)
     */
    public SelectQuery replaceDataset(@Nullable final Dataset dataset) {
        if (Objects.equal(this.dataset, dataset)) {
            return this;
        } else {
            try {
                return from(this.expression, dataset);
            } catch (final ParseException ex) {
                throw new Error("Unexpected error - replacing dataset made the query invalid (!)",
                        ex);
            }
        }
    }

    /**
     * Replaces some variables of this queries with the constant values specified, returning the
     * resulting <tt>SelectQuery</tt> object.
     * 
     * @param bindings
     *            the bindings to apply
     * @return the resulting <tt>SelectQuery</tt> object (possibly <tt>this</tt> if no change is
     *         required).
     */
    public SelectQuery replaceVariables(final BindingSet bindings) {

        if (bindings.size() == 0) {
            return this;
        }

        // TODO: check whether the visitor code (taken from BindingAssigner) is enough, especially
        // w.r.t. variables appearing in projection nodes (= SELECT clause).
        final TupleExpr newExpression = this.expression.clone();
        newExpression.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                if (!var.hasValue() && bindings.hasBinding(var.getName())) {
                    final Value value = bindings.getValue(var.getName());
                    var.setValue(value);
                }
            }

        });

        try {
            return from(newExpression, this.dataset);
        } catch (final ParseException ex) {
            throw new Error("Unexpected error - replacing variables made the query invalid (!)",
                    ex);
        }
    }

    /**
     * {@inheritDoc} Two instances are equal if they have the same string representation.
     */
    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof SelectQuery)) {
            return false;
        }
        final SelectQuery other = (SelectQuery) object;
        return this.string.equals(other.string);
    }

    /**
     * {@inheritDoc} The returned hash code depends only on the string representation.
     */
    @Override
    public int hashCode() {
        return this.string.hashCode();
    }

    /**
     * {@inheritDoc} Returns the query string.
     */
    @Override
    public String toString() {
        return this.string;
    }

    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(this.string);
    }

    private static final class SerializedForm {

        private final String string;

        SerializedForm(final String string) {
            this.string = string;
        }

        private Object readResolve() throws ObjectStreamException {
            SelectQuery query = CACHE.getIfPresent(this.string);
            if (query == null) {
                try {
                    query = SelectQuery.from(this.string);
                } catch (final ParseException ex) {
                    throw new Error("Serialized form denotes an invalid SPARQL queries (!)", ex);
                }
            }
            return query;
        }

    }

}
