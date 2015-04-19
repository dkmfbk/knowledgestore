package eu.fbk.knowledgestore.server.http.jaxrs;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.codehaus.enunciate.jaxrs.ResponseCode;
import org.codehaus.enunciate.jaxrs.StatusCodes;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

import eu.fbk.knowledgestore.Operation;
import eu.fbk.knowledgestore.OperationException;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Criteria;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.vocabulary.KSR;

public abstract class Crud extends Resource {

    private static final long DEFAULT_RETRIEVE_LIMIT = 1000;

    private final URI recordType;

    Crud(final URI recordType) {
        this.recordType = Preconditions.checkNotNull(recordType);
    }

    final URI getRecordType() {
        return this.recordType;
    }

    @GET
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    @StatusCodes({ @ResponseCode(code = 200, condition = "if the request is acceptable and the "
            + "query is being executed") })
    public Response retrieve(
            @QueryParam(Protocol.PARAMETER_CONDITION) final List<XPath> conditions,
            @QueryParam(Protocol.PARAMETER_ID) final List<String> ids,
            @QueryParam(Protocol.PARAMETER_PROPERTY) final List<String> properties,
            @QueryParam(Protocol.PARAMETER_OFFSET) final Long offset,
            @QueryParam(Protocol.PARAMETER_LIMIT) final Long limit) throws OperationException {

        // Apply default limit, if not explicitly given
        final long actualLimit = MoreObjects.firstNonNull(limit, DEFAULT_RETRIEVE_LIMIT);

        // Prepare the retrieve operation, returning an error if parameters are wrong
        final Operation.Retrieve operation;
        try {
            operation = getSession().retrieve(getRecordType()) //
                    .timeout(getTimeout()) //
                    .conditions(emptyToNull(conditions)) //
                    .ids(emptyToNull(parseURIs(ids))) //
                    .properties(emptyToNull(parseURIs(properties))) //
                    .offset(offset) //
                    .limit(actualLimit);
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions, using default last modified and tag
        init(false, null, null, null);

        // Setup the resulting stream (materialized only for GET requests)
        Stream<Record> entity;
        if (getMethod().equals(HttpMethod.HEAD)) {
            entity = Stream.create();
        } else {
            entity = operation.exec();
        }
        entity.setProperty("types", ImmutableSet.of(getRecordType()));

        // Stream the results in the HTTP response
        return newResponseBuilder(Status.OK, closeOnCompletion(entity), Protocol.STREAM_OF_RECORDS)
                .build();
    }

    @GET
    @Path(Protocol.SUBPATH_COUNT)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    @StatusCodes({ @ResponseCode(code = 200, condition = "if the request is acceptable and the "
            + "requested count is being returned") })
    public Response count( //
            @QueryParam(Protocol.PARAMETER_CONDITION) final List<XPath> conditions, //
            @QueryParam(Protocol.PARAMETER_ID) final List<String> ids) throws OperationException {

        // Prepare the count operation, returning an error if parameters are wrong
        final Operation.Count operation;
        try {
            operation = getSession() //
                    .count(getRecordType()) //
                    .timeout(getTimeout()) //
                    .conditions(emptyToNull(conditions)) //
                    .ids(emptyToNull(parseURIs(ids)));
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions, using default last modified and tag
        init(false, null, null, null);

        // Setup the resulting stream (materialized only for GET requests)
        Stream<Statement> entity;
        if (getMethod().equals(HttpMethod.HEAD)) {
            entity = Stream.create();
        } else {
            final long count = operation.exec();
            final ValueFactory factory = Data.getValueFactory();
            final Literal literal = factory.createLiteral(count);
            final Statement statement = factory.createStatement(getInvocationID(), KSR.RESULT,
                    literal);
            entity = Stream.create(statement);
        }

        // Return the result in the HTTP response
        return newResponseBuilder(Status.OK, closeOnCompletion(entity),
                Protocol.STREAM_OF_STATEMENTS).build();
    }

    @POST
    @Path(Protocol.SUBPATH_CREATE)
    @Consumes(Protocol.MIME_TYPES_RDF)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    public Response create(final Stream<Record> records) throws OperationException {

        // Schedule closing of input entity
        closeOnCompletion(records);

        // Setup the Create operation, returning an error if parameters are wrong
        final Operation.Create operation;
        try {
            operation = getSession() //
                    .create(getRecordType()) //
                    .timeout(getTimeout()) //
                    .records(records);
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions and handle probing
        init(true, null);

        // Setup record decoding
        records.setProperty("types", ImmutableList.of(getRecordType()));
        closeOnCompletion(records);

        // Perform the operation
        final List<Outcome> outcomes = Lists.newArrayList();
        operation.exec(outcomes);

        // Setup the resulting stream
        final Stream<Outcome> entity = Stream.create(outcomes);
        entity.setProperty("types", ImmutableSet.of(KSR.INVOCATION));

        // final Stream<Outcome> entity = new Stream<Outcome>() {
        //
        // @Override
        // protected void doToHandler(final Handler<? super Outcome> handler) {
        // try {
        // operation.exec(handler);
        // } catch (final Throwable ex) {
        // propagateIfNotBulk(ex);
        // }
        // }
        //
        // };

        // Stream the results in the HTTP response
        return newResponseBuilder(Status.OK, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    @POST
    @Path(Protocol.SUBPATH_MERGE)
    @Consumes(Protocol.MIME_TYPES_RDF)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    public Response merge( //
            @QueryParam(Protocol.PARAMETER_CRITERIA) final List<Criteria> criterias, //
            final Stream<Record> records) throws OperationException {

        // Schedule closing of input entity
        closeOnCompletion(records);

        // Setup the merge operation, returning an error if parameters are wrong
        final Operation.Merge operation;
        try {
            operation = getSession() //
                    .merge(getRecordType()) //
                    .timeout(getTimeout()) //
                    .records(records) //
                    .criteria(emptyToNull(criterias));
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions
        init(true, null);

        // Setup record decoding
        records.setProperty("types", ImmutableList.of(getRecordType()));
        closeOnCompletion(records);

        // Perform the operation
        final List<Outcome> outcomes = Lists.newArrayList();
        operation.exec(outcomes);

        // Setup the resulting stream
        final Stream<Outcome> entity = Stream.create(outcomes);
        entity.setProperty("types", ImmutableSet.of(KSR.INVOCATION));

        // final Stream<Outcome> entity = new Stream<Outcome>() {
        //
        // @Override
        // protected void doToHandler(final Handler<? super Outcome> handler) {
        // try {
        // operation.exec(handler);
        // } catch (final Throwable ex) {
        // propagateIfNotBulk(ex);
        // }
        // }
        //
        // };

        // Stream the results in the HTTP response
        return newResponseBuilder(Status.OK, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    @POST
    @Path(Protocol.SUBPATH_UPDATE)
    @Consumes(Protocol.MIME_TYPES_RDF)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    public Response update( //
            @QueryParam(Protocol.PARAMETER_CONDITION) final List<XPath> conditions, //
            @QueryParam(Protocol.PARAMETER_ID) final List<URI> ids, //
            @QueryParam(Protocol.PARAMETER_CRITERIA) final List<Criteria> criterias, //
            final Stream<Record> records) throws OperationException {

        // Schedule closing of input entity
        closeOnCompletion(records);

        // Setup the update operation (apart record), returning an error if parameters are wrong
        final Operation.Update operation;
        try {
            operation = getSession() //
                    .update(getRecordType()) //
                    .timeout(getTimeout()) //
                    .conditions(emptyToNull(conditions)) //
                    .ids(emptyToNull(ids)) //
                    .criteria(emptyToNull(criterias));
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions and handle probing
        init(true, null);

        // Decode input record
        records.setProperty("types", ImmutableList.of(getRecordType()));
        final Record record = records.getUnique();

        // Perform the operation
        final List<Outcome> outcomes = Lists.newArrayList();
        operation.record(record).exec(outcomes);

        // Setup the resulting stream
        final Stream<Outcome> entity = Stream.create(outcomes);
        entity.setProperty("types", ImmutableSet.of(KSR.INVOCATION));

        // final Stream<Outcome> entity = new Stream<Outcome>() {
        //
        // @Override
        // protected void doToHandler(final Handler<? super Outcome> handler) {
        // try {
        // operation.record(record).exec(handler);
        // } catch (final Throwable ex) {
        // propagateIfNotBulk(ex);
        // }
        // }
        //
        // };

        // Stream the results in the HTTP response
        return newResponseBuilder(Status.OK, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    @POST
    @Path(Protocol.SUBPATH_DELETE)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    public Response delete( //
            @QueryParam(Protocol.PARAMETER_CONDITION) final List<XPath> conditions, //
            @QueryParam(Protocol.PARAMETER_ID) final List<URI> ids) throws OperationException {

        // Setup the delete operation, returning an error if parameters are wrong
        final Operation.Delete operation;
        try {
            operation = getSession() //
                    .delete(getRecordType()) //
                    .timeout(getTimeout()) //
                    .conditions(emptyToNull(conditions)) //
                    .ids(emptyToNull(ids));
        } catch (final IllegalArgumentException ex) {
            throw new OperationException(newOutcome(Outcome.Status.ERROR_INVALID_INPUT,
                    ex.getMessage()), ex);
        }

        // Validate client preconditions and handle probing
        init(true, null);

        // Perform the operation
        final List<Outcome> outcomes = Lists.newArrayList();
        operation.exec(outcomes);

        // Setup the resulting stream
        final Stream<Outcome> entity = Stream.create(outcomes);
        entity.setProperty("types", ImmutableSet.of(KSR.INVOCATION));

        // final Stream<Outcome> entity = new Stream<Outcome>() {
        //
        // @Override
        // protected void doToHandler(final Handler<? super Outcome> handler) {
        // try {
        // operation.exec(handler);
        // } catch (final Throwable ex) {
        // propagateIfNotBulk(ex);
        // }
        // }
        //
        // };

        // Stream the results in the HTTP response
        return newResponseBuilder(Status.OK, closeOnCompletion(entity),
                Protocol.STREAM_OF_OUTCOMES).build();
    }

    private <T extends Iterable<?>> T emptyToNull(final T iterable) {
        return iterable == null || Iterables.isEmpty(iterable) ? null : iterable;
    }

    private List<URI> parseURIs(final Iterable<String> strings) {
        final List<URI> uris = Lists.newArrayList();
        for (final String string : strings) {
            final int length = string.length();
            boolean escape = false;
            boolean qname = false;
            int start = -1;
            for (int i = 0; i < length; ++i) {
                final char ch = string.charAt(i);
                if (escape) {
                    escape = false;
                } else if (start >= 0) {
                    if (qname) {
                        if (ch == ',' || ch == ';' || Character.isWhitespace(ch)) {
                            uris.add((URI) Data.parseValue(string.substring(start, i),
                                    Data.getNamespaceMap()));
                            start = -1;
                        }
                    } else {
                        if (ch == '\\') {
                            escape = true;
                        } else if (ch == '>') {
                            uris.add((URI) Data.parseValue(string.substring(start, i + 1),
                                    Data.getNamespaceMap()));
                            start = -1;

                        }
                    }
                } else if (ch == '<') {
                    start = i;
                    qname = false;
                } else if (!Character.isWhitespace(ch)) {
                    start = i;
                    qname = true;
                }
            }
            if (start >= 0) {
                if (qname) {
                    uris.add((URI) Data.parseValue(string.substring(start), Data.getNamespaceMap()));
                } else {
                    throw new IllegalArgumentException("Invalid ID(s): " + string);
                }
            }
        }

        return uris;
    }

}
