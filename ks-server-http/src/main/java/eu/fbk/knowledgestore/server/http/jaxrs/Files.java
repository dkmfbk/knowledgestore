package eu.fbk.knowledgestore.server.http.jaxrs;

import java.io.InputStream;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.enunciate.jaxrs.ResponseCode;
import org.codehaus.enunciate.jaxrs.ResponseHeader;
import org.codehaus.enunciate.jaxrs.ResponseHeaders;
import org.codehaus.enunciate.jaxrs.StatusCodes;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.Operation;
import eu.fbk.knowledgestore.Outcome;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Representation;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.internal.jaxrs.Protocol;
import eu.fbk.knowledgestore.vocabulary.NFO;
import eu.fbk.knowledgestore.vocabulary.NIE;

/**
 * Manages a collection of files.
 * <p>
 * This root REST resource allows the download, upload and removal of resource files in the
 * KnowledgeStore.
 * </p>
 * <p>
 * File download is performed via GET requests and supports caching and conditional requests based
 * on file modification date and ETag (MD5 hash of file content); file metadata is taken from the
 * <tt>ks:storedAs</tt> resource property and is returned via standard HTTP headers.
 * </p>
 * <p>
 * File upload can be performed via PUT requests whose body is the file content, or via POST
 * request with <tt>multipart/form-data</tt> body; the PUT approach should be preferred in client
 * libraries supporting the PUT operation, whereas the POST approach can be used when uploading
 * from an HTML form using a browser. File metadata can be supplied either via standard HTTP
 * headers or via custom <tt>X-KS-Content-Meta</tt> key-value headers.
 * </p>
 * <p>
 * File deletion can be performed either with DELETE requests or via POST requests lacking a
 * <tt>file</tt> form parameter.
 * </p>
 */
@Path("/" + Protocol.PATH_REPRESENTATIONS)
public class Files extends Resource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Files.class);

    /**
     * Retrieves a file. Technically, this operation returns the representation of a file <i>HTTP
     * resource</i> whose URI is fully determined by the <tt>id</tt> query parameter that encodes
     * the URI of the KnowledgeStore resource the file refers to. The operation:
     * <ul>
     * <li>supports the use of HTTP preconditions in the form of If-Match, If-None-Match,
     * If-Modified-Since, If-Unmodified-Since headers;</li>
     * <li>allows the client to accept only representations in a certain MIME type, via Accept
     * header;</li>
     * <li>can enable / disable the use of server-side caches via header Cache-Control (specify
     * <tt>no-cache</tt> or <tt>no-store</tt> to disable caches).</li>
     * </ul>
     *
     * @param id
     *            the URI identifier of the KnowledgeStore resource (mandatory)
     * @param accept
     *            the MIME type accepted by the client (optional); a 406 NOT ACCEPTABLE response
     *            will be returned if the file representation has a non-compatible MIME type
     * @return the file content, on success, encoded using the specific file MIME type
     * @throws Exception
     *             on error
     */
    @GET
    @Produces("*/*")
    @TypeHint(InputStream.class)
    @StatusCodes({
            @ResponseCode(code = 200, condition = "if the file is found and its representation "
                    + "is returned"),
            @ResponseCode(code = 404, condition = "if the requested file does not exist (the "
                    + "associated resource may exist or not)") })
    @ResponseHeaders({
            @ResponseHeader(name = "Content-Language", description = "the 2-letters ISO 639 "
                    + "language code for file representation, if known"),
            @ResponseHeader(name = "Content-Disposition", description = "a content disposition "
                    + "directive for browsers, including the suggested file name and date for "
                    + "saving the file"),
            @ResponseHeader(name = "Content-MD5", description = "the MD5 hash of the file "
                    + "representation") })
    public Response get(@QueryParam(Protocol.PARAMETER_ID) final URI id,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.WILDCARD) final String accept)
            throws Exception {

        // Check query string parameters
        checkNotNull(id, Outcome.Status.ERROR_INVALID_INPUT, "Missing 'id' query parameter");

        // Retrieve the file to return
        final Representation representation = getSession() //
                .download(id) //
                .timeout(getTimeout()) //
                .accept(accept.split(",")) //
                .caching(isCachingEnabled()) //
                .exec();

        // Fail if file does not exist
        checkNotNull(representation, Outcome.Status.ERROR_OBJECT_NOT_FOUND,
                "Specified file does not exist");
        closeOnCompletion(representation);

        // Retrieve file metadata and build the resulting Content-Disposition header
        final Record metadata = representation.getMetadata();
        final Long fileSize = metadata.getUnique(NFO.FILE_SIZE, Long.class, null);
        final String fileName = metadata.getUnique(NFO.FILE_NAME, String.class, null);
        final String mimeType = metadata.getUnique(NIE.MIME_TYPE, String.class, null);
        final Date lastModified = extractLastModified(representation);
        final String tag = extractMD5(representation);
        final ContentDisposition disposition = ContentDisposition.type("attachment")
                .fileName(fileName) //
                .modificationDate(lastModified) //
                .size(fileSize != null ? fileSize : -1) //
                .build();

        // Validate client preconditions, do negotiation and handle probe requests
        init(false, mimeType, lastModified, tag);

        // Stream the file to the client. Note that Content-Length is not set as it will not be
        // valid after GZIP compression is applied (Jersey should remove it, but it doesn't)
        return newResponseBuilder(Status.OK, representation, null).header(
                HttpHeaders.CONTENT_DISPOSITION, disposition).build();
    }

    /**
     * Creates or updates a file, uploading its content as the entity of the HTTP request.
     * Technically, this operation stores the representation of a file <i>HTTP resource</i> whose
     * URI is fully determined by the <tt>id</tt> query parameter that encodes the URI of the
     * KnowledgeStore resource the file refers to. The operation:
     * <ul>
     * <li>can result either in the file being created or updated;</li>
     * <li>supports the use of HTTP preconditions in the form of If-Match, If-None-Match,
     * If-Modified-Since, If-Unmodified-Since headers;</li>
     * <li>can supply arbitrary metadata about the file using zero or more occurrences of the
     * X-KS-Content-Meta <tt>property value</tt> non-standard header, where properties and values
     * are encoded using the Turtle syntax.</li>
     * </ul>
     *
     * @param id
     *            the URI identifier of the KnowledgeStore resource (mandatory, must refer to an
     *            existing resource for the request to be valid)
     * @param representation
     *            the file to store
     * @return the operation outcome, encoded in one of the supported RDF MIME types
     * @throws Exception
     *             on error
     */
    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    @StatusCodes({ @ResponseCode(code = 200, condition = "if the file has been updated"),
            @ResponseCode(code = 201, condition = "if the file has been created") })
    public Response put(@QueryParam(Protocol.PARAMETER_ID) final URI id,
            final Representation representation) throws Exception {

        // Schedule closing of input entity
        closeOnCompletion(representation);

        // Check query string parameters
        checkNotNull(id, Outcome.Status.ERROR_INVALID_INPUT, "Missing 'id' query parameter");

        // Setup the UPLOAD operation, returning an error if parameters are wrong
        final Operation.Upload operation;
        try {
            operation = getSession().upload(id).timeout(getTimeout())
                    .representation(representation);
        } catch (final RuntimeException ex) {
            throw newException(Outcome.Status.ERROR_INVALID_INPUT, ex, null);
        }

        // Retrieve old file for the same resource
        Representation oldRepresentation = null;
        try {
            oldRepresentation = getSession().download(id).timeout(getTimeout()).exec();
            closeOnCompletion(oldRepresentation);
        } catch (final Throwable ex) {
            LOGGER.error("Error retrieving current files associated to resource " + id, ex);
        }

        // Handle two cases for validating preconditions, doing negotiation and handling probes
        if (oldRepresentation == null) {
            // No old file: new file will be stored
            init(true, null);

        } else {
            // Old file exists: check preconditions based on its ETag and last modified
            final Date getLastModified = extractLastModified(oldRepresentation);
            final String getTag = extractMD5(oldRepresentation);
            oldRepresentation.close();
            init(true, null, getLastModified, getTag);
        }

        // Perform the operation
        final Outcome outcome = operation.exec();

        // Setup the response stream
        final int httpStatus = outcome.getStatus().getHTTPStatus();
        final Stream<Outcome> entity = Stream.create(outcome);

        // Stream the Outcome result to the client
        return newResponseBuilder(httpStatus, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    /**
     * Creates, updates or deletes a file, using a multipart form data HTTP entity that is
     * compatible with the POST submission HTML forms. Technically, the operation targets the
     * <tt>Files</tt> <i>HTTP resource</i> (controller), supplying all the data necessary for
     * uploading the file in a single multipart message. The operation:
     * <ul>
     * <li>can result either in the file being created, updated or deleted (deletion occurs if no
     * file content is included in the multipart message);</li>
     * <li>can supply arbitrary metadata about the file using either <tt>property = value</tt>
     * form parameters encoded in the multipart message or by sending zero or more occurrences of
     * the X-KS Content-Meta non-standard <tt>property value</tt> header; in both cases,
     * properties and values are encoded using Turtle syntax.</li>
     * </ul>
     *
     * @param formData
     *            a multipart form data entity containing a body part for the <tt>id</tt> URI
     *            parameter (must denote an existing resource for the request to be valid), a body
     *            part for the <tt>file</tt> parameter and optional body parts for additional
     *            metadata attributes about the uploaded file
     * @return the operation outcome, encoded in one of the supported RDF MIME types
     * @throws Exception
     *             on error
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Stream.class)
    @StatusCodes({
            @ResponseCode(code = 200, condition = "if the file has been updated or deleted"),
            @ResponseCode(code = 201, condition = "if the file has been created") })
    @ResponseHeaders({ @ResponseHeader(name = "Location", description = "the URI of "
            + "the created file") })
    public Response post(final FormDataMultiPart formData) throws Exception {

        // Validate preconditions and handle probe requests here, before body is consumed
        // POST URI does not support GET, hence no tag and last modified
        init(true, null);

        // Process the form parameters encoded in the request body
        URI id = null;
        Representation representation = null;
        final Record record = Record.create();
        for (final BodyPart bodyPart : formData.getBodyParts()) {

            // Handle three types of parameters
            final FormDataBodyPart part = (FormDataBodyPart) bodyPart;
            final String name = part.getName();
            if ("id".equals(name)) {
                // 'id' parameter: the ID of the resource
                id = Data.getValueFactory().createURI(name);

            } else if ("file".equals(name)) {
                // 'file' parameter: the uploaded file, with some metadata
                representation = closeOnCompletion(part.getEntityAs(Representation.class));
                final ContentDisposition disposition = checkNotNull(part.getContentDisposition(),
                        Outcome.Status.ERROR_INVALID_INPUT,
                        "Missing Content-Disposition header for body part " + part.getName());
                final Record metadata = representation.getMetadata();
                metadata.set(NFO.FILE_NAME, disposition.getFileName());
                metadata.set(NFO.FILE_LAST_MODIFIED, disposition.getModificationDate());

            } else {
                // other parameters: treat them as additional file metadata
                final URI property = (URI) Data.parseValue(name, Data.getNamespaceMap());
                final Value value = Data.parseValue(part.getEntityAs(String.class),
                        Data.getNamespaceMap());
                record.add(property, value);
            }
        }

        // Check the ID parameters was supplied
        checkNotNull(id, Outcome.Status.ERROR_INVALID_INPUT, "Missing 'id' form parameter");
        assert id != null;

        // If a file was uploaded, extend it with the additional metadata
        if (representation != null) {
            // Protocol.decodeMetadata(encodedMetadata, metadata);
            final Record metadata = representation.getMetadata();
            for (final URI property : record.getProperties()) {
                metadata.set(property, record.get(property));
            }
        }

        // Perform the operation
        final Outcome outcome = getSession().upload(id).timeout(getTimeout())
                .representation(representation).exec();

        // Setup the response stream
        final int httpStatus = outcome.getStatus().getHTTPStatus();
        final Stream<Outcome> entity = Stream.create(outcome);

        // Stream the result to the client
        return newResponseBuilder(httpStatus, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    /**
     * Deletes a file. Technically, the operation targets a file <i>HTTP resource</i> whose URI is
     * fully determined by the <tt>id</tt> query parameter that encodes the URI of the
     * KnowledgeStore resource the file refers to. The operation supports the use of HTTP
     * preconditions in the form of If-Match, If-None-Match, If-Modified-Since,
     * If-Unmodified-Since headers.
     *
     * @param id
     *            the URI identifier of the KnowledgeStore resource (mandatory)
     * @return the operation outcome, encoded in one of the supported RDF MIME types
     * @throws Exception
     *             on error
     */
    @DELETE
    @Produces(Protocol.MIME_TYPES_RDF)
    @TypeHint(Outcome.class)
    @StatusCodes({
            @ResponseCode(code = 200, condition = "if the file has been deleted"),
            @ResponseCode(code = 404, condition = "if the file does not exist (the associated "
                    + "resource may exist or not)") })
    public Response delete(@QueryParam(Protocol.PARAMETER_ID) final URI id) throws Exception {

        // Check query string parameters
        checkNotNull(id, Outcome.Status.ERROR_INVALID_INPUT, "Missing 'id' query parameter");

        // Retrieve the file to delete and fail if it does not exist
        final Representation oldRepresentation = getSession().download(id).timeout(getTimeout())
                .exec();
        closeOnCompletion(oldRepresentation);
        checkNotNull(oldRepresentation, Outcome.Status.ERROR_OBJECT_NOT_FOUND,
                "Specified file does not exist.");

        // Retrieve ETag and last modified for validation of preconditions
        final Date getLastModified = extractLastModified(oldRepresentation);
        final String getTag = extractMD5(oldRepresentation);
        oldRepresentation.close();

        // Setup the UPLOAD operation, returning an error if parameters are wrong
        final Operation.Upload operation;
        try {
            operation = getSession().upload(id).timeout(getTimeout()).representation(null);
        } catch (final RuntimeException ex) {
            throw newException(Outcome.Status.ERROR_INVALID_INPUT, ex, null);
        }

        // Validate preconditions, do negotiation and handle probing
        init(true, null, getLastModified, getTag);

        // Perform the operation
        final Outcome outcome = operation.exec();

        // Setup the resulting stream
        final int httpStatus = outcome.getStatus().getHTTPStatus();
        final Stream<Outcome> entity = Stream.create(outcome);

        // Stream the Outcome result to the client
        return newResponseBuilder(httpStatus, entity, Protocol.STREAM_OF_OUTCOMES).build();
    }

    private static Date extractLastModified(final Representation representation) {
        final Record metadata = representation.getMetadata();
        return metadata.getUnique(NFO.FILE_LAST_MODIFIED, Date.class, null);
    }

    private static String extractMD5(final Representation representation) {
        final Record metadata = representation.getMetadata();
        final Record hash = metadata.getUnique(NFO.HAS_HASH, Record.class, null);
        if (hash != null && "MD5".equals(hash.getUnique(NFO.HASH_ALGORITHM, String.class, null))) {
            return hash.getUnique(NFO.HASH_VALUE, String.class, null);
        }
        return null;
    }

}
