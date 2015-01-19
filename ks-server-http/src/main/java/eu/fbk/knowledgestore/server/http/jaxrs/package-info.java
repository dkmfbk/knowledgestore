/**
 * KnowledgeStore Web API.
 * <p>
 * The KnowledgeStore Web API provides access to the operation of the KnowledgeStore using a
 * (almost-) REST interface. Below, general information about the API (supported MIME types,
 * common HTTP status codes and response headers) are reported, followed by the list of REST
 * resources exposed by the API which link to supported operations.
 * </p>
 * <p style="color: red">
 * <b>TODO: WORK IN PROGRESS, PROVIDE BETTER OVERVIEW AND LINK TO ADDITIONAL DOCUMENTATION</b>
 * </p>
 * <h2>Supported MIME types</h2>
 * <p>
 * RDF is used for the description of resources, mentions, entities and axioms. The RDF MIME types
 * in the following table are supported:
 * </p>
 * <table border="1" style="width: 100%">
 * <tr>
 * <th>MIME type</th>
 * <th>Format</th>
 * <th>Comments</th>
 * </tr>
 * <tr>
 * <td><tt>application/ld+json</tt></td>
 * <td><a href="http://www.w3.org/TR/json-ld/">JSONLD</a></td>
 * <td>preferred MIME type</td>
 * </tr>
 * <tr>
 * <td><tt>application/rdf+xml</tt></td>
 * <td><a href="http://www.w3.org/TR/REC-rdf-syntax/">RDF/XML</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>text/turtle</tt></td>
 * <td><a href="http://www.w3.org/TR/turtle/">Turtle</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>text/n3</tt></td>
 * <td><a href="http://www.w3.org/TeamSubmission/n3/">N3</a></td>
 * <td>the Turtle subset is actually used</td>
 * </tr>
 * <tr>
 * <td><tt>application/n-triples</tt></td>
 * <td><a href="http://www.w3.org/TR/n-triples/">N-Triples</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>text/x-nquads</tt></td>
 * <td><a href="http://www.w3.org/TR/n-quads/">N-Quads</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>application/x-trig</tt></td>
 * <td><a href="http://www.w3.org/TR/2014/PR-trig-20140109/">TriG</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>application/trix</tt></td>
 * <td><a href="http://www.hpl.hp.com/techreports/2004/HPL-2004-56.html">TriX</a></td>
 * <td>non standard, discouraged</td>
 * </tr>
 * <tr>
 * <td><tt>application/x-binary-rdf</tt></td>
 * <td><a href="http://rivuli-development.com/2011/11/binary-rdf-in-sesame/">Sesame binary RDF
 * format</a></td>
 * <td>non standard, discouraged</td>
 * </tr>
 * <tr>
 * <td><tt>application/rdf+json</tt></td>
 * <td><a href="https://dvcs.w3.org/hg/rdf/raw-file/default/rdf-json/index.html">RDF+JSON</a></td>
 * <td>discouraged by W3C in favour of JSONLD</td>
 * </tr>
 * </table>
 * <p>
 * DESCRIBE and CONSTRUCT operations submitted to the KnowledgeStore SPARQL endpoint returns RDF
 * data (see above), while SELECT and ASK queries return tabular data using one of the following
 * MIME types:
 * </p>
 * <table border="1" style="width: 100%">
 * <tr>
 * <th>MIME type</th>
 * <th>Format</th>
 * <th>Comments</th>
 * </tr>
 * <tr>
 * <td><tt>application/sparql-results+json</tt></td>
 * <td><a href="http://www.w3.org/TR/2013/REC-sparql11-results-json-20130321/">SPARQL JSON query
 * results Format</a></td>
 * <td>preferred MIME type</td>
 * </tr>
 * <tr>
 * <td><tt>application/sparql-results+xml</tt></td>
 * <td><a href="http://www.w3.org/TR/rdf-sparql-XMLres/">SPARQL XML query results</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>text/csv</tt></td>
 * <td><a href="http://www.w3.org/TR/2013/REC-sparql11-results-csv-tsv-20130321/#csv">SPARQL CSV
 * query results</a></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td><tt>text/tab-separated-values</tt></td>
 * <td><a href="http://www.w3.org/TR/2013/REC-sparql11-results-csv-tsv-20130321/#tsv">SPARQL TSV
 * query results</a></td>
 * <td></td>
 * </tr>
 * </table>
 * <p>
 * The MIME types of resource files is not fixed. It is the responsibility of the client to
 * specify the correct MIME type when uploading a file, which is then returned when downloading
 * it.
 * </p>
 * <h2>Common status codes</h2>
 * <p>
 * The following HTTP status codes are common to all the API operation. Therefore, they are
 * documented here rather than being reported for each specific operation:
 * </p>
 * <table border="1">
 * <tr>
 * <th style="min-width: 140px;">HTTP Status Code</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>302 Found</td>
 * <td>returned when accessing a secured URL via HTTP, redirecting to the corresponding HTTPS URL
 * (see Location header); note that request to the HTTPS URL should include valid user credentials
 * using the Authorization header (HTTP basic authentication is used)</td>
 * </tr>
 * <tr>
 * <td>304 Not Modified</td>
 * <td>returned in case of read requests (HTTP GET) when headers If-Modified-Since or
 * If-None-Match are specified and requested data has not been modified since the supplied
 * timestamp / ETag, meaning that previously cached copies of the data can be used</td>
 * </tr>
 * <tr>
 * <td>400 Bad Request</td>
 * <td>returned if the request is not valid, e.g., because a supplied query parameter, request
 * header or the request body are not valid (specific constraints are listed for each API
 * resource)</td>
 * </tr>
 * <tr>
 * <td>401 Unauthorized</td>
 * <td>returned if the client didn't authenticated itself and the requested operation has been
 * configured to require client authentication and authorization; this error can be avoided by
 * submitting the request over HTTPS including an appropriate Authorization header (HTTP basic
 * authentication is used)</td>
 * </tr>
 * <tr>
 * <td>403 Forbidden</td>
 * <td>returned if the client authenticated itself but has not enough privileges to perform the
 * requested operation; to avoid this error, either use privileged credentials or ask the server
 * administration the grant of required privileges</td>
 * </tr>
 * <tr>
 * <td>406 Not Acceptable</td>
 * <td>returned if the server cannot return a response body (e.g., query response) in any of the
 * formats specified by the client in the request Accept header</td>
 * </tr>
 * <tr>
 * <td>412 Precondition Failed</td>
 * <td>returned for conditioanl read requests (GET method) using the If-Match and
 * If-Unmodified-Since headers and conditional write requests (PUT, POST, DELETE methods) using
 * headers If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since, in case client
 * preconditions are not met</td>
 * </tr>
 * <tr>
 * <td>500 Server Error</td>
 * <td>returned in case of unexpected server error</td>
 * </tr>
 * </table>
 * <h2>Common response headers</h2>
 * <p>
 * The following HTTP response headers are common to all the API operation and are thus documented
 * here:
 * </p>
 * <table border="1">
 * <tr>
 * <th style="min-width: 140px;">Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Content-Type</td>
 * <td>always included in case an HTTP entity is returned in the response; it specifies the MIME
 * type of the returned entity</td>
 * </tr>
 * <tr>
 * <td>Content-Length</td>
 * <td>included only when the response length can be determined in advance (i.e., the length is
 * lower than the configured buffer size); this header may be omitted in case the response is
 * streamed to the client meanwhile the corresponding retrieval / modification operation is being
 * performed on the server</td>
 * </tr>
 * <tr>
 * <td>Cache-control</td>
 * <td>HTTP cache control directive either allowing or preventing caching of a response by
 * intermediate caches. The value depends on the type of operation (retrievals are cached,
 * modifications no), on whether the requested URL is secured (caching is private to the
 * authenticated user) and on whether caching was allowed by the client in the request
 * Cache-Control header. In general, whenever caching is authorized, cache revalidation is always
 * required</td>
 * </tr>
 * <tr>
 * <td>ETag</td>
 * <td>always included if an HTTP entity is returned in the response; it allows cache revalidation
 * and conditional GET requests from clients / intermediate (caching) proxies</td>
 * </tr>
 * <tr>
 * <td>Last-Modified</td>
 * <td>always included if an HTTP entity is returned in the response; it also allows cache
 * revalidation and conditional GET requests from clients / intermediate (caching) proxies</td>
 * </tr>
 * </table>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.server.http.jaxrs;

