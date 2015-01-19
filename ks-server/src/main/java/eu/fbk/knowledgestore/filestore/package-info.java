/**
 * {@code FileStore} server-side component API ({@code ks-server}).
 * <p>
 * This package defines the API of the {@code FileStore} internal server-side component, whose
 * task is to provide a persistent storage for resource representations, acting in a way similar
 * to a file system. More in details, the package provides:
 * </p>
 * <ul>
 * <li>the {@code FileStore} API ({@link eu.fbk.knowledgestore.filestore.FileStore});</li>
 * <li>a base implementation ({@link eu.fbk.knowledgestore.filestore.HadoopFileStore}) that relies
 * on the Hadoop API to abstract the access to the underlying file system, allowing to
 * transparently use both the local file system and a distributed file systems such as HDFS;</li>
 * <li>an abstract class ({@link eu.fbk.knowledgestore.filestore.ForwardingFileStore}) for
 * implementing the decorator pattern;</li>
 * <li>two concrete decorator classes that augment a {@code FileStore} with logging capabilities (
 * {@link eu.fbk.knowledgestore.filestore.LoggingFileStore} and transparent GZip compression (
 * {@link eu.fbk.knowledgestore.filestore.GzippedFileStore}).</li>
 * </ul>
 * <p>
 * Custom implementations of the {@code FileStore} component may be provided by the user to
 * customize the way the KnowledgeStore stores resource files.
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.filestore;

