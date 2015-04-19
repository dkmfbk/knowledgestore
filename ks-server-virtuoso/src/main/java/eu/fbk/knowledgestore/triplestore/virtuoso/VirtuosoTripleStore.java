package eu.fbk.knowledgestore.triplestore.virtuoso;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.Nullable;
import javax.sql.ConnectionPoolDataSource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jdbc4.VirtuosoConnectionPoolDataSource;
import virtuoso.sesame2.driver.VirtuosoRepository;
import virtuoso.sesame2.driver.VirtuosoRepositoryConnection;

import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.triplestore.SynchronizedTripleStore;
import eu.fbk.knowledgestore.triplestore.TripleStore;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;

/**
 * A {@code TripleStore} implementation accessing an external OpenLink Virtuoso server.
 * <p>
 * This class stores and access triples in an external Virtuoso triple store, communicating to it
 * via the Virtuoso Sesame driver. Data modification is performed without relying on Virtuoso
 * transactions, in order to support bulk loading. When writing data in a read-write
 * {@code TripleTransaction}, a Virtuoso transaction is thus not created; a marker file is instead
 * stored and later removed upon successful 'commit' of the {@code TripleTransaction}; in case of
 * failure, the marker file remain on disk and signals that the triplestore is in a potentially
 * corrupted state, triggering repopulation starting from master data. Given this mechanism, it is
 * thus important for the component to be wrapped in a {@link SynchronizedTripleStore} that allows
 * at most a write transaction at a time, preventing simultaneous read transactions
 * (synchronization N:WX). Note that configuration, startup, shutdown and management in general of
 * the Virtuoso server are a responsibility of the user, with the {@code VirtuosoTripleStore}
 * component limiting to access Virtuoso for reading and writing triples.
 * </p>
 */
public final class VirtuosoTripleStore implements TripleStore {

    // see also the following resources for reference:
    // - https://newsreader.fbk.eu/trac/wiki/TripleStoreNotes
    // - http://docs.openlinksw.com/sesame/ (Virtuoso javadoc)
    // - http://www.openlinksw.com/vos/main/Main/VirtSesame2Provider

    private static final String DEFAULT_HOST = "localhost";

    private static final int DEFAULT_PORT = 1111;

    private static final String DEFAULT_USERNAME = "dba";

    private static final String DEFAULT_PASSWORD = "dba";

    private static final boolean DEFAULT_POOLING = false;

    private static final int DEFAULT_BATCH_SIZE = 5000;

    private static final int DEFAULT_FETCH_SIZE = 200;

    private static final String DEFAULT_MARKER_FILENAME = "virtuoso.bulk.transaction";

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoTripleStore.class);

    private final VirtuosoRepository virtuoso;

    private final FileSystem fileSystem;

    private final Path markerPath;

    /**
     * Creates a new instance based on the supplied most relevant properties.
     * 
     * @param fileSystem
     *            the file system where to store the marker file
     * @param host
     *            the name / IP address of the host where virtuoso is running; if null defaults to
     *            localhost
     * @param port
     *            the port Virtuoso is listening to; if null defaults to 1111
     * @param username
     *            the username to login into Virtuoso; if null defaults to dba
     * @param password
     *            the password to login into Virtuoso; if null default to dba
     */
    public VirtuosoTripleStore(final FileSystem fileSystem, @Nullable final String host,
            @Nullable final Integer port, @Nullable final String username,
            @Nullable final String password) {
        this(fileSystem, host, port, username, password, null, null, null, null);
    }

    /**
     * Creates a new instance based the supplied complete set of configuration properties.
     * 
     * @param fileSystem
     *            the file system where to store the marker file
     * @param host
     *            the name / IP address of the host where virtuoso is running; if null defaults to
     *            localhost
     * @param port
     *            the port Virtuoso is listening to; if null defaults to 1111
     * @param username
     *            the username to login into Virtuoso; if null defaults to dba
     * @param password
     *            the password to login into Virtuoso; if null default to dba
     * @param pooling
     *            true if connection pooling should be used (impact on performances is
     *            negligible); if null defaults to false
     * @param batchSize
     *            the number of added/removed triples to buffer on the client before sending them
     *            to Virtuoso as a single chunk; if null defaults to 5000
     * @param fetchSize
     *            the number of results (solutions, triples, ...) to fetch from Virtuoso in a
     *            single operation when query results are iterated; if null defaults to 200
     * @param markerFilename
     *            the name of the marker file created to signal Virtuoso is being used in a
     *            non-transactional mode; if null defaults to virtuoso.bulk.transaction
     */
    public VirtuosoTripleStore(final FileSystem fileSystem, @Nullable final String host,
            @Nullable final Integer port, @Nullable final String username,
            @Nullable final String password, @Nullable final Boolean pooling,
            @Nullable final Integer batchSize, @Nullable final Integer fetchSize,
            @Nullable final String markerFilename) {

        // Apply default values
        final String actualMarkerFilename = MoreObjects.firstNonNull(markerFilename,
                DEFAULT_MARKER_FILENAME);
        final String actualHost = MoreObjects.firstNonNull(host, DEFAULT_HOST);
        final int actualPort = MoreObjects.firstNonNull(port, DEFAULT_PORT);
        final String actualUsername = MoreObjects.firstNonNull(username, DEFAULT_USERNAME);
        final String actualPassword = MoreObjects.firstNonNull(password, DEFAULT_PASSWORD);
        final boolean actualPooling = MoreObjects.firstNonNull(pooling, DEFAULT_POOLING);
        final int actualBatchSize = MoreObjects.firstNonNull(batchSize, DEFAULT_BATCH_SIZE);
        final int actualFetchSize = MoreObjects.firstNonNull(fetchSize, DEFAULT_FETCH_SIZE);

        // Check parameters
        Preconditions.checkArgument(actualPort > 0 && actualPort < 65536);
        Preconditions.checkArgument(actualBatchSize > 0);
        Preconditions.checkArgument(actualFetchSize > 0);

        // Instantiate the VirtuosoRepository
        if (actualPooling) {
            // Pooling (see http://docs.openlinksw.com/virtuoso/VirtuosoDriverJDBC.html, section
            // 7.4.4.2) doesn't seem to affect performances. We keep this implementation: perhaps
            // things may change with future versions of Virtuoso.
            final VirtuosoConnectionPoolDataSource source = new VirtuosoConnectionPoolDataSource();
            source.setServerName(actualHost);
            source.setPortNumber(actualPort);
            source.setUser(actualUsername);
            source.setPassword(actualPassword);
            this.virtuoso = new VirtuosoRepository((ConnectionPoolDataSource) source,
                    "sesame:nil", true);
        } else {
            final String url = String.format("jdbc:virtuoso://%s:%d", actualHost, actualPort);
            this.virtuoso = new VirtuosoRepository(url, actualUsername, actualPassword,
                    "sesame:nil", true);
        }

        // Further configure the VirtuosoRepository
        this.virtuoso.setBatchSize(actualBatchSize);
        this.virtuoso.setFetchSize(actualFetchSize);

        // Setup marker variables
        this.fileSystem = Preconditions.checkNotNull(fileSystem);
        this.markerPath = new Path(actualMarkerFilename).makeQualified(fileSystem);

        // Log relevant information
        LOGGER.info("VirtuosoTripleStore URL: {}", actualHost + ":" + actualPort);
        LOGGER.info("VirtuosoTripleStore marker: {}", this.markerPath);
    }

    @Override
    public void init() throws IOException {
        try {
            this.virtuoso.initialize(); // looking at Virtuoso code this seems a NOP
        } catch (final RepositoryException ex) {
            throw new IOException("Failed to initialize Virtuoso driver", ex);
        }
    }

    @Override
    public TripleTransaction begin(final boolean readOnly) throws DataCorruptedException,
            IOException {
        // Check if there was an interrupted transaction.
        if (existsTransactionMarker()) {
            throw new DataCorruptedException("The triple store performed a bulk operation "
                    + "that didn't complete successfully.");
        }
        return new VirtuosoTripleTransaction(this, readOnly);
    }

    @Override
    public void reset() throws IOException {
        VirtuosoRepositoryConnection connection = null;
        try {
            connection = (VirtuosoRepositoryConnection) this.virtuoso.getConnection();
            connection.getQuadStoreConnection().prepareCall("RDF_GLOBAL_RESET ()").execute();
        } catch (final RepositoryException ex) {
            throw new IOException("Could not connect to Virtuoso server", ex);
        } catch (final SQLException e) {
            throw new IOException("Something went wrong while invoking stored procedure.", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (final RepositoryException re) {
                    throw new IOException("Error while closing connection.", re);
                }
            }
        }

        final boolean removedTransactionMarker = removeTransactionMarker();
        LOGGER.info("Database reset. Transaction marker removed: " + removedTransactionMarker);
    }

    @Override
    public void close() {
        // no need to terminate pending transactions: this is done externally
        try {
            this.virtuoso.shutDown(); // looking at Virtuoso code this should be a NOP
        } catch (final RepositoryException ex) {
            LOGGER.error("Failed to shutdown Virtuoso driver", ex);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    VirtuosoRepository getVirtuoso() {
        return this.virtuoso;
    }

    /**
     * Checks if the transaction file exists.
     * 
     * @return <code>true</code> if the marker is present, <code>false</code> otherwise.
     */
    boolean existsTransactionMarker() throws IOException {
        // try {
        // return this.fileSystem.exists(this.markerPath)
        // } catch (final IOException ioe) {
        // throw new IOException("Error while checking virtuoso transaction file.", ioe);
        // }
        return false; // TODO disabled so not to depend on HDFS
    }

    /**
     * Adds the transaction file.
     * 
     * @return <code>true</code> if the marker was not present, <code>false</code> otherwise.
     */
    boolean addTransactionMarker() throws IOException {
        // try {
        // return this.fileSystem.createNewFile(this.markerPath);
        // } catch (final IOException ioe) {
        // throw new IOException("Error while adding virtuoso transaction file.", ioe);
        // }
        return false; // TODO disabled so not to depend on HDFS
    }

    /**
     * Removes the transaction file.
     * 
     * @return <code>true</code> if the marker was present, <code>false</code> otherwise.
     */
    boolean removeTransactionMarker() throws IOException {
        // try {
        // return this.fileSystem.delete(this.markerPath, false);
        // } catch (final IOException ioe) {
        // throw new IOException("Error while adding virtuoso transaction file.", ioe);
        // }
        return false; // TODO disabled so not to depend on HDFS
    }

}
