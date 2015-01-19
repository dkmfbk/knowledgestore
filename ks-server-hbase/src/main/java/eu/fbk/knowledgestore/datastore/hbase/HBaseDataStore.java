package eu.fbk.knowledgestore.datastore.hbase;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASEDATASTORE_TABLEPREFIX_PROP;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASEDATASTORE_TABLEPREFIX_DEFAULT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASEDATASTORE_SERVERFILTERFLAG;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.URIDICT_RELATIVEPATH_DEFAULT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.URIDICT_RELATIVEPATH_PROP;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.datastore.DataStore;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.datastore.hbase.utils.AbstractHBaseUtils;
import eu.fbk.knowledgestore.datastore.hbase.utils.AvroSerializer;
import eu.fbk.knowledgestore.runtime.Dictionary;

/**
 * HBaseDataStore used to read and write data into HBase specific tables e.g. Resources, Mentions,
 * Entities.
 */
public class HBaseDataStore implements DataStore {

    /** Hadoop Configuration object. */
    public org.apache.hadoop.conf.Configuration hbaseCfg;

    /** Logger object used inside HdfsFileStore. */
    private static Logger logger = LoggerFactory.getLogger(HBaseDataStore.class);

    /** flag to set where to perform filtering (default is on server-side) */
    private boolean serverFilterFlag;
        
    /** Serializer used to transform record to byte arrays and back. */
    private AvroSerializer serializer;

    /** hbase utilities hiding the modality (OMID or NATIVE) */
    private AbstractHBaseUtils hbaseUtils;

    /** the FileSystem utilized to store additional data (e.g. the URI dictionary) */
    private final FileSystem fileSystem;

    /** the prefix of table names */
    private final String hbaseTableNamePrefix;

    /**
     * Constructor.
     * 
     * @param fileSystem
     *            the file system where to store the dictionary
     * @param properties
     *            the configuration properties
     */
    public HBaseDataStore(final FileSystem fileSystem, final Properties properties) {

        this.hbaseUtils = AbstractHBaseUtils.factoryHBaseUtils(properties);
        this.hbaseCfg = this.hbaseUtils.getHbcfg();

        // set serverFilterFlag
        this.serverFilterFlag = Boolean.parseBoolean(properties.getProperty(HBASEDATASTORE_SERVERFILTERFLAG, 
									    "true"));

        // set the prefix of table names
        this.hbaseTableNamePrefix = properties.getProperty(HBASEDATASTORE_TABLEPREFIX_PROP,
							   HBASEDATASTORE_TABLEPREFIX_DEFAULT);
	
        // set fileSystem
        this.fileSystem = fileSystem;

        try {
            // check if the htables exist: create them if necessary
            checkAndCreateTable(hbaseTableNamePrefix + DEFAULT_RES_TAB_NAME, DEFAULT_RES_FAM_NAME);
            checkAndCreateTable(hbaseTableNamePrefix + DEFAULT_MEN_TAB_NAME, DEFAULT_MEN_FAM_NAME);
            checkAndCreateTable(hbaseTableNamePrefix + DEFAULT_ENT_TAB_NAME, DEFAULT_ENT_FAM_NAME);
            checkAndCreateTable(hbaseTableNamePrefix + DEFAULT_CON_TAB_NAME, DEFAULT_CON_FAM_NAME);
            checkAndCreateTable(hbaseTableNamePrefix + DEFAULT_USR_TAB_NAME, DEFAULT_USR_FAM_NAME);

            // set Avro serializer
            final String dictRelPathString = properties.getProperty(URIDICT_RELATIVEPATH_PROP,
                    URIDICT_RELATIVEPATH_DEFAULT);
            final Path dictFullPath = new Path(dictRelPathString).makeQualified(fileSystem);
            this.serializer = new AvroSerializer(new Dictionary<URI>(URI.class,
                    dictFullPath.toString()));

        } catch (final IOException e) {
            logger.error("Error while creating a new HBaseStore.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void init() throws IOException {
    }

    @Override
    public DataTransaction begin(final boolean readOnly) throws IOException
    {
        logger.debug("Starting transaction in HBaseStore.");
        hbaseUtils.initSerializer(this.serializer);
        hbaseUtils.initServerFilterFlag(this.serverFilterFlag);
        hbaseUtils.initHbaseTableNamePrefix(this.hbaseTableNamePrefix);
        return new HBaseDataTransaction(hbaseUtils);
    }

    @Override
    public void close()
    {
        logger.debug("Finishing transaction in HBaseStore.");
    }

    /**
     * Verifies the existence of tables.
     * @param tableName to be verified
     * @param columnFamilyName of the table
     * @throws IOException
     */
    private void checkAndCreateTable(final String tabName, final String colFamName)
            throws IOException
    {
        hbaseUtils.checkAndCreateTable(tabName, colFamName);
    }

    /**
     * @return the logger
     */
    public static Logger getLogger()
    {
        return logger;
    }

    /**
     * @param logger to be set
     */
    public static void setLogger(final Logger logger)
    {
        HBaseDataStore.logger = logger;
    }

    /**
     * @return the value of serverFilterFlag
     */
    public boolean getServerFilterFlag()
    {
        return serverFilterFlag;
    }

    /**
     * @return the serializer
     */
    public AvroSerializer getSerializer()
    {
        return this.serializer;
    }

    /**
     * @return the hbaseUtils
     */
    public AbstractHBaseUtils getHbaseUtils()
    {
        return this.hbaseUtils;
    }

    /**
     * @return the value of hbaseTableNamePrefix;
     */
    public String getHbaseTableNamePrefix()
    {
        return this.hbaseTableNamePrefix;
    }

}
