package eu.fbk.knowledgestore.datastore.hbase.utils;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HADOOP_FS_DEFAULT_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_TRAN_LAYER;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_ZOOKEEPER_CLIENT_PORT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_ZOOKEEPER_QUORUM;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TRAN_LAYER_OPT;
import static java.lang.Integer.MAX_VALUE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

/**
 * Class defining all HBase operations.
 */
public abstract class AbstractHBaseUtils {

    /** Logger object used inside HdfsFileStore. */
    public static Logger logger = LoggerFactory.getLogger(AbstractHBaseUtils.class);

    private org.apache.hadoop.conf.Configuration hbcfg;
    
    private AvroSerializer serializer;
    
    private String hbaseTableNamePrefix;

    private boolean serverFilterFlag;

    /**
     * Constructor.
     * @param xmlConf holds all configuration properties.
     */
    public AbstractHBaseUtils(Properties properties) {
        createConfiguration(properties);
    }

    public static AbstractHBaseUtils factoryHBaseUtils(Properties properties) {
        AbstractHBaseUtils hbaseUtils = null;
        if (properties.getProperty(HBASE_TRAN_LAYER, OMID_TRAN_LAYER_OPT).equalsIgnoreCase(
                OMID_TRAN_LAYER_OPT)) {
            logger.info("Using OMID HbaseUtils");
            hbaseUtils = new OmidHBaseUtils(properties);
        } else {
            logger.info("Using Native HbaseUtils");
            hbaseUtils = new HBaseUtils(properties);
        }
        return hbaseUtils;
    }

    /**
     * Begins a transaction.
     */
    public abstract void begin();

    /**
     * Commits operations done.
     * @throws DataCorruptedException in case a rollback has not been successful.
     * @throws IOException in case a commit has not been successful.
     */
    public abstract void commit() throws DataCorruptedException, IOException;

    /**
     * Rollbacks operations done.
     * @throws DataCorruptedException in case a rollback has not been successful.
     * @throws IOException in case a commit has not been successful.
     */
    public abstract void rollback() throws DataCorruptedException, IOException;

    /**
     * Gets a handle of a specific table.
     * @param tableName of the table to be accessed.
     * @return Object of the table found.
     */
    public abstract Object getTable(String tableName);

    /**
     * Process operations on the Resource table.
     * @param record to be put.
     * @param tableName where operations will be performed.
     * @param ops to be performed into tableName.
     * @param conf object to connect to HBase.
     * @param isPut to determine if the operation is put or a delete.
     */
    public abstract void processPut(Record record, String tabName,
            String famName, String quaName);

    /**
     * Process operations on the Resource table.
     * @param record to be deleted.
     * @param tabName where operations will be performed.
     * @param famName where operation will be performed.
     * @param quaName where operation will be performed.
     * @param conf object to connect to HBase.
     */
    public abstract void processDelete(URI id, String tabName,
            String famName, String quaName);

    /**
     * Gets a Record based on information passed.
     * @param tableName to do the get.
     * @param id of the Record needed
     * @throws IOException
     */
    public abstract Record get(String tableName, URI id)
            throws IOException;

    /**
     * Gets a scanner based on the Scan object
     * @param scan to retrieve an HBase scanner
     * @return
     */
    public abstract ResultScanner getScanner(String tableName, Scan scan);

    /**
     * Gets a resource based on information passed.
     * @param tableName table name to get data from
     * @param ids to be retrieved
     * @param conf
     * @return
     * @throws IOException
     */
    public abstract List<Record> get(String tableName, List<URI> ids)
            throws IOException;

    /**
     * Checking for errors after operations have been processed.
     * @param objs
     * @return
     */
    public abstract List<Object> checkForErrors(Object[] objs);

    /**
     * Counts the records having the type and matching the optional condition specified. This
     * method returns the number of matching instances instead of retrieving the corresponding {@code Record}
     * objects.
     * 
     * @param type
     *            the URI of the type of records to return
     * @param condition
     *            an optional condition to be satisfied by matching records; if null, no condition
     *            must be checked
     * @return the number of records matching the optional condition and type specified
     * @throws IOException
     *             in case some IO error occurs
     */
    public abstract long count(String tableName, String familyName, XPath condition)
            throws IOException;

    /**
     * Creates an HBase configuration object.
     * 
     * @param properties the configuration properties
     */
    public void createConfiguration(final Properties properties) {

        setHbcfg(HBaseConfiguration.create());

        getHbcfg().set(HBASE_ZOOKEEPER_QUORUM,
                properties.getProperty(HBASE_ZOOKEEPER_QUORUM, "hlt-services4"));

        getHbcfg().set(HBASE_ZOOKEEPER_CLIENT_PORT,
                properties.getProperty(HBASE_ZOOKEEPER_CLIENT_PORT, "2181"));

        getHbcfg().set(HADOOP_FS_DEFAULT_NAME,
                properties.getProperty(HADOOP_FS_DEFAULT_NAME, "hdfs://hlt-services4:9000"));

        // getHbcfg().set("hbase.client.retries.number", "1");
    }

    /** 
     * Gets filter based on the condition to be performed
     * @param condition To be applied server sided
     * @param passAll boolean if all elements have to pass the test
     * @param famNames to be checked
     * @param qualNames to be checked
     * @param params that could be needed
     * @return FilterList containing all filters needed
     */
    public FilterList getFilter(XPath condition, boolean passAll,
            String []famNames, String []qualNames, String []params) {
        FilterList list = new FilterList((passAll)?FilterList.Operator.MUST_PASS_ALL:
        FilterList.Operator.MUST_PASS_ONE);
        for (int iCont = 0; iCont < famNames.length; iCont ++) {
            SingleColumnValueFilter filterTmp = new SingleColumnValueFilter(
                Bytes.toBytes(famNames[iCont]),
                Bytes.toBytes(qualNames[iCont]),
                CompareOp.EQUAL,
                Bytes.toBytes(params[iCont])
                );
            list.addFilter(filterTmp);
        }
        return list;
    }
 
    /**
     * Creates a scan
     * @param tableName to be scan
     * @param famName to be checked
     * @param startKey to query the table
     * @param endKey to query the table
     * @param conf 
     * @return Scan inside the table
     * @throws IOException
     */
     public Scan getResultScan(String tableName, String famName,
             ByteBuffer startKey, ByteBuffer endKey) throws IOException {
	 logger.debug("AbstractHBaseUtils Begin of getResultScan(" + tableName + ", " + famName + ")");
         Scan scan = new Scan();
         scan.addFamily(Bytes.toBytes(famName));
         // For a range scan, set start / stop id or just start.
         if (startKey != null)
             scan.setStartRow(Bytes.toBytes(startKey));
         if (endKey != null)
             scan.setStopRow(Bytes.toBytes(endKey));
         return scan;
     }

    /**
     * Creates a result scanner
     * @param tableName
     * @param famName
     * @param conf
     * @return
     * @throws IOException
     */
     public Scan getScan(String tableName,
             String famName) throws IOException {
         return getResultScan(tableName, famName, null, null);
     }

    /**
     * Creates puts for HBase
     * @param record
     * @throws IOException
     */
    public Put createPut(Record record, String tableName,
            String famName, String quaName) throws IOException {
        Object tTable = getTable(tableName);
        Put put = null;
        if (tTable != null) {
            // Transforming data model record into an Avro record
            AvroSerializer serializer = getSerializer();
            final byte[] bytes = serializer.toBytes(record);
            // Resource's Key
            put = new Put(Bytes.toBytes(record.getID().toString()));
            // Resource's Value
            put.add(Bytes.toBytes(famName), Bytes.toBytes(quaName), bytes);
        }
        return put;
    }

    /**
     * Creates deletes for HBase tables
     * @param id
     * @param tableName
     * @param conf
     * @return
     * @throws IOException
     */
    public Delete createDelete(URI id, String tableName) throws IOException {
        Delete del = null;
        Object tTable = getTable(tableName);
        if (tTable != null) {
            del = new Delete(Bytes.toBytes(id.toString()));
        }
        return del;
    }

    /**
     * @return the logger
     */
    public static Logger getLogger() {
        return logger;
    }

    /**
     * @param logger the logger to set
     */
    public void setLogger(Logger pLogger) {
        logger = pLogger;
    }

    /**
     * Checks and/or create table with specific column family
     * @param tabName 
     * @param colFamName
     * @throws IOException 
     */
    public void checkAndCreateTable(String tabName, String colFamName) throws IOException {
        HBaseAdmin hba;
        try {
            hba = new HBaseAdmin(this.getHbcfg());
            if (hba.tableExists(tabName) == false) {
                logger.debug("creating table " + tabName);
                final HTableDescriptor tableDescriptor = new HTableDescriptor(tabName);
                final HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFamName);
		columnDescriptor.setMaxVersions(MAX_VALUE);
		tableDescriptor.addFamily(columnDescriptor);
                hba.createTable(tableDescriptor);
            } else {
                logger.debug("already existent table " + tabName);
            }
            hba.close();
        } catch (MasterNotRunningException e) {
            throw new IOException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
    /**
     * @return the hbcfg
     */
    public org.apache.hadoop.conf.Configuration getHbcfg() {
        return hbcfg;
    }
    /**
     * @param hbcfg the hbcfg to set
     */
    public void setHbcfg(org.apache.hadoop.conf.Configuration hbcfg) {
        this.hbcfg = hbcfg;
    }

    public void initServerFilterFlag(boolean serverFilterFlag) {
        this.serverFilterFlag = serverFilterFlag;
    }
   
    /**
     * @return the server filter flag
     */
    public boolean getServerFilterFlag() {
        return serverFilterFlag;
    }

    public void initSerializer(AvroSerializer serializer) {
        this.serializer = serializer;
    }
   
    /**
     * @return the serializer
     */
    public AvroSerializer getSerializer() {
        return serializer;
    }
    
    public void initHbaseTableNamePrefix(String hbaseTableNamePrefix) {
        this.hbaseTableNamePrefix = hbaseTableNamePrefix;
    }
   
    /**
     * @return the hbase table prefix
     */
    public String getHbaseTableNamePrefix() {
        return hbaseTableNamePrefix;
    }

}
