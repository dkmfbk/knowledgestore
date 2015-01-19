package eu.fbk.knowledgestore.datastore.hbase.utils;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_REGION_MEMSTORE_FLUSH_SIZE;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_REGION_NRESERVATION_BLOCKS;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_REGION_MEMSTORE_FLUSH_SIZE_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_REGION_NRESERVATION_BLOCKS_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TSO_DEFAULT_HOST_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TSO_DEFAULT_PORT_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TSO_HOST;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TSO_PORT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.TransactionManager;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.hbase.HBaseDataStore;
import eu.fbk.knowledgestore.datastore.hbase.HBaseScanIterator;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

/**
 * Implements HBase operations using Yahoo!'s Omid.
 */
public class OmidHBaseUtils extends AbstractHBaseUtils {

    /** Transaction Manager used inside Yahoo!'s OMID. */
    private static TransactionManager tranManager;

    /** Transaction to be started. */
    private Transaction t1;

    /** The map tableName -> table handle */
    private static Map<String, TTable> tableNameHandleMap = new HashMap<String, TTable>();

    /**
     * Constructor.
     * 
     * @param properties
     *            the configuration properties
     */
    public OmidHBaseUtils(final Properties properties) {
        // setting basic configuration inside parent class.
        super(properties);

        getHbcfg().setInt(
                HBASE_REGION_MEMSTORE_FLUSH_SIZE,
                Integer.parseInt(properties.getProperty(HBASE_REGION_MEMSTORE_FLUSH_SIZE, ""
                        + OMID_REGION_MEMSTORE_FLUSH_SIZE_OPT)));

        getHbcfg().setInt(
                HBASE_REGION_NRESERVATION_BLOCKS,
                Integer.parseInt(properties.getProperty(HBASE_REGION_NRESERVATION_BLOCKS, ""
                        + OMID_REGION_NRESERVATION_BLOCKS_OPT)));

        getHbcfg().set(OMID_TSO_HOST,
                properties.getProperty(OMID_TSO_HOST, OMID_TSO_DEFAULT_HOST_OPT));

        getHbcfg().setInt(
                OMID_TSO_PORT,
                Integer.parseInt(properties.getProperty(OMID_TSO_PORT, ""
                        + OMID_TSO_DEFAULT_PORT_OPT)));

        // Creating transaction manager
        try {
            tranManager = new TransactionManager(this.getHbcfg());
        } catch (IOException e) {
            logger.error("Error trying to create a TransactionManager of OMID.");
            logger.error(e.getMessage());
        }
    }

    /**
     * Commits work done.
     */
    @Override
    public void commit() throws DataCorruptedException, IOException, IllegalStateException{
        try {
            tranManager.commit(t1);
        } catch (RollbackException e) {
            rollback();
            throw new IOException("Error trying to commit transaction.", e);
        } catch (TransactionException e) {
            rollback();
            throw new IOException("Error trying to commit transaction.", e);
        }
    }

    /**
     * Rollbacks work done.
     */
    @Override
    public void rollback() throws DataCorruptedException, IOException, IllegalStateException{
        try {
            tranManager.rollback(t1);
        } catch (Exception e) {
            throw new DataCorruptedException("Error trying to rollback a Transaction.", e);
        }
    }

    /**
     * Gets a handle of a specific table.
     * @param tableName of the table to be accessed.
     * @return HTable of the table found.
     */
    @Override
    public Object getTable(String tableName) {
	logger.debug("OMID Begin of getTable for " + tableName);
        TTable table = tableNameHandleMap.get(tableName);
        if (table != null) {
            logger.debug("OMIDE Found a cached handle for table " + tableName);
            return table;
        }
        try {
            table = new TTable(this.getHbcfg(), tableName);
        } catch (IOException e) {
            logger.error("OMID Error trying to get a TransactionTable of OMID.");
            logger.error(e.getMessage());
        }
        tableNameHandleMap.put(tableName, table);
        logger.debug("OMID Cached a handle of table: " + tableName);
        return table;
    }

    @Override
    public void processPut(Record record, String tabName,
            String famName, String quaName) {
	logger.debug("OMID Begin processPut(" + record + ", " + tabName + ")");
        TTable tTable = (TTable) getTable(tabName);
        Put op = null;
        try {
            op = createPut(record, tabName, famName, quaName);
            tTable.put(t1, op);
            //TODO rollback is there is an exception
        } catch (IllegalArgumentException e) {
            //TODO propagate exceptions
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        } catch (IOException e) {
            //TODO propagate exceptions
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void begin() {
        try {
            t1 = tranManager.begin();
        } catch (TransactionException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void processDelete(URI id, String tabName,
            String famName, String quaName) {
	logger.debug("OMID Begin processDelete(" + id + ", " + tabName + ")");
        TTable tTable = (TTable) getTable(tabName);
        Delete op = null;
        try {
            op = createDelete(id, tabName);
            tTable.delete(t1, op);
        } catch (IllegalArgumentException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    /**
     * Gets a Record based on information passed.
     * @param tableName to do the get.
     * @param id of the Record needed
     * @throws IOException
     */
    @Override
    public Record get(String tableName, URI id) throws IOException {
	logger.debug("OMID Begin of get(" + tableName + ", " + id + ")");
        TTable tTable = (TTable) getTable(tableName);
        Record resGotten = null;
        if (tTable != null) {
	    // Resource's Key
	    Get get = new Get(Bytes.toBytes(id.toString())).setMaxVersions(1);
	    Result rs = tTable.get(t1, get);
	    logger.debug("Value obtained: " + new String(rs.value()));
	    final AvroSerializer serializer = getSerializer();
	    resGotten = (Record) serializer.fromBytes(rs.value());
        }
        return resGotten;
    }

    @Override
    public List<Record> get(String tableName,
			    List<URI> ids) throws IOException {
	logger.debug("OMID Begin of get(" + tableName + ", " + ids + ")");
        TTable tTable = (TTable)getTable(tableName);
	List<Record> resGotten = new ArrayList<Record> ();
	List<Get> gets = new ArrayList<Get> ();
	AvroSerializer serializer = getSerializer();

	for (URI id : ids) {
	    gets.add(new Get(Bytes.toBytes(id.toString())));
	}
	// OMID does support the usage of a list of gets
	Result[] results = tTable.get(t1, gets);

	for (Result res : results) {
	    final byte[] bytes = res.value();
	    if (bytes != null) {
		resGotten.add((Record) serializer.fromBytes(bytes));
	    }
	}
	return resGotten;
    }

    @Override
    public List<Object> checkForErrors(Object[] objs) {
        return new ArrayList<Object>();
    }

    /**
     * Gets a number of record of tableName matching condition
     * 
     * @param tableName
     *            to scan
     * @param condition
     *            to match
     * @param scan
     *            to scan
     * @throws IOException
     */
    @Override
    public long count(final String tableName, final String familyName,
            @Nullable final XPath condition) throws IOException {
        logger.debug("OMID Begin count");
        // VERY INEFFICIENT: to be improved!
        final Stream<Record> cur = Stream.create(new HBaseScanIterator(this, tableName,
                familyName, condition, null, getServerFilterFlag()));
        try {
            return cur.count();
        } finally {
            cur.close();
        }
    }

    /**
     * Gets a scanner for a specific table
     * @param tableName to get the scanner from
     * @param scan for the specific table
     * @param conf object to get a hold of an HBase table
     */
    @Override
    public ResultScanner getScanner(String tableName, Scan scan) {
	logger.debug("OMID Begin of getScanner(" + tableName + ", " + scan + ")");
        TTable tTable = (TTable)getTable(tableName);
        ResultScanner resScanner = null;
        try {
            resScanner = tTable.getScanner(t1, scan);
        } catch (IOException e) {
            logger.error("Error while trying to obtain a ResultScanner: " + tableName);
            logger.error(e.getMessage());
        }
        return resScanner;
    }
}
