package eu.fbk.knowledgestore.datastore.hbase.utils;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_TAB_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import com.continuuity.tephra.TransactionContext;
import com.continuuity.tephra.TransactionFailureException;
import com.continuuity.tephra.TransactionSystemClient;
import com.continuuity.tephra.hbase.TransactionAwareHTable;
import com.continuuity.tephra.inmemory.InMemoryTxSystemClient;
import com.continuuity.tephra.inmemory.InMemoryTransactionManager;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.hbase.HBaseScanIterator;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;

/**
 * Implements HBase operations using Continuuity Tephra.
 */
public class TephraHBaseUtils extends AbstractHBaseUtils
{

    /** Transaction client managed by Tephra */
    private TransactionSystemClient txClient;

    /** Transaction context managed by Tephra */
    private final TransactionContext txContext;

    /** The map tableName -> table handle */
    private static Map<String, TransactionAwareHTable> tableNameHandleMap = new HashMap<String, TransactionAwareHTable>();

    /**
     * Constructor.
     * 
     * @param properties
     *            the configuration properties
     */
    public TephraHBaseUtils(final Properties properties)
    {
        // setting basic configuration inside parent class.
        super(properties);

        final String TEPHRA_HOST = "data.tx.bind.address";
        final String TEPHRA_PORT = "data.tx.bind.port";

        getHbcfg().set(TEPHRA_HOST, "escher");
        getHbcfg().setInt(TEPHRA_PORT, 15165);

        // create and cache all the 5 TransactionAwareHTable tables (one for each HBase table)
        final List<TransactionAwareHTable> txTabList = new ArrayList<TransactionAwareHTable>(5);
        try {
            logger.debug("TEPHRA create the 5 TransactionAwareHTable tables");
            final List<String> tableNames = Arrays.asList(DEFAULT_RES_TAB_NAME,
                    DEFAULT_MEN_TAB_NAME, DEFAULT_ENT_TAB_NAME, DEFAULT_CON_TAB_NAME,
                    DEFAULT_USR_TAB_NAME);
            HTable hTab;
            TransactionAwareHTable txTab;
            for (final String tableName : tableNames) {
                hTab = new HTable(this.getHbcfg(), tableName);
                txTab = new TransactionAwareHTable(hTab);
                tableNameHandleMap.put(tableName, txTab);
                txTabList.add(txTab);
                logger.debug("TEPHRA Cached a handle of table: " + tableName);
            }
        } catch (final IOException e) {
            logger.error("TEPHRA Error while creating 5 TransactionAwareHTable tables");
            logger.error(e.getMessage());
        }


        // Creating transaction client and context
	this.txClient = new InMemoryTxSystemClient(new InMemoryTransactionManager(this.getHbcfg()));
        this.txContext = new TransactionContext(txClient,
						txTabList.toArray(new TransactionAwareHTable[0]));
    }

    /**
     * Commits work done.
     */
    @Override
    public void commit() throws DataCorruptedException, IOException, IllegalStateException
    {
        try {
            this.txContext.finish();
        } catch (final TransactionFailureException tfe1) {
            try {
                this.txContext.abort();
            } catch (final TransactionFailureException tfe2) {
            }
            throw new IOException("Error trying to commit transaction.", tfe1);
        }
    }

    /**
     * Rollbacks work done.
     */
    @Override
    public void rollback() throws DataCorruptedException, IOException, IllegalStateException
    {
        try {
            this.txContext.abort();
        } catch (final Exception e) {
            throw new DataCorruptedException("Error trying to rollback a Transaction.", e);
        }
    }

    /**
     * Gets a handle of a specific table.
     * 
     * @param tableName
     *            of the table to be accessed.
     * @return HTable of the table found.
     */
    @Override
    public Object getTable(final String tableName)
    {
        logger.debug("TEPHRA Begin of getTable for " + tableName);
        final TransactionAwareHTable table = tableNameHandleMap.get(tableName);
        if (table == null) {
            // TODO propagate exceptions
            logger.error("TEPHRA error: unebale to find cached table " + tableName);
            return table;
        } else {
            logger.debug("TEPHRA Found a cached handle for table " + tableName);
            return table;
        }
    }

    @Override
    public void processPut(final Record record, final String tabName, final String famName,
            final String quaName)
    {
        logger.debug("TEPHRA Begin processPut(" + record + ", " + tabName + ")");
        final TransactionAwareHTable txTable = (TransactionAwareHTable) getTable(tabName);
        Put op = null;
        try {
            op = createPut(record, tabName, famName, quaName);
            txTable.put(op);
            // TODO rollback if there is an exception
        } catch (final IllegalArgumentException e) {
            // TODO propagate exceptions
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        } catch (final IOException e) {
            // TODO propagate exceptions
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void begin()
    {
        try {
            this.txContext.start();
        } catch (final TransactionFailureException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void processDelete(final URI id, final String tabName, final String famName,
            final String quaName)
    {
        logger.debug("TEPHRA Begin processDelete(" + id + ", " + tabName + ")");
        final TransactionAwareHTable txTable = (TransactionAwareHTable) getTable(tabName);
        Delete op = null;
        try {
            op = createDelete(id, tabName);
            txTable.delete(op);
        } catch (final IllegalArgumentException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        } catch (final IOException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    /**
     * Gets a Record based on information passed.
     * 
     * @param tableName
     *            to do the get.
     * @param id
     *            of the Record needed
     * @throws IOException
     */
    @Override
    public Record get(final String tableName, final URI id) throws IOException
    {
        logger.debug("TEPHRA Begin of get(" + tableName + ", " + id + ")");
        final TransactionAwareHTable txTable = (TransactionAwareHTable) getTable(tableName);
        Record resGotten = null;
        if (txTable != null) {
            // Resource's Key
            final Get get = new Get(Bytes.toBytes(id.toString())).setMaxVersions(1);
            final Result rs = txTable.get(get);
            logger.debug("Value obtained: " + new String(rs.value()));
            final AvroSerializer serializer = getSerializer();
            resGotten = (Record) serializer.fromBytes(rs.value());
        }
        return resGotten;
    }

    @Override
    public List<Record> get(final String tableName, final List<URI> ids) throws IOException
    {
        logger.debug("TEPHRA Begin of get(" + tableName + ", " + ids + ")");
        final TransactionAwareHTable txTable = (TransactionAwareHTable) getTable(tableName);
        final List<Record> resGotten = new ArrayList<Record>();
        final List<Get> gets = new ArrayList<Get>();
        final AvroSerializer serializer = getSerializer();

        for (final URI id : ids) {
            gets.add(new Get(Bytes.toBytes(id.toString())));
        }
        // OMID does support the usage of a list of gets
        final Result[] results = txTable.get(gets);

        for (final Result res : results) {
            final byte[] bytes = res.value();
            if (bytes != null) {
                resGotten.add((Record) serializer.fromBytes(bytes));
            }
        }
        return resGotten;
    }

    @Override
    public List<Object> checkForErrors(final Object[] objs)
    {
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
            @Nullable final XPath condition) throws IOException
    {
        logger.debug("TEPHRA Begin count");
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
     * 
     * @param tableName
     *            to get the scanner from
     * @param scan
     *            for the specific table
     * @param conf
     *            object to get a hold of an HBase table
     */
    @Override
    public ResultScanner getScanner(final String tableName, final Scan scan)
    {
        logger.debug("TEPHRA Begin of getScanner(" + tableName + ", " + scan + ")");
        final TransactionAwareHTable txTable = (TransactionAwareHTable) getTable(tableName);
        ResultScanner resScanner = null;
        try {
            resScanner = txTable.getScanner(scan);
        } catch (final IOException e) {
            logger.error("Error while trying to obtain a ResultScanner: " + tableName);
            logger.error(e.getMessage());
        }
        return resScanner;
    }
}
