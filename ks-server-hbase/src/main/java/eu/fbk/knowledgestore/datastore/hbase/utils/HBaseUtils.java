package eu.fbk.knowledgestore.datastore.hbase.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;

public class HBaseUtils extends AbstractHBaseUtils {

    /** The map tableName -> table handle */
    private static Map<String, HTable> tableNameHandleMap = new HashMap<String, HTable>();

    public HBaseUtils(final Properties properties) {
        super(properties);
    }

    /**
     * Gets a handle of a specific table.
     * @param tableName of the table to be accessed.
     * @return HTable of the table found.
     */
    @Override
    public HTable getTable(String tableName) {
	logger.debug("NATIVE Begin of getTable for " + tableName);
        HTable table = tableNameHandleMap.get(tableName);
        if (table != null) {
            logger.debug("NATIVE Found a cached handle for table " + tableName);
            return table;
        }
        try {
            logger.debug("NATIVE Looking for a handle of table: " + tableName);
            HBaseAdmin admin = new HBaseAdmin(this.getHbcfg());
            HTableDescriptor[] resources = admin.listTables(tableName);
	    Preconditions.checkElementIndex(0, resources.length, "no table " + tableName + " found");
            admin.close();
            table = new HTable(this.getHbcfg(), tableName);
        } catch (IOException e) {
            logger.error("NATIVE Error while trying to obtain table: " + tableName);
            logger.error(e.getMessage());
        };
        tableNameHandleMap.put(tableName, table);
        logger.debug("NATIVE Cached a handle of table: " + tableName);
        return table;
    }

    /**
     * Commits work done.
     */
    @Override
    public void commit() {
    }

    /**
     * Rollbacks work done.
     */
    @Override
    public void rollback() {
    }

    /**
     * Gets a scanner for a specific table
     * @param tableName to get the scanner from
     * @param scan for the specific table
     * @param conf object to get a hold of an HBase table
     */
    @Override
    public ResultScanner getScanner(String tableName, Scan scan) {
	logger.debug("NATIVE Begin of getScanner(" + tableName + ", " + scan + ")");
        HTable tab = (HTable) getTable(tableName);
        ResultScanner resScanner = null;
        try {
            resScanner = tab.getScanner(scan);
        } catch (IOException e) {
            logger.error("Error while trying to obtain a ResultScanner: " + tableName);
            logger.error(e.getMessage());
        }
        return resScanner;
    }

    /**
     * Process put operations on an HBase table.
     */
    @Override
    public void processPut(Record record, String tabName,
            String famName, String quaName) {
	logger.debug("NATIVE Begin processPut(" + record + ", " + tabName + ")");
        HTable hTable = getTable(tabName);
        try {
            Put op = createPut(record, tabName, famName, quaName);
            hTable.put(op);
        } catch (IOException e) {
            logger.error("Error while attempting to perform operations at HBaseDataTransactions.");
            logger.error(e.getMessage());
        }
    }

    /**
     * Process delete operations on an HBase table.
     */
    @Override
    public void processDelete(URI id, String tabName,
            String famName, String quaName) {
	logger.debug("NATIVE Begin processDelete(" + id + ", " + tabName + ")");
        HTable hTable = getTable(tabName);
        try {
            Delete op = createDelete(id, tabName);
            hTable.delete(op);
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
	logger.debug("NATIVE Begin of get(" + tableName + ", " + id + ")");
        HTable selTable = getTable(tableName);
        Record resGotten = null;
        if (selTable != null) {
           // Resource's Key
           Get get = new Get(Bytes.toBytes(id.toString()));
           Result rs = selTable.get(get);
           logger.debug("Value obtained: " + new String(rs.value()));
           final AvroSerializer serializer = getSerializer();
           resGotten = (Record) serializer.fromBytes(rs.value());
        }
        return resGotten;
    }
 
    @Override
    public List<Record> get(String tableName,
            List<URI> ids) throws IOException {
	logger.debug("NATIVE Begin of get(" + tableName + ", " + ids + ")");
        HTable selTable = getTable(tableName);
        List<Record> resGotten = new ArrayList<Record> ();
        List<Get> gets = new ArrayList<Get> ();
        AvroSerializer serializer = getSerializer();

        for (URI id : ids) {
            gets.add(new Get(Bytes.toBytes(id.toString())));
        }
        Result[] results = selTable.get(gets);

        //TODO check if this is ok
        for (Result res : results) {
            final byte[] bytes = res.value();
            if (bytes != null) {
                resGotten.add((Record) serializer.fromBytes(bytes));
            }
        }
        return resGotten;
    }
 
    /**
     * Creates puts for HBase
     * @param record
     * @throws IOException
     */
    @Override
    public Put createPut(Record record, String tableName,
            String famName, String quaName) throws IOException {
        HTable hTable = getTable(tableName);
        Put put = null;
        if (hTable != null) {
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
     * @param record
     * @param tableName
     * @param conf
     * @return
     * @throws IOException
     */
    @Override
    public Delete createDelete(URI id, String tableName) throws IOException {
        Delete del = null;
        HTable hTable = getTable(tableName);
        if (hTable != null) {
            del = new Delete(Bytes.toBytes(id.toString()));
        }
        return del;
    }

    /**
     * Checking for errors after operations have been processed.
     * @param objs
     * @return
     */
    @Override
    public List<Object> checkForErrors(Object[] objs) {
        List<Object> errors = new ArrayList<Object>();
        if (objs != null) {
            for (int cont = 0; cont < objs.length; cont ++) {
                if (objs[cont] == null) {
                    logger.debug("A operation could not be performed.");
                    errors.add(objs[cont]);
                }
            }
        }
        return errors;
    }

    /**
     * Gets a number of record of tableName matching condition
     * @param tableName the table name
     * @param familyName the family
     * @param condition to match
     * @throws IOException
     */
    @Override
    public long count(String tableName, String familyName, XPath condition) throws IOException {
	logger.debug("NATIVE Begin count");
	// clone the current conf
	org.apache.hadoop.conf.Configuration customConf = new org.apache.hadoop.conf.Configuration(super.getHbcfg());
	// Increase RPC timeout, in case of a slow computation
        customConf.setLong("hbase.rpc.timeout", 600000);
        // Default is 1, set to a higher value for faster scanner.next(..)
        customConf.setLong("hbase.client.scanner.caching", 1000);

	/*
        System.out.println("HBaseUtils begin of |customConf|");
	Configuration.dumpConfiguration(customConf, new PrintWriter(System.out));
	System.out.println("\nHBaseUtils end of |customConf|");
	*/

        AggregationClient agClient = new AggregationClient(customConf);
	long rowCount = 0;
	byte[] tName = Bytes.toBytes(tableName);
	try {
	    Scan scan = getScan(tableName, familyName);
	    rowCount = agClient.rowCount(tName, null, scan);
	} catch (Throwable e) {
            throw new IOException(e.toString());
	}
        return rowCount;
    }

    @Override
    public void begin() {
    }
}
