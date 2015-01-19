package eu.fbk.knowledgestore.datastore.hbase;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HADOOP_FS_DEFAULT_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HADOOP_FS_URL;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASEDATASTORE_TABLEPREFIX_PROP;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_TRAN_LAYER;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.HBASE_ZOOKEEPER_QUORUM;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.NATIVE_TRAN_LAYER_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TRAN_LAYER_OPT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.OMID_TSO_HOST;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.URIDICT_RELATIVEPATH_DEFAULT;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.URIDICT_RELATIVEPATH_PROP;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_QUA_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.datastore.hbase.utils.AbstractHBaseUtils;
import eu.fbk.knowledgestore.datastore.hbase.utils.AvroSerializer;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.runtime.Dictionary;
import eu.fbk.knowledgestore.runtime.Files;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Class for update the timestamp in a table
 */
public class HBaseLowlevelUtilities
{

    private HBaseDataStore ds;

    private HBaseDataTransaction dt;

    /** Logger object */
    private static Logger logger = LoggerFactory.getLogger(HBaseLowlevelUtilities.class);

    private static boolean printCfgFiles = false;

    private static boolean OmidMode = false;

    /** regulate the transaction end:
          if  1 then "commit"     => dataTransaction.end(true);
          if  0 then "rollback"   => dataTransaction.end(false);
          if -1 then do-nothing   => empty code
    **/
    private static int transactionEndMode = 1;

    private static String generalPrefix = null;

    private static String hbaseTableNamePrefix = "";

    private static String masterHost = "";

    /**
     * Constructor.
     */
    public HBaseLowlevelUtilities(boolean readOnly) {
        try {
            final String propertiesFileName = getClass().getSimpleName() + ".properties";
            final URL url = getClass().getResource(propertiesFileName);
            logger.info("url is " + url);
            final InputStream stream = url.openStream();
            final Properties properties = new Properties();
            properties.load(stream);
            stream.close();
            logger.info("read properties from file");

            // check and set the printCfgFiles variable
            printCfgFiles = Boolean.parseBoolean(properties.getProperty("print.cfg.files", ""
                    + printCfgFiles));

	    /* 
	       Override properties from file with those from options
	    */

            // override property HBASE_TRAN_LAYER
            if (OmidMode) {
                properties.setProperty(HBASE_TRAN_LAYER, OMID_TRAN_LAYER_OPT);
            } else {
                properties.setProperty(HBASE_TRAN_LAYER, NATIVE_TRAN_LAYER_OPT);
            }

	    if (! masterHost.equals("")) {
		// override property HBASE_ZOOKEEPER_QUORUM
		properties.setProperty(HBASE_ZOOKEEPER_QUORUM, masterHost);

		// override property HADOOP_FS_URL and HADOOP_FS_DEFAULT_NAME
		properties.setProperty(HADOOP_FS_URL, "hdfs://" + masterHost + ":9000/");
		properties.setProperty(HADOOP_FS_DEFAULT_NAME, properties.getProperty(HADOOP_FS_URL));

		// override property OMID_TSO_HOST
		properties.setProperty(OMID_TSO_HOST, masterHost);
	    }
	    
            // override property HBASEDATASTORE_TABLEPREFIX_PROP
	    if (generalPrefix != null) {
		if (generalPrefix.equals("")) {
		    hbaseTableNamePrefix = "";
		} else {
		    hbaseTableNamePrefix = generalPrefix + ".";
		}
		properties.setProperty(HBASEDATASTORE_TABLEPREFIX_PROP, hbaseTableNamePrefix);

		// override property URIDICT_RELATIVEPATH_PROP
		String uriDictPath;
		if (generalPrefix.equals("")) {
		    uriDictPath = "KnowledgeStore/" + URIDICT_RELATIVEPATH_DEFAULT;
		} else {
		    uriDictPath = "KnowledgeStore." + generalPrefix + "/" + URIDICT_RELATIVEPATH_DEFAULT;
		}	    
		properties.setProperty(URIDICT_RELATIVEPATH_PROP, uriDictPath);
	    }

	    // set local variables 
	    hbaseTableNamePrefix = properties.getProperty(HBASEDATASTORE_TABLEPREFIX_PROP);
	    
            logger.info("transactionEndMode = " + transactionEndMode);

            if (printCfgFiles) {
                System.out.println("\nBEGIN OF |origXmlCfg|");
                System.out.println(Joiner.on("\n").withKeyValueSeparator("=").join(properties));
                System.out.println("END OF |origXmlCfg|\n");
            }

            // create filesystem
            final String fsURL = properties.getProperty("fs.url");
            final Map<String, String> fsProperties = Maps.newHashMap();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                if (entry.getKey().toString().startsWith("fs.")) {
                    fsProperties.put(entry.getKey().toString(), entry.getValue().toString());
                }
            }
            final FileSystem fileSystem = Files.getFileSystem(fsURL, fsProperties);

            // create new DataStore
            ds = new HBaseDataStore(fileSystem, properties);

            final org.apache.hadoop.conf.Configuration hbaseCfg = ds.hbaseCfg;

            if (printCfgFiles) {
                // print the two conf files
                System.out.println("\nBEGIN OF |hbaseCfg|");
                if (hbaseCfg != null) {
                    org.apache.hadoop.conf.Configuration.dumpConfiguration(hbaseCfg,
                            new PrintWriter(System.out));
                } else {
                    System.out.println("hbaseCfg null");
                }
                System.out.println("END OF |hbaseCfg|\n");

                System.out.println("\nBEGIN OF |xmlCfg|");
                System.out.println(Joiner.on("\n").withKeyValueSeparator("=").join(properties));
                System.out.println("END OF |xmlCfg|\n");
            }

            ds.init();

            dt = (HBaseDataTransaction) ds.begin(readOnly);
	    logger.info("created dt = ds.begin(" + readOnly + ")");

	    if (printCfgFiles) {
                final Dictionary<URI> dict = ds.getSerializer().getDictionary();
                final String dictUrl = dict.getDictionaryURL();
                System.out.println("\nDictionary " + dictUrl + " begin");
                for (int i = 1; i < 1000; i++) {
                    try {
                        final URI val = (URI) dict.objectFor(i, true);
                        System.out.println(i + " -> " + val);
                    } catch (Exception e) {
                        break;
                    }
                }
                System.out.println("Dictionary end\n");
            }

            System.out.println("end of " + this.getClass().getSimpleName() + " Constructor");

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void createRecordWithTimestamp(Record record, String tableName,
					   String famName, String quaName, long timestamp) throws IOException 
    {
	AbstractHBaseUtils hbaseUtils = dt.getHbaseUtils();
        HTable hTable = (HTable) hbaseUtils.getTable(tableName);
        Put put = null;
        if (hTable != null) {
            // Transforming data model record into an Avro record
            AvroSerializer serializer = hbaseUtils.getSerializer();
            final byte[] bytes = serializer.toBytes(record);
            // Resource's Key
            put = new Put(Bytes.toBytes(record.getID().toString()));
            // Resource's Value
            put.add(Bytes.toBytes(famName), Bytes.toBytes(quaName), timestamp, bytes);
        }
        hTable.put(put);
    }

    /*
      add a delete with the given timestamp (at the whole row)
    */
    private void deleteRecordWithTimestamp(Record record, String tableName, long timestamp) 
	throws IOException 
    {
	AbstractHBaseUtils hbaseUtils = dt.getHbaseUtils();
        HTable hTable = (HTable) hbaseUtils.getTable(tableName);
        Delete del = null;
        if (hTable != null) {
            // delete the whole row (i.e. all the column families)
            del = new Delete(Bytes.toBytes(record.getID().toString()), timestamp);
        }
        hTable.delete(del);
    }

    private void compactTable(String tableName) 
	throws IOException, InterruptedException
    {
	AbstractHBaseUtils hbaseUtils = dt.getHbaseUtils();
	HBaseAdmin admin = new HBaseAdmin(hbaseUtils.getHbcfg());
        admin.flush(tableName);
        admin.majorCompact(tableName);
	admin.close();
    }

    class TimestampedRecord {
	private Record record;
	private long timestamp;
	// Constructor
	TimestampedRecord(Record r, long t) {
	    this.record = r;
	    this.timestamp = t;
	}
	// get Record field
	public Record getRecord() {
	    return this.record;
	}
	// get timestamp field
	public long getTimestamp() {
	    return this.timestamp;
	}
    }

    private TimestampedRecord getTimestampedRecord(String tableName, String famName, String quaName, String id)
	throws IOException {
	HBaseLowlevelUtilities.TimestampedRecord tr = null;
	AbstractHBaseUtils hbaseUtils = dt.getHbaseUtils();
	HTable hTable = (HTable) hbaseUtils.getTable(tableName);
	if (hTable != null) {
	    Get get = new Get(Bytes.toBytes(id.toString()));
	    Result rs = hTable.get(get);
	    if (rs == null) {
		return null;
	    }
	    final AvroSerializer serializer = hbaseUtils.getSerializer();
	    Record record = (Record) serializer.fromBytes(rs.value());
	    long timestamp = rs.getColumnLatest(Bytes.toBytes(famName), Bytes.toBytes(quaName)).getTimestamp();
	    tr = new TimestampedRecord(record, timestamp);
	}
	return tr;
    }

    private URI getUriTypeFromTablename (String tableName) 
    {
	if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_RES_TAB_NAME)) {
	    return KS.RESOURCE;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_MEN_TAB_NAME)) {
	    return KS.MENTION;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_ENT_TAB_NAME)) {
	    return KS.ENTITY;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_CON_TAB_NAME)) {
	    return KS.CONTEXT;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_USR_TAB_NAME)) {
	    return KS.USER;
	} else {
	    System.out.println("getUriTypeFromTablename: unknown tableName " + tableName);
	    return null;
	}
    }

    private String getFamilyNameFromTablename (String tableName) 
    {
	if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_RES_TAB_NAME)) {
	    return DEFAULT_RES_FAM_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_MEN_TAB_NAME)) {
	    return DEFAULT_MEN_FAM_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_ENT_TAB_NAME)) {
	    return DEFAULT_ENT_FAM_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_CON_TAB_NAME)) {
	    return DEFAULT_CON_FAM_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_USR_TAB_NAME)) {
	    return DEFAULT_USR_FAM_NAME;
	} else {
	    System.out.println("getFamilyNameFromTablename: unknown tableName " + tableName);
	    return null;
	}
    }

    private String getQualifierNameFromTablename (String tableName) 
    {
	if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_RES_TAB_NAME)) {
	    return DEFAULT_RES_QUA_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_MEN_TAB_NAME)) {
	    return DEFAULT_MEN_QUA_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_ENT_TAB_NAME)) {
	    return DEFAULT_ENT_QUA_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_CON_TAB_NAME)) {
	    return DEFAULT_CON_QUA_NAME;
	} else if (tableName.equalsIgnoreCase(hbaseTableNamePrefix + DEFAULT_USR_TAB_NAME)) {
	    return DEFAULT_USR_QUA_NAME;
	} else {
	    System.out.println("getQualifierNameFromTablename: unknown tableName " + tableName);
	    return null;
	}
    }    

    private static void endTransaction (DataTransaction dataTran) throws DataCorruptedException, IOException {
	if (transactionEndMode == 1) {
	    dataTran.end(true);
	    logger.info("doTransactionEnd: dataTran.end(true) [= commit]");
	} else if (transactionEndMode == 0) {
	    dataTran.end(false);
	    logger.info("doTransactionEnd: dataTran.end(false) [= rollback]");
	} else {
	    logger.info("doTransactionEnd: NOTHING [= neither commit nor rollback]");
	}
    }

    // Generate random integer in range 0..(limit-1)");
    public static int getRandomInt(final int limit)
    {
        final Random randomGenerator = new Random();
        final int randomInt = randomGenerator.nextInt(limit);
        return randomInt;
    }

    private static void replicateRowkeysInTableWithTimestamp(final String tableName, long timestamp)
    {
        System.out.println("replicateRowkeysInTableWithTimestamp: tableName " + tableName + ", timestamp " + timestamp);

	/* WARNING: Native mode is required!! */
	OmidMode = false;
	transactionEndMode = 1;

        final HBaseLowlevelUtilities hlu = new HBaseLowlevelUtilities(false);
        if (hlu.ds == null) {
            return;
        }

	URI type = hlu.getUriTypeFromTablename(tableName);
	String famName = hlu.getFamilyNameFromTablename(tableName);
	String quaName = hlu. getQualifierNameFromTablename(tableName);
	if ((type == null) || (famName == null) || (quaName == null)) {
	    return;
	}

        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;

	int numRecord = 0;
        try {
	    DataTransaction dataTran = hlu.dt;
            time1 = System.currentTimeMillis();
            Stream<Record> cur = dataTran.retrieve(type, null, null);
            try {
                time2 = System.currentTimeMillis();
                for (Record r : cur) {
                    hlu.createRecordWithTimestamp(r, tableName, famName, quaName, timestamp);
                    numRecord++;
                }
                time3 = System.currentTimeMillis();
                endTransaction(dataTran);
                time4 = System.currentTimeMillis();
            } finally {
                cur.close();
            }
	    
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Processed " + numRecord + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
    }


   private static void addDeleteWithTimestampToAllRowkeysInTable(final String tableName, long timestamp)
    {
        System.out.println("addDeleteWithTimestampToAllRowkeysInTable: tableName " + tableName + ", timestamp " + timestamp);

	/* WARNING: Native mode is required!! */
	OmidMode = false;
	transactionEndMode = 1;

        final HBaseLowlevelUtilities hlu = new HBaseLowlevelUtilities(false);
        if (hlu.ds == null) {
            return;
        }

	URI type = hlu.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}

        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;

	int numRecord = 0;
        try {
	    DataTransaction dataTran = hlu.dt;

            time1 = System.currentTimeMillis();
            Stream<Record> cur = dataTran.retrieve(type, null, null);
            try {
                time2 = System.currentTimeMillis();
                for (Record r : cur) {
                    hlu.deleteRecordWithTimestamp(r, tableName, timestamp);
                    numRecord++;
                }
                time3 = System.currentTimeMillis();
                endTransaction(dataTran);
                time4 = System.currentTimeMillis();
            } finally {
                cur.close();
            }

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Processed " + numRecord + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
    }

    private static void addDeleteAndReplicateAllRowkeysInTable(final String tableName, long timestamp1, long timestamp2)
    {
        System.out.println("addDeleteAndReplicateAllRowkeysInTable: tableName " + tableName + ", timestamp1 " + timestamp1 + ", timestamp2 " + timestamp2);

	/* WARNING: Native mode is required!! */
	OmidMode = false;
	transactionEndMode = 1;

        final HBaseLowlevelUtilities hlu = new HBaseLowlevelUtilities(false);
        if (hlu.ds == null) {
            return;
        }

	URI type = hlu.getUriTypeFromTablename(tableName);
	String famName = hlu.getFamilyNameFromTablename(tableName);
	String quaName = hlu. getQualifierNameFromTablename(tableName);
	if ((type == null) || (famName == null) || (quaName == null)) {
	    return;
	}

        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;

        int numRecord = 0;
        try {
            DataTransaction dataTran = hlu.dt;

            time1 = System.currentTimeMillis();
            Stream<Record> cur = dataTran.retrieve(type, null, null);
            try {
                time2 = System.currentTimeMillis();
                for (Record r : cur) {
                    hlu.deleteRecordWithTimestamp(r, tableName, timestamp1);
                    hlu.createRecordWithTimestamp(r, tableName, famName, quaName, timestamp2);
                    numRecord++;
                }
                time3 = System.currentTimeMillis();
                endTransaction(dataTran);
                time4 = System.currentTimeMillis();
            } finally {
                cur.close();
            }

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Processed " + numRecord + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
    }

    private static void printIdContentWithTimestamp(final String tableName, final String id)
    {
        System.out.println("printIdContentWithTimestamp: tableName " + tableName + ", id " + id);

	/* WARNING: Native mode is required!! */
	OmidMode = false;
	transactionEndMode = 1;

        final HBaseLowlevelUtilities hlu = new HBaseLowlevelUtilities(true);
        if (hlu.ds == null) {
            return;
        }

	String famName = hlu.getFamilyNameFromTablename(tableName);
	String quaName = hlu. getQualifierNameFromTablename(tableName);
	if ((famName == null) || (quaName == null)) {
	    return;
	}

        long time1 = 0, time2 = 0, time3 = 0;

	HBaseLowlevelUtilities.TimestampedRecord tRecord = null;
        try {
	    DataTransaction dataTran = hlu.dt;
	    
	    time1 = System.currentTimeMillis();
	    tRecord = hlu.getTimestampedRecord(tableName, famName, quaName, id);
            time2 = System.currentTimeMillis();
            endTransaction(dataTran);
            time3 = System.currentTimeMillis();

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

	Record record  = tRecord.getRecord();
	long timestamp = tRecord.getTimestamp();
	
	String str = record.toString(Data.getNamespaceMap(), true);
	System.out.println("Found:\n" + str + "\nwith timestamp " + timestamp);
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
    }

    private static void omidize(final String tableName)
    {
        System.out.println("omidize: tableName " + tableName);

	HBaseLowlevelUtilities hlu;
	DataTransaction dataTran;
	Record r;
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0;


	// a) create a new temporary entry in OMID mode
	//
	OmidMode = true;
	hlu = new HBaseLowlevelUtilities(false);

	URI type = hlu.getUriTypeFromTablename(tableName);
	String famName = hlu.getFamilyNameFromTablename(tableName);
	String quaName = hlu. getQualifierNameFromTablename(tableName);
	if ((type == null) || (famName == null) || (quaName == null)) {
	    return;
	}

	transactionEndMode = 1;
	time1 = System.currentTimeMillis();

	try {
	    dataTran = hlu.dt;
	    r = Record.create();
	    r.set(RDF.TYPE, type);
	    URI id = new URIImpl("rol:///tmp_omidize_" + Integer.toString(getRandomInt(6666)));
	    r.setID(id);
	    dataTran.store(type, r);
	    endTransaction(dataTran);
	    logger.info("new temporary record created with id " + id);
	    time2 = System.currentTimeMillis();

	    // b) get the timestamp of the new entry as a OMID VCTS
	    //
	    OmidMode = false; // WARNING: Native mode is required!
	    hlu = new HBaseLowlevelUtilities(true);
	    dataTran = hlu.dt;
	    TimestampedRecord tr = hlu.getTimestampedRecord(tableName, famName, quaName, id.toString());
	    long VCTS = tr.getTimestamp();
	    endTransaction(dataTran);
	    r = tr.getRecord();
	    logger.info("VCTS for " + r.toString() + " is " + VCTS);
	    time3 = System.currentTimeMillis();
	
	    // c) fix all the entries in the table with such VCTS 
	    //
	    addDeleteAndReplicateAllRowkeysInTable(tableName,VCTS-1,VCTS);
	    System.out.println("fixed all entries in table " + tableName);
	    time4 = System.currentTimeMillis();
	
	    // d) + e) delete the temporary entry and compact the table
	    //
	    OmidMode = false; // WARNING: Native mode is required!
	    hlu = new HBaseLowlevelUtilities(false);
	    dataTran = hlu.dt;
	    r = Record.create();
	    r.set(RDF.TYPE, type);
	    r.setID(id);
	    hlu.deleteRecordWithTimestamp(r, tableName, VCTS);
	    time5 = System.currentTimeMillis();
	    hlu.compactTable(tableName);
	    endTransaction(dataTran);
	    logger.info("deleted new temporary record with id " + id + " and compacted table");
	    time6 = System.currentTimeMillis();

	} catch (final Exception e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
	}

        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
        System.out.println("time 5-4: " + String.valueOf(time5 - time4) + " ms");
        System.out.println("time 6-5: " + String.valueOf(time6 - time5) + " ms");
    }

    private static void printStatisticsOfTimestampsOfTableEntries(final String tableName)
    {
        System.out.println("printStatisticsOfTimestampsOfTableEntries: tableName " + tableName);

	/* WARNING: Native mode is required!! */
	OmidMode = false;
	transactionEndMode = 1;

        final HBaseLowlevelUtilities hlu = new HBaseLowlevelUtilities(false);
        if (hlu.ds == null) {
            return;
        }

	URI type = hlu.getUriTypeFromTablename(tableName);
	String famName = hlu.getFamilyNameFromTablename(tableName);
	String quaName = hlu. getQualifierNameFromTablename(tableName);
	if ((type == null) || (famName == null) || (quaName == null)) {
	    return;
	}

        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;

        int numRecord = 0;
	Map<Long, Integer> tsMap = new HashMap<Long, Integer>();
        try {
            time1 = System.currentTimeMillis();

	    Scan scan = hlu.ds.getHbaseUtils().getScan(tableName, famName);
	    ResultScanner scanner = hlu.ds.getHbaseUtils().getScanner(tableName, scan);
	    long ts;
	    Long tsL;

	    Result r = scanner.next();
            while (r != null) {
		numRecord++;
		ts = r.getColumnLatest(Bytes.toBytes(famName), Bytes.toBytes(quaName)).getTimestamp();
		tsL = new Long(ts);
                // increment the occurrence of attribute value
                int occ = 1;
                if (tsMap.containsKey(tsL)) {occ = tsMap.get(tsL).intValue() + 1;}
		tsMap.put(tsL, new Integer(occ));
		r = scanner.next();
	    }
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }
	time2 = System.currentTimeMillis();

	int tot = 0;
	StringBuffer str = new StringBuffer();
	for (Map.Entry<Long, Integer> entry : tsMap.entrySet()) {
            Long key = entry.getKey();
	    Integer value = entry.getValue();
            tot += value.intValue();
            str.append(key.toString() + " -> " + value.toString() + "\n");
	}
	time3 = System.currentTimeMillis();
        System.out.println("tsMap:\n" + str);
	time4 = System.currentTimeMillis();
	
        System.out.println("numRecord " + numRecord + ", tot " + tot + ", size " + tsMap.size());
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
    }

    private static void printUsage(Options options) {
        int WIDTH = 80;
        final PrintWriter out = new PrintWriter(System.out);
        final HelpFormatter formatter = new HelpFormatter();
	// String fullClassName = Thread.currentThread().getStackTrace()[1].getClassName();
	// String className = fullClassName.split("\\.")[fullClassName.split("\\.").length - 1];
	String className = "<thisClass>";
	String cmdLineSyntax = className + " [options] cmd table [args]";
	String header = "";
        String footer = "where:\n"
	    + "1 TABLE TIMESTAMP: replicate each entry with timestamp TIMESTAMP\n"
	    + "2 TABLE TIMESTAMP: for each entry add a delete entry with timestamp TIMESTAMP\n"
	    + "3 TABLE TIMESTAMP1 TIMESTAMP2: for each entry add a delete entry with TIMESTAMP1 and replicate it with TIMESTAMP2\n"
	    + "4 TABLE ID: print content and most recent timestamp of ID (= entry/rowkey)\n"
	    + "5 TABLE: 'OMIDize' all the entries\n"
	    + "6 TABLE: print statistics about the entry timestamps\n";

        formatter.printHelp(out, WIDTH, cmdLineSyntax, header, options, 2, 2, footer);
        out.flush();

        System.exit(1);
    }

    public static void main(final String[] args) throws Throwable
    {
	final Options options = new Options();
        options.addOption("cfg", "config_print",          false, "print configuration settings");
        options.addOption("h", "help",                    false, "print help and exit");
        options.addOption("m", "master_host",            true, "the host running hdfs master, zookeeper and omid daemon ");
	options.addOption("p", "prefix",                 true, "the prefix of the tables and FS");

	CommandLine cl = new GnuParser().parse(options, args);
	if (cl.hasOption("h")) {
	    printUsage(options);
	}
	if (cl.hasOption("cfg")) {
	    printCfgFiles = true;
	}
	if (cl.hasOption("m")) {
	    masterHost = cl.getOptionValue("m");
	}
	if (cl.hasOption("p")) {
	    generalPrefix = cl.getOptionValue("p");
	}

	String[] leftArgs = cl.getArgs();
	if (leftArgs.length < 2) {
	    if (leftArgs.length == 0) {
		System.err.println("error: missing cmd");
	    } else {
		System.err.println("error: missing table");
	    }
	    printUsage(options);
	}

        int cmd = 0;
	cmd = Integer.parseInt(leftArgs[0]);
	String tableName = leftArgs[1];

	if (printCfgFiles) {
	    System.out.println("CommandLine Options");
	    for (Option o : cl.getOptions()) {
		System.out.println(" " + o.getOpt() + " -> " + o.getValue());
	    }
	    System.out.println("Args");
	    for (String arg : leftArgs) {
		System.out.println(" " + arg);
	    }
	    System.out.println("");
	}
	

        switch (cmd) {

        case 1:
	    // replicate all the rowkeys in given table with the given timestamp
            if (leftArgs.length < 3) {
                printUsage(options);
            }
            {
                long timestamp = 0;
		try {
		    timestamp = Long.parseLong(leftArgs[2]);
		} catch (final NumberFormatException e) {
		    System.err.println("error in parsing long for timestamp");
		    printUsage(options);
		}
                replicateRowkeysInTableWithTimestamp(tableName, timestamp);
            }
            break;

        case 2:
	    // add a delete entry with the given timestamp for all the rowkeys in given table
            if (leftArgs.length < 3) {
                printUsage(options);
            }
            {
                long timestamp = 0;
		try {
		    timestamp = Long.parseLong(leftArgs[2]);
		} catch (final NumberFormatException e) {
		    System.err.println("error in parsing long for timestamp");
		    printUsage(options);
		}
                addDeleteWithTimestampToAllRowkeysInTable(tableName, timestamp);
            }
            break;

        case 3:
	    // for ech rowkey in given table: 
	    //   a) add a delete entry with the given timestamp1 and 
	    //   b) replicate its content with the given timestamp2
            if (leftArgs.length < 4) {
                printUsage(options);
            }
            {
                long timestamp1 = 0;
                long timestamp2 = 0;
		try {
		    timestamp1 = Long.parseLong(leftArgs[2]);
		    timestamp2 = Long.parseLong(leftArgs[3]);
		} catch (final NumberFormatException e) {
		    System.err.println("error in parsing long for timestamp1 and timestamp2");
		    printUsage(options);
		}
                addDeleteAndReplicateAllRowkeysInTable(tableName, timestamp1, timestamp2);
            }
            break;

        case 4:
	    // print the content and most recent timestamp of given id in the given table
            if (leftArgs.length < 3) {
                printUsage(options);
            }
            {
                String id = new String(leftArgs[2]);
                printIdContentWithTimestamp(tableName, id);
            }
            break;

        case 5:
	    // OMIDize the given table
            {
                omidize(tableName);
            }
            break;

        case 6:
	    // print statistcs of timestamps of table entries
            {
                printStatisticsOfTimestampsOfTableEntries(tableName);
            }
            break;

        default:
            printUsage(options);
        }
        System.exit(0);
    }

}
