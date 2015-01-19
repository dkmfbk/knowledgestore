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

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_TAB_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.datastore.hbase.exception.DataTransactionBlockingException;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.runtime.Dictionary;
import eu.fbk.knowledgestore.runtime.Files;
import eu.fbk.knowledgestore.vocabulary.KS;

/**
 * Class for testing HBaseDataTransaction
 */
public class HBaseDataTransactionTester
{

    private HBaseDataStore ds;

    private DataTransaction dt;

    /** Logger object */
    private static Logger logger = LoggerFactory.getLogger(HBaseDataTransactionTester.class);

    private static boolean printCfgFiles = false;

    private static boolean OmidMode = true;

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
    public HBaseDataTransactionTester(boolean readOnly) {
        try {
            final String propertiesFileName = getClass().getSimpleName() + ".properties";
            final URL url = getClass().getResource(propertiesFileName);
            logger.info("url is " + url);
            final InputStream stream = url.openStream();
            final Properties properties = new Properties();
            properties.load(stream);
            stream.close();
            logger.info("read properties from file");


	    /* 
	       Override properties from file with those from options
	    */

            // override property HBASE_TRAN_LAYER
            if (OmidMode) {
                properties.setProperty(HBASE_TRAN_LAYER, OMID_TRAN_LAYER_OPT);
            } else {
                properties.setProperty(HBASE_TRAN_LAYER, NATIVE_TRAN_LAYER_OPT);
            }

            // override property "transaction.end.mode" (specific to this class)
	    properties.setProperty("transaction.end.mode", Integer.toString(transactionEndMode));

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
	    
            if (printCfgFiles) {
                System.out.println("\nBEGIN OF |origXmlCfg|");
                System.out.println(Joiner.on("\n").withKeyValueSeparator("=").join(properties));
                System.out.println("END OF |origXmlCfg|\n");
            }

            // create filesystem
            final String fsURL = properties.getProperty(HADOOP_FS_URL);
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

            dt = ds.begin(readOnly);
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

    // Generate random integerin range 0..(limit-1)");
    public static int getRandomInt(final int limit)
    {
        final Random randomGenerator = new Random();
        final int randomInt = randomGenerator.nextInt(limit);
        return randomInt;
    }

    private static String recordToString(final Record r)
    {
	String str = new String(r.toString(Data.getNamespaceMap(), true));
	return str;
    }

    private static Record createRecordWithRandomValue(final URI type, final int limit, final int id)
    {
        final URI uriId = new URIImpl("http://rolexample.org/" + id);
        final Record r = Record.create();
        r.setID(uriId);
        r.set(RDF.TYPE, type);
        final String value = "comment # " + Integer.toString(getRandomInt(limit));
        r.set(RDFS.COMMENT, value);
        return r;
    }

    private static void printOccurrenceMap(final Map<String, Integer> occurrenceMap)
    {
        int tot = 0;
        System.out.println("Print occurrenceMap");
        for (final Map.Entry<String, Integer> entry : occurrenceMap.entrySet()) {
            final String key = entry.getKey();
            final Integer value = entry.getValue();
            tot += value.intValue();
            System.out.println("  occurrenceMap value: " + key + " -> " + value.toString());
        }
        System.out.println("found tot occurrenceMap values " + Integer.toString(tot));
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
	    System.out.println("unknown tableName " + tableName);
	    return null;
	}
    }


    private static void populateTableRandomly(String tableName, final int num, final int startId, int tem)
            throws Throwable
    {
        System.out.println("populateTableRandomly: tableName " + tableName + ", num " + num + ", startId " + startId + ", tem " + tem);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(false);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
	long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0;
        time1 = System.currentTimeMillis();
        final int randomLimit = 10;
        final List<Record> recordList = new ArrayList<Record>();
        for (int i = 0; i < num; i++) {
            final Record r = createRecordWithRandomValue(type, randomLimit, startId + i);
            recordList.add(r);
        }
        time2 = System.currentTimeMillis();

        if (num > 0) {
            // collect statistic of randomly generated value of the "RDFS.COMMENT" attribute of
            // records
            final Map<String, Integer> occurrenceMap = new HashMap<String, Integer>();
            for (int i = 0; i < num; i++) {
                final Record r = recordList.get(i);

                // the id:
                // final String id = r.getID().toString();

                // the value of property/attribute RDFS.COMMENT:
                final String val = r.getUnique(RDFS.COMMENT, String.class);

                // increment the occurrence of attribute value
                int occ = 1;
                if (occurrenceMap.containsKey(val)) {
                    occ = occurrenceMap.get(val).intValue() + 1;
                }
                occurrenceMap.put(val, new Integer(occ));
            }
            // print the occurrenceMap
            printOccurrenceMap(occurrenceMap);
        }
        time3 = System.currentTimeMillis();

        String msg = "";
        DataTransaction dataTran = dtt.dt;

	transactionEndMode = tem;
        try {
            for (final Record r : recordList) {
                dataTran.store(type, r);
            }
            time4 = System.currentTimeMillis();
	    // logger.info("sleep(10000)"); Thread.sleep(10000);
	    endTransaction(dataTran);
            dataTran = null;
            time5 = System.currentTimeMillis();
            msg = "Added " + num + " records";
            msg += "\ntime 2-1: " + String.valueOf(time2 - time1) + " ms";
            msg += "\ntime 3-2: " + String.valueOf(time3 - time2) + " ms";
            msg += "\ntime 4-3: " + String.valueOf(time4 - time3) + " ms";
            msg += "\ntime 5-4: " + String.valueOf(time5 - time4) + " ms";
        } catch (final DataTransactionBlockingException e) {
            msg = "WARNING: DataTransactionBlockingException";
            msg += " No records added!";
        } catch (final CancellationException e) {
            // not our case: ignore
            msg = "CancelledException";
            msg += " No records added!";
        } catch (final Exception e) {
            msg = "WARNING: Exception";
            msg += " No records added!";
            e.printStackTrace();
        } finally {
            if (dataTran != null) {
                dataTran.end(false);
		logger.info("dataTran.end(false)");
            }
        }
        System.out.println(msg);
    }

    private static void retrieveRowsInTable(final String tableName, int maxRecords)
    {
        System.out.println("retrieveRowsInTable: tableName " + tableName + ", maxRecords " + maxRecords);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0, time7 = 0;
        int numRecords = 0;
        DataTransaction dataTran = dtt.dt;
        try {
            time1 = System.currentTimeMillis();
	    logger.info("before retrieve() 1");
            Stream<Record> cur = dataTran.retrieve(type, null, null);
            try {
                time2 = System.currentTimeMillis();
                numRecords += cur.count();
            } finally {
                cur.close();
            }
            time3 = System.currentTimeMillis();

	    logger.info("before retrieve() 2");
            cur = dataTran.retrieve(type, null, null);
	    int numRecords2 = 0;
            try {
                time4 = System.currentTimeMillis();
                // collect statistic of randomly generated value of the "RDFS.COMMENT" attribute
                // of records
                final Map<String, Integer> occurrenceMap = new HashMap<String, Integer>();
                for (Record r : cur) {
		    numRecords2++;

		    // print the first record
		    // if (numRecords2 == 1) {System.out.println("first record: " + recordToString(r));}

                    // the value of property/attribute RDFS.COMMENT:
                    final String val = r.getUnique(RDFS.COMMENT, String.class);

                    // increment the occurrence of attribute value
                    int occ = 1;
                    if (occurrenceMap.containsKey(val)) {
                        occ = occurrenceMap.get(val).intValue() + 1;
                    }
                    occurrenceMap.put(val, new Integer(occ));
		    if ((maxRecords > 0) && (numRecords2 >= maxRecords)) {
			break;
		    }
                }
                time5 = System.currentTimeMillis();
                endTransaction(dataTran);
                time6 = System.currentTimeMillis();

                // print the occurrenceMap
                printOccurrenceMap(occurrenceMap);
                time7 = System.currentTimeMillis();
            } finally {
                cur.close();
            }
	    
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Found " + Integer.toString(numRecords) + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
        System.out.println("time 5-4: " + String.valueOf(time5 - time4) + " ms");
        System.out.println("time 6-5: " + String.valueOf(time6 - time5) + " ms");
        System.out.println("time 7-6: " + String.valueOf(time7 - time6) + " ms");
    }

    private static void retrieveWithFilter(String tableName, final String conditionString,
					   final boolean doFilterOnClientSide)
    {
        System.out.println("retrieveWithFilter: tableName " + tableName + ", conditionString " 
			   + conditionString + ", doFilterOnClientSide " + doFilterOnClientSide);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        final HBaseDataStore ds = dtt.ds;
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        final boolean serverSideFiltering = ds.getServerFilterFlag();
        if (doFilterOnClientSide != !serverSideFiltering) {
            String msg = "Unsupported filtering modality: ";
            msg += "requested " + (doFilterOnClientSide ? "client-side" : "server-side");
            msg += ", application configured with "
                    + (serverSideFiltering ? "server-side" : "client-side");
            System.out.println(msg);
            return;
        }
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0, time7 = 0;
        int numRecords = 0;
        DataTransaction dataTran = dtt.dt;
        try {
            // Condition cond = Condition.create("/<" + RDFS.COMMENT + "> = 'comment # 0'");
            final XPath cond = XPath.parse(conditionString);
            System.out.println("cond is " + cond.toString());
            time1 = System.currentTimeMillis();
	    logger.info("before retrieve() 1");
            Stream<Record> cur = dataTran.retrieve(type, cond, null);
            try {
                time2 = System.currentTimeMillis();
                numRecords += cur.count();
                time3 = System.currentTimeMillis();
                System.out.println("first retrieve() found " + numRecords + " records");
            } finally {
                cur.close();
            }
	    logger.info("before retrieve() 2");
            cur = dataTran.retrieve(type, cond, null);
            try {
                time4 = System.currentTimeMillis();
                // collect statistic of randomly generated value of the "RDFS.COMMENT" attribute
                // of records
                final Map<String, Integer> occurrenceMap = new HashMap<String, Integer>();
                for (Record m : cur) {
                    // the value of property/attribute RDFS.COMMENT:
                    final String val = m.getUnique(RDFS.COMMENT, String.class);

                    // increment the occurrence of attribute value
                    int occ = 1;
                    if (occurrenceMap.containsKey(val)) {
                        occ = occurrenceMap.get(val).intValue() + 1;
                    }
                    occurrenceMap.put(val, new Integer(occ));
                }
                time5 = System.currentTimeMillis();
                endTransaction(dataTran);
                time6 = System.currentTimeMillis();

                // print the occurrenceMap
                printOccurrenceMap(occurrenceMap);
                time7 = System.currentTimeMillis();
            } finally {
                cur.close();
            }

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Found " + numRecords + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
        System.out.println("time 5-4: " + String.valueOf(time5 - time4) + " ms");
        System.out.println("time 6-5: " + String.valueOf(time6 - time5) + " ms");
        System.out.println("time 7-6: " + String.valueOf(time7 - time6) + " ms");
    }

    private static void retrieveAllAndSelectLocally(String tableName, final String conditionString)
    {
        System.out.println("retrieveAllAndSelectLocally: tableName " + tableName + ", conditionString " + conditionString);
	XPath cond = XPath.parse(conditionString);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0, time7 = 0;
        int numRecords = 0;
        DataTransaction dataTran = dtt.dt;
        try {
            time1 = System.currentTimeMillis();
	    logger.info("before retrieve() 1");
            Stream<Record> cur = dataTran.retrieve(type, null, null);
            try {
                time2 = System.currentTimeMillis();
                for (Record r : cur) {
                    if (cond.evalBoolean(r)) {
                        numRecords++;
                        // print the first record
			// if (numRecords == 1) { System.out.println("first record\n" + recordToString(r)); }
                    }
                }
                time3 = System.currentTimeMillis();
            } finally {
                cur.close();
            }
            
	    logger.info("before retrieve() 2");
            cur = dataTran.retrieve(type, null, null);
            try {
                time4 = System.currentTimeMillis();
                // collect statistic of randomly generated value of the "RDFS.COMMENT" attribute
                // of
                // records
                final Map<String, Integer> occurrenceMap = new HashMap<String, Integer>();
                for (Record m : cur) {
                    if (cond.evalBoolean(m)) {
                        // the value of property/attribute RDFS.COMMENT:
                        final String val = m.getUnique(RDFS.COMMENT, String.class);

                        // increment the occurrence of attribute value
                        int occ = 1;
                        if (occurrenceMap.containsKey(val)) {
                            occ = occurrenceMap.get(val).intValue() + 1;
                        }
                        occurrenceMap.put(val, new Integer(occ));
                    }
                }
                time5 = System.currentTimeMillis();
                endTransaction(dataTran);
                time6 = System.currentTimeMillis();

                // print the occurrenceMap
                printOccurrenceMap(occurrenceMap);
                time7 = System.currentTimeMillis();
            } finally {
                cur.close();
            }

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Found " + numRecords + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
        System.out.println("time 5-4: " + String.valueOf(time5 - time4) + " ms");
        System.out.println("time 6-5: " + String.valueOf(time6 - time5) + " ms");
        System.out.println("time 7-6: " + String.valueOf(time7 - time6) + " ms");
    }

    private static void countRowsInTable(final String tableName)
    {
        System.out.println("countRowsInTable: tableName " + tableName);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        long time1 = 0, time2 = 0, time3 = 0;
        long rowCounts = 0;
        DataTransaction dataTran = dtt.dt;
        try {
            time1 = System.currentTimeMillis();
	    logger.info("before dataTran.count() " + type.toString());
            rowCounts = dataTran.count(type, null);
	    logger.info("after dataTran.count()");
            time2 = System.currentTimeMillis();
            endTransaction(dataTran);
            time3 = System.currentTimeMillis();

        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Found " + Long.toString(rowCounts) + " entries");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
    }

    private static void lookupIdInTable(final String id, final String tableName)
    {
        System.out.println("lookupIdInTable: id " + id + ", tableName " + tableName);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
	Set<URIImpl> ids = new HashSet<URIImpl>();
	ids.add(new URIImpl(id));

        long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0;
	int numRecords = 0;
        DataTransaction dataTran = dtt.dt;

        try {
            time1 = System.currentTimeMillis();
	    logger.info("before lookup() 1");
            Stream<Record> cur = dataTran.lookup(type, ids, null);
            try {
                time2 = System.currentTimeMillis();
                numRecords += cur.count();
                time3 = System.currentTimeMillis();
            } finally {
                cur.close();
            }

	    logger.info("before lookup() 2");
            cur = dataTran.lookup(type, ids, null);
            try {
                time4 = System.currentTimeMillis();
                for (Record r : cur) {
                    String str = "found ";
                    str += recordToString(r);
                    System.out.println(str);
                }
                time5 = System.currentTimeMillis();
                endTransaction(dataTran);
                time6 = System.currentTimeMillis();
            } finally {
                cur.close();
            }
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Found " + Integer.toString(numRecords) + " Records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
        System.out.println("time 5-4: " + String.valueOf(time5 - time4) + " ms");
        System.out.println("time 6-5: " + String.valueOf(time6 - time5) + " ms");
    }

    private static void updateIdInTable(final String id, final String tableName, final String newvalue)
    {
        System.out.println("updateIdInTable: id " + id + ", tableName " + tableName + ", newvalue " + newvalue);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(false);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
	Record r = Record.create();
	URI uriID = new URIImpl(id);
	r.setID(uriID);
	r.set(RDF.TYPE, type);
        r.set(RDFS.COMMENT, newvalue);

        long time1 = 0, time2 = 0, time3 = 0;
        DataTransaction dataTran = dtt.dt;

        try {
            time1 = System.currentTimeMillis();
	    logger.info("before update()");
	    dataTran.store(type, r);
            time2 = System.currentTimeMillis();
            endTransaction(dataTran);
            time3 = System.currentTimeMillis();
	    
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Updated record");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
    }

    private static void deleteIdInTable(final String id, final String tableName)
    {
        System.out.println("deleteIdInTable: id " + id + ", tableName " + tableName);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(false);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
	Record r = Record.create();
	URI uriID = new URIImpl(id);
	r.set(RDF.TYPE, type);
	r.setID(uriID);

        long time1 = 0, time2 = 0, time3 = 0;
        DataTransaction dataTran = dtt.dt;

        try {
            time1 = System.currentTimeMillis();
	    logger.info("before delete()");
	    dataTran.delete(type, r.getID());
            time2 = System.currentTimeMillis();
            endTransaction(dataTran);
            time3 = System.currentTimeMillis();
	    
        } catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
        }

        System.out.println("Deleted record");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
    }

    private static void onceRetrieveRowsInTable(final String tableName, int maxRecords)
    {
        System.out.println("onceRetrieveRowsInTable: tableName " + tableName + ", maxRecords " + maxRecords);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;
        int numRecords = 0;
        DataTransaction dataTran = dtt.dt;
	Stream<Record> cur = null;
        try {
            time1 = System.currentTimeMillis();
	    logger.info("before retrieve() ");
	    cur = dataTran.retrieve(type, null, null);
	    time2 = System.currentTimeMillis();
	    for (Record r : cur) {
		numRecords++;
		System.out.println(recordToString(r));

		if ((maxRecords > 0) && (numRecords >= maxRecords)) {
		    break;
                }
	    }
	    time3 = System.currentTimeMillis();
	    endTransaction(dataTran);
	    time4 = System.currentTimeMillis();

	} catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
	} finally {
	    cur.close();
        }

        System.out.println("Found " + Integer.toString(numRecords) + " records");
        System.out.println("time 2-1: " + String.valueOf(time2 - time1) + " ms");
        System.out.println("time 3-2: " + String.valueOf(time3 - time2) + " ms");
        System.out.println("time 4-3: " + String.valueOf(time4 - time3) + " ms");
    }

    private static void onceRetrieveRowsInTable_IdOnly(final String tableName, int maxRecords)
    {
        System.out.println("onceRetrieveRowsInTable_IdOnly: tableName " + tableName + ", maxRecords " + maxRecords);
        final HBaseDataTransactionTester dtt = new HBaseDataTransactionTester(true);
        if (dtt.ds == null) {
            return;
        }
	URI type = dtt.getUriTypeFromTablename(tableName);
	if (type == null) {
	    return;
	}
        long time1 = 0, time2 = 0, time3 = 0, time4 = 0;
        int numRecords = 0;
        DataTransaction dataTran = dtt.dt;
	Stream<Record> cur = null;
        try {
            time1 = System.currentTimeMillis();
	    logger.info("before retrieve() ");
	    cur = dataTran.retrieve(type, null, null);
	    time2 = System.currentTimeMillis();
	    for (Record r : cur) {
		numRecords++;
		System.out.println(r.toString());

		if ((maxRecords > 0) && (numRecords >= maxRecords)) {
		    break;
                }
	    }
	    time3 = System.currentTimeMillis();
	    endTransaction(dataTran);
	    time4 = System.currentTimeMillis();

	} catch (final IOException e) {
            System.out.println("WARNING Exception");
            e.printStackTrace();
	} finally {
	    cur.close();
        }

        System.out.println("Found " + Integer.toString(numRecords) + " records");
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
	String cmdLineSyntax = className + " [options] cmd table [args*]";
	String header = "";
        String footer = "where cmd:\n"
	    + "1 TABLE NUM INDEX: populate with num records starting at index\n"
	    + "2 TABLE [MAX_RECORD]: retrieve max_record records (default 0 means all)\n"
	    + "3 TABLE CONDITION: filter records with condition on server-side\n"
	    + "4 TABLE CONDITION: retrieve all records and select locally with condition\n"
	    + "5 TABLE: count rows\n"
	    + "6 TABLE ID: lookup identifier\n"
	    + "7 TABLE ID NEW_VALUE: update the attribute RDF.COMMENT of identifier with new_value\n"
	    + "8 TABLE ID: delete identifier in table\n"
	    + "12 TABLE [MAX_RECORD]: once-retrieve max_record records (default 0 means all)\n"
	    + "13 TABLE [MAX_RECORD]: once-retrieve_IdOnly max_record records (default 0 means all)\n";

        formatter.printHelp(out, WIDTH, cmdLineSyntax, header, options, 2, 2, footer);
        out.flush();

        System.exit(1);
    }

    public static void main(final String[] args) throws Throwable
    {
	final Options options = new Options();
        options.addOption("cfg", "config_print",          false, "print configuration settings");
        options.addOption("csf", "client_side_filtering", false, "do filtering on client side (default server)");
        options.addOption("h", "help",                    false, "print help and exit");
        options.addOption("n", "native_mode",             false, "set native mode (default omid)");
        options.addOption("m", "master_host",            true, "the host running hdfs master, zookeeper and omid daemon ");
        options.addOption("p", "prefix",                 true, "the prefix of the tables and FS");
        options.addOption("tem", "transaction_end_mode", true, "set the mode of transaction end: 1 commit, 0 rollback, -1 do-nothing (default 1)");
	
	CommandLine cl = new GnuParser().parse(options, args);
	if (cl.hasOption("h")) {
	    printUsage(options);
	}
	if (cl.hasOption("cfg")) {
	    printCfgFiles = true;
	}
	boolean clientSideFlag = false;
	if (cl.hasOption("csf")) {
	    clientSideFlag = true;
	}
	if (cl.hasOption("n")) {
	    OmidMode = false;
	}

	if (cl.hasOption("m")) {
	    masterHost = cl.getOptionValue("m");
	}
	if (cl.hasOption("p")) {
	    generalPrefix = cl.getOptionValue("p");
	}
	if (cl.hasOption("tem")) {
	    int value = Integer.parseInt(cl.getOptionValue("tem"));
	    switch (value) {
	    case 1:
	    case 0:
	    case -1:
		transactionEndMode = value;
		break;
	    default:
		System.err.println("error: unknown value for transaction_end_mode " + cl.getOptionValue("tem"));
		printUsage(options);
		break;
	    }
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
	    // generate and store numEntries records from startIndex with random content
            if (leftArgs.length < 3) {
		System.err.println("error: missing num_of_entries");
                printUsage(options);
            } else if (leftArgs.length < 4) {
		System.err.println("error: missing start_index");
                printUsage(options);
            }
	    {
		int numEntries = -1;
		int startIndex = -1;
		numEntries = Integer.parseInt(leftArgs[2]);
		startIndex = Integer.parseInt(leftArgs[3]);
	
                populateTableRandomly(tableName, numEntries, startIndex, transactionEndMode);
            }
            break;

	case 2:
            // retrieve all the rows in the given table
            {
		int maxRecords = 0;
		if (leftArgs.length > 2) {
		    maxRecords = Integer.parseInt(leftArgs[2]);
		}

		retrieveRowsInTable(tableName, maxRecords);
            }
            break;

        case 3:
            // filter with condition (default on server-side)
            if (leftArgs.length < 3) {
		System.err.println("error: missing condition");
                printUsage(options);
            }
	    {
		String conditionString = leftArgs[2];

		retrieveWithFilter(tableName, conditionString, clientSideFlag);
	    }
            break;

        case 4:
            // retrieve all the records and select locally with condition
            if (leftArgs.length < 3) {
		System.err.println("error: missing condition");
                printUsage(options);
            }
            {
		String conditionString = leftArgs[2];

                retrieveAllAndSelectLocally(tableName, conditionString);
            }
            break;

        case 5:
            // count the rows in the given table
            {
                countRowsInTable(tableName);
            }
            break;

        case 6:
            // lookup in the given table the given id (=URI=rowkey)
            if (leftArgs.length < 3) {
		System.err.println("error: missing identifier");
                printUsage(options);
            }
            {
		String identifier = leftArgs[2];

                lookupIdInTable(identifier, tableName);
            }
            break;

        case 7:
            // update the attribute RDF.COMMENT of id (=URI=rowkey) in table with newvalue
            if (leftArgs.length < 3) {
		System.err.println("error: missing identifier");
                printUsage(options);
            } else if (leftArgs.length < 4) {
		System.err.println("error: missing new_value");
                printUsage(options);
            }
            {
		String identifier = leftArgs[2];
		String newValue   = leftArgs[3];

                updateIdInTable(identifier, tableName, newValue);
            }
            break;

        case 8:
            // delete in the given table the given id (=URI=rowkey)
            if (leftArgs.length < 3) {
		System.err.println("error: missing identifier");
                printUsage(options);
            }
            {
		String identifier = leftArgs[2];
		
                deleteIdInTable(identifier, tableName);
            }
            break;

	case 12:
            // once retrieve all the rows in the given table
            {
		int maxRecords = 0;
		if (leftArgs.length > 2) {
		    maxRecords = Integer.parseInt(leftArgs[2]);
		}

		onceRetrieveRowsInTable(tableName, maxRecords);
            }
            break;

	case 13:
            // once retrieve all the rows in the given table printing only the ID
            {
		int maxRecords = 0;
		if (leftArgs.length > 2) {
		    maxRecords = Integer.parseInt(leftArgs[2]);
		}

		onceRetrieveRowsInTable_IdOnly(tableName, maxRecords);
            }
            break;

	default:
            printUsage(options);
        }
	
        System.exit(0);
    }

}
