package eu.fbk.knowledgestore.datastore.hbase.utils;

/**
 * Class containing the default values for the HBase data store module.
 */
public class HBaseConstants {

    public static final String HBASEDATASTORE_TABLEPREFIX_PROP = "hbasedatastore.tableprefix";
    public static final String HBASEDATASTORE_TABLEPREFIX_DEFAULT = "nwr.";

    /** Default table name for resources. */
    public static final String DEFAULT_RES_TAB_NAME = "resources";
    public static final String DEFAULT_RES_FAM_NAME = "rf";
    public static final String DEFAULT_RES_QUA_NAME = "r";

    /** Default table name for mentions. */
    public static final String DEFAULT_MEN_TAB_NAME = "mentions";
    public static final String DEFAULT_MEN_FAM_NAME = "mf";
    public static final String DEFAULT_MEN_QUA_NAME = "m";

    /** Default table name for entities. */
    public static final String DEFAULT_ENT_TAB_NAME = "entities";
    public static final String DEFAULT_ENT_FAM_NAME = "ef";
    public static final String DEFAULT_ENT_QUA_NAME = "e";

    /** Default table name for contexts. */
    public static final String DEFAULT_CON_TAB_NAME = "contexts";
    public static final String DEFAULT_CON_FAM_NAME = "cf";
    public static final String DEFAULT_CON_QUA_NAME = "c";

    /** Default table name for users. */
    public static final String DEFAULT_USR_TAB_NAME = "users";
    public static final String DEFAULT_USR_FAM_NAME = "uf";
    public static final String DEFAULT_USR_QUA_NAME = "u";

    /** Hadoop configuration parameters */
    public static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
    public static final String HADOOP_FS_URL          = "fs.url";

    /** HBase configuration parameters */
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    public static final String HBASEDATASTORE_SERVERFILTERFLAG = "hbasedatastore.serverfilterflag";

    /** HBase transactional layer property. */
    public static final String HBASE_TRAN_LAYER = "hbase.transactional.layer";
    /** HBase transactional layers options. */
    public static final String TEPHRA_TRAN_LAYER_OPT = "tephra";
    public static final String OMID_TRAN_LAYER_OPT = "omid";
    public static final String NATIVE_TRAN_LAYER_OPT = "native";

    /** HBase properties to be loaded into regions. */
    public static final String HBASE_REGION_COPROCESSOR_CLASSES = "hbase.coprocessor.region.classes";
    public static final String HBASE_REGION_MEMSTORE_FLUSH_SIZE = "hbase.hregion.memstore.flush.size";
    public static final String HBASE_REGION_NRESERVATION_BLOCKS = "hbase.regionserver.nbreservationblocks";

    /** OMID properties. */
    public static final String OMID_TSO_HOST = "tso.host";
    public static final String OMID_TSO_PORT = "tso.port";

    /** OMID options. */
    public static final String OMID_REGIONSERVER_COMPACTER_OPT = "com.yahoo.omid.regionserver.Compacter";
    public static final String OMID_TSO_DEFAULT_HOST_OPT = "hlt-services4";
    public static final int OMID_REGION_MEMSTORE_FLUSH_SIZE_OPT = 100*1024;
    public static final int OMID_REGION_NRESERVATION_BLOCKS_OPT = 1;
    public static final int OMID_TSO_DEFAULT_PORT_OPT = 1234;


    /** Default URIs dictionary file. */
    public static final String URIDICT_RELATIVEPATH_PROP = "uris.dictionary.path";
    public static final String URIDICT_RELATIVEPATH_DEFAULT = "tmp/ROL-ks/uri.dic";
}
