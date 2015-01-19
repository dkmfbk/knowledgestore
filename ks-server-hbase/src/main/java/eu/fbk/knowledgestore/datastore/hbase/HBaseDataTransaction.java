package eu.fbk.knowledgestore.datastore.hbase;

import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_USR_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_CON_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_ENT_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_MEN_TAB_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_FAM_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_QUA_NAME;
import static eu.fbk.knowledgestore.datastore.hbase.utils.HBaseConstants.DEFAULT_RES_TAB_NAME;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.DataStore;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.datastore.hbase.utils.AbstractHBaseUtils;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.vocabulary.KS;
// import java.io.PrintWriter;

/**
 * Class HBaseDataTransaction to perform operations on top of
 * HBase, using specific operations to create transactions.
 */
public class HBaseDataTransaction implements DataTransaction {

    /** Logger object used inside HBaseFileStore. */
    private static Logger logger = LoggerFactory.getLogger(HBaseDataTransaction.class);

    /** Represents the transactional layer to be used. */
    private AbstractHBaseUtils hbaseUtils;

    /**
     * Constructor.
     * @param pHbaseUtils represents the transactional layer to be used.
     */
    HBaseDataTransaction (AbstractHBaseUtils pHbaseUtils) {
        this.setHbaseUtils(pHbaseUtils);
        this.getHbaseUtils().begin();
    }
    
    @Nullable
    static URI getRecordType(Record record) {
        for (URI type : record.get(RDF.TYPE, URI.class)) {
            if (DataStore.SUPPORTED_TYPES.contains(type)) {
                return type;
            }
        }
        return null;
    }

    @Override
    public Stream<Record> lookup(final URI type, final Set<? extends URI> ids,
            final @Nullable Set<? extends URI> properties) throws DataCorruptedException,
            IOException, IllegalArgumentException, IllegalStateException {
        // Record.type, set of ids, a set of properties to be obtained
        // for the objects retrieved select all needed properties
        // create cursor on top of that list
        HBaseIterator iterator = null;
        if (KS.RESOURCE.equals(type)) {
            iterator = new HBaseIterator(hbaseUtils, hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_RES_TAB_NAME, 
					 ids, properties);
        } else if (KS.MENTION.equals(type)) {
            iterator = new HBaseIterator(hbaseUtils, hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_MEN_TAB_NAME,
					 ids, properties);
        } else if (KS.ENTITY.equals(type)) {
            iterator = new HBaseIterator(hbaseUtils, hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_ENT_TAB_NAME,
					 ids, properties);
        } else if (KS.CONTEXT.equals(type)) {
            iterator = new HBaseIterator(hbaseUtils, hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_CON_TAB_NAME,
					 ids, properties);
        } else if (KS.USER.equals(type)) {
            iterator = new HBaseIterator(hbaseUtils, hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_USR_TAB_NAME,
					 ids, properties);
        } else {
            throw new IllegalArgumentException("Unsupported record type "
                    + Data.toString(type, Data.getNamespaceMap()));
        }
        return Stream.create(iterator);
    }

    @Override
    public Stream<Record> retrieve(final URI type, @Nullable final XPath condition,
            @Nullable final Set<? extends URI> properties) throws DataCorruptedException,
            IOException, IllegalArgumentException, IllegalStateException {

        String tableName;
        String familyName;

        if (KS.RESOURCE.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_RES_TAB_NAME;
            familyName = DEFAULT_RES_FAM_NAME;
        } else if (KS.MENTION.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_MEN_TAB_NAME;
            familyName = DEFAULT_MEN_FAM_NAME;
        } else if (KS.ENTITY.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_ENT_TAB_NAME;
            familyName = DEFAULT_ENT_FAM_NAME;
        } else if (KS.CONTEXT.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_CON_TAB_NAME;
            familyName = DEFAULT_CON_FAM_NAME;
        } else if (KS.USER.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_USR_TAB_NAME;
            familyName = DEFAULT_USR_FAM_NAME;
        } else {
            throw new IllegalArgumentException("Unsupported record type "
                    + Data.toString(type, Data.getNamespaceMap()));
        }

        return Stream.create(new HBaseScanIterator(hbaseUtils, tableName, familyName,
                condition, properties, hbaseUtils.getServerFilterFlag()));
    }

    @Override
    public long count(URI type, XPath condition)
            throws DataCorruptedException, IOException,
		   IllegalArgumentException, IllegalStateException {

        String tableName = null;
        String familyName = null;

        if (KS.RESOURCE.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_RES_TAB_NAME;
            familyName = DEFAULT_RES_FAM_NAME;
        } else if (KS.MENTION.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_MEN_TAB_NAME;
            familyName = DEFAULT_MEN_FAM_NAME;
        } else if (KS.ENTITY.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_ENT_TAB_NAME;
            familyName = DEFAULT_ENT_FAM_NAME;
        } else if (KS.CONTEXT.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_CON_TAB_NAME;
            familyName = DEFAULT_CON_FAM_NAME;
        } else if (KS.USER.equals(type)) {
            tableName = hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_USR_TAB_NAME;
            familyName = DEFAULT_USR_FAM_NAME;
        } else {
            throw new IllegalArgumentException("Unsupported record type "
                    + Data.toString(type, Data.getNamespaceMap()));
        }

        return getHbaseUtils().count(tableName, familyName, condition);
    }

    @Override
    public Stream<Record> match(final Map<URI, XPath> conditions, final Map<URI, Set<URI>> ids,
            final Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc} Puts a record inside the HBase store.
     */
    @Override
    public void store(final URI type, final Record record) {
        if (KS.RESOURCE.equals(type)) {
            getHbaseUtils().processPut(record,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_RES_TAB_NAME,
                    DEFAULT_RES_FAM_NAME,
                    DEFAULT_RES_QUA_NAME);
        } else if (KS.MENTION.equals(type)) {
            getHbaseUtils().processPut(record,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_MEN_TAB_NAME,
                    DEFAULT_MEN_FAM_NAME,
                    DEFAULT_MEN_QUA_NAME);
        } else if (KS.ENTITY.equals(type)) {
            getHbaseUtils().processPut(record,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_ENT_TAB_NAME,
                    DEFAULT_ENT_FAM_NAME,
                    DEFAULT_ENT_QUA_NAME);
        } else if (KS.CONTEXT.equals(type)) {
            getHbaseUtils().processPut(record,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_CON_TAB_NAME,
                    DEFAULT_CON_FAM_NAME,
                    DEFAULT_CON_QUA_NAME);
        } else if (KS.USER.equals(type)) {
            getHbaseUtils().processPut(record,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_USR_TAB_NAME,
                    DEFAULT_USR_FAM_NAME,
                    DEFAULT_USR_QUA_NAME);
        } else {
            throw new IllegalArgumentException("Unsupported record:\n"
                    + record.toString(Data.getNamespaceMap(), true));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final URI type, final URI id)  {
        if (KS.RESOURCE.equals(type)) {
            getHbaseUtils().processDelete(id,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_RES_TAB_NAME,
                    DEFAULT_RES_FAM_NAME,
                    DEFAULT_RES_QUA_NAME);
        } else if (KS.MENTION.equals(type)) {
            getHbaseUtils().processDelete(id,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_MEN_TAB_NAME,
                    DEFAULT_MEN_FAM_NAME,
                    DEFAULT_MEN_QUA_NAME);
        } else if (KS.ENTITY.equals(type)) {
            getHbaseUtils().processDelete(id,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_ENT_TAB_NAME,
                    DEFAULT_ENT_FAM_NAME,
                    DEFAULT_ENT_QUA_NAME);
        } else if (KS.CONTEXT.equals(type)) {
            getHbaseUtils().processDelete(id,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_CON_TAB_NAME,
                    DEFAULT_CON_FAM_NAME,
                    DEFAULT_CON_QUA_NAME);
        } else if (KS.USER.equals(type)) {
            getHbaseUtils().processDelete(id,
                    hbaseUtils.getHbaseTableNamePrefix() + DEFAULT_USR_TAB_NAME,
                    DEFAULT_USR_FAM_NAME,
                    DEFAULT_USR_QUA_NAME);
        } else {
            throw new IllegalArgumentException("Unsupported record type:\n" + type);
        }
    }

    @Override
    public void end(boolean commit) throws DataCorruptedException, IOException,
            IllegalStateException {
        if (commit)
            getHbaseUtils().commit();
        else
            getHbaseUtils().rollback();
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
    public static void setLogger(Logger logger) {
        HBaseDataTransaction.logger = logger;
    }

    /**
     * @return the hbaseUtils
     */
    public AbstractHBaseUtils getHbaseUtils() {
        return hbaseUtils;
    }

    /**
     * @param hbaseUtils the hbaseUtils to set
     */
    public void setHbaseUtils(AbstractHBaseUtils hbaseUtils) {
        this.hbaseUtils = hbaseUtils;
    }
}
