package eu.fbk.knowledgestore.datastore.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.datastore.hbase.utils.AbstractHBaseUtils;

/**
 * Performs batch lookup of records by ID from a given HBase table, optionally returning only a
 * subset of record properties.
 */
public class HBaseIterator extends AbstractIterator<Record> {

    /** The batch size. */
    private static final int BATCH_SIZE = 100;

    /** Object referencing transactional layer. */
    private final AbstractHBaseUtils hbaseUtils;

    /** HBase table name to be used */
    private final String tableName;

    /** Properties to be looked up. */
    private final URI[] properties;

    /** An iterator over the IDs of the records to lookup. */
    private final Iterator<URI> idIterator;

    /** An iterator over records buffered in the last batch looked up from HBase. */
    private Iterator<Record> recordIterator;

    /**
     * Creates a new {@code HBaseStream} for the parameters supplied.
     * 
     * @param hbaseUtils
     *            the {@code AbstractHBaseUtils} object for accessing HBase, not null
     * @param tableName
     *            the name of the HBase table to access, not null
     * @param ids
     *            the IDs of records to fetch from HBase, not null
     * @param properties
     *            the properties of records to return, null if all properties should be returned
     */
    public HBaseIterator(final AbstractHBaseUtils hbaseUtils, final String tableName,
            final Set<? extends URI> ids, @Nullable final Set<? extends URI> properties) {

        Preconditions.checkNotNull(hbaseUtils);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(ids);

        this.hbaseUtils = hbaseUtils;
        this.tableName = tableName;
        this.properties = properties == null ? null : Iterables.toArray(properties, URI.class);
        this.idIterator = ImmutableList.copyOf(ids).iterator();
        this.recordIterator = Iterators.emptyIterator();
    }

    @Override
    protected Record computeNext() {
        while (true) {
            // Return a record previously buffered, if available
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            }

            // Otherwise, retrieve next batch of IDs
            final List<URI> ids = Lists.newArrayListWithCapacity(BATCH_SIZE);
            while (this.idIterator.hasNext() && ids.size() < BATCH_SIZE) {
                ids.add(this.idIterator.next());
            }

            // EOF reached if there are no more IDs to retrieve
            if (ids.isEmpty()) {
                return endOfData();
            }

            // Retrieve next batch of records corresponding to IDs batch
            final List<Record> records;
            try {
                records = this.hbaseUtils.get(this.tableName, ids);
            } catch (final IOException ex) {
                throw Throwables.propagate(ex);
            }

            // Perform client-side projection, if requested
            if (this.properties != null) {
                for (int i = 0; i < records.size(); ++i) {
                    records.set(i, records.get(i).retain(this.properties));
                }
            }

            // Store fetched record in record iterator and return first one
            this.recordIterator = records.iterator();
        }
    }

}
