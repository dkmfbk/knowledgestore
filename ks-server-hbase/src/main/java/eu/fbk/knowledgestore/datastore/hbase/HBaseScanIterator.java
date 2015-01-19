package eu.fbk.knowledgestore.datastore.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.hbase.utils.AbstractHBaseUtils;
import eu.fbk.knowledgestore.datastore.hbase.utils.AvroSerializer;
import eu.fbk.knowledgestore.datastore.hbase.utils.HBaseFilter;

/**
 * Performs an HBase scanner-based retrieval of the records matching an optional condition from a
 * certain HBase table/column family.
 */
public class HBaseScanIterator extends AbstractIterator<Record> implements Closeable {

    /** Logger object used inside HdfsFileStore. */
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanIterator.class);

    /** Optional conditions to be applied locally to match records. */
    @Nullable
	private final XPath condition;

    /** The properties to return, null if all properties are requested. */
    @Nullable
	private final URI[] properties;

    /** Created scanner, kept in order to close it after iteration. */
    @Nullable
	private final ResultScanner scanner;

    /** The iterator returned by the scanner. */
    private final Iterator<Result> hbaseIterator;

    /** The {@code AvroSerializer} used to deserialize rows into records. */
    private final AvroSerializer serializer;

    /**
     * Creates a new {@code HBaseScanStream} based on the parameters supplied.
     * 
     * @param hbaseUtils
     *            the {@code AbstractHBaseUtils} object for accessing HBase, not null
     * @param tableName
     *            the name of the HBase table to access, not null
     * @param familyName
     *            the name of the HBase column family to access, not null
     * @param condition
     *            optional condition to be satisfied by matching records, possibly null
     * @param properties
     *            properties to return, null if all properties are requested
     * @param localFiltering
     *            true if filtering should be performed locally to the HBase client
     * @throws IOException
     *             on failure
     */
    public HBaseScanIterator(final AbstractHBaseUtils hbaseUtils, final String tableName,
			     final String familyName, @Nullable final XPath condition,
			     @Nullable final Iterable<? extends URI> properties, final boolean localFiltering)
	throws IOException {

        // Check parameters
        Preconditions.checkNotNull(hbaseUtils);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(familyName);

        // Configure Scan operation, differentiating between local or remote filtering
        final Scan scan = hbaseUtils.getScan(tableName, familyName);
        if (condition != null && !localFiltering) {
            scan.setFilter(new HBaseFilter(condition, hbaseUtils.getSerializer()));
        }

        // Open a result scanner and keep track of it, so that it can be closed at the end
        final ResultScanner scanner = hbaseUtils.getScanner(tableName, scan);

        // Initialize state
        this.condition = localFiltering ? condition : null; // unset on server-side filtering
        this.properties = properties == null ? null : Iterables.toArray(properties, URI.class);
        this.serializer = hbaseUtils.getSerializer();
        this.scanner = scanner;
        this.hbaseIterator = this.scanner.iterator();
    }

    @Override
	protected Record computeNext() {

	try {
	    // Iterate until a matching record is found or EOF is reached
	    while (this.hbaseIterator.hasNext()) {

		// Retrieve next binary result from HBase
		final byte[] bytes = this.hbaseIterator.next().value();

		// Attempt deserialization. Log and skip result on failure
		Record record;
		try {
		    record = (Record) this.serializer.fromBytes(bytes);
		} catch (final Throwable ex) {
		    LOGGER.error("discarded record with avroBytes \"" + bytes 
				 + ", " + ex.toString());
		    continue;
		}

		// Evaluate condition locally, if required
		if (this.condition != null) {
		    final boolean matches = this.condition.evalBoolean(record);
		    if (!matches) {
			continue;
		    }
		}

		// Perform client-side projection, if requested
		if (this.properties != null) {
		    record = record.retain(this.properties);
		}

		// Return the record
		return record;
	    }

	    // Signal EOF
	    return endOfData();

	} catch (Exception e) {
	    LOGGER.warn("ignored Exception |" + e.toString() + "| and returned");
	    return null;
	}
    }

    /**
     * {@inheritDoc} Closes the HBase scanner, if previousy created.
     */
    @Override
    public void close() {
        if (this.scanner != null) {
            LOGGER.debug("Closing HBaseScanIterator");
            this.scanner.close();
        }
    }

}
