package eu.fbk.knowledgestore.datastore.hbase.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.knowledgestore.data.Dictionary;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.XPath;

public class HBaseFilter extends FilterBase {

    static final Logger LOGGER = LoggerFactory.getLogger(HBaseFilter.class);

    /** The condition to be applied to filter out records. */
    private XPath condition;

    /** The serializer to be used to decode stored byte arrays into records. */
    private AvroSerializer serializer;

    /**
     * Default constructor for exclusive use by HBase. This constructor is used for
     * deserialization of an {@code HBaseFilter} on a region server; initialization of the object
     * is done by {@link #readFields(DataInput)}.
     */
    public HBaseFilter() {
    }

    /**
     * Constructor for normal use.
     * 
     * @param condition
     *            the condition to be evaluated for each Node.
     * @param serializer
     *            the serializer for decoding stored byte arrays into records
     */
    public HBaseFilter(final XPath condition, final AvroSerializer serializer) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkNotNull(serializer);
        this.condition = condition;
        this.serializer = serializer;
    }

    @Override
    public ReturnCode filterKeyValue(final KeyValue keyValue) {
        final Record record = (Record) this.serializer.fromBytes(keyValue.getValue());
        return this.condition.evalBoolean(record) ? ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
    }

    @Override
    public void readFields(final DataInput dataIn) throws IOException {
        final String conditionString = dataIn.readUTF();
        final String dictionaryURL = dataIn.readUTF();
        this.condition = XPath.parse(conditionString);
        this.serializer = new AvroSerializer(Dictionary.createHadoopDictionary(URI.class, dictionaryURL));
    }

    @Override
    public void write(final DataOutput dataOut) throws IOException {
        dataOut.writeUTF(this.condition.toString());
        dataOut.writeUTF(this.serializer.getDictionary().getDictionaryURL());
    }

}
