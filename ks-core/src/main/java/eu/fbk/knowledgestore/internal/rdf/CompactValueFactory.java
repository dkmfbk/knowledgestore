package eu.fbk.knowledgestore.internal.rdf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Objects;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.CalendarLiteralImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompactValueFactory implements ValueFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactValueFactory.class);

    private static final DatatypeFactory DATATYPE_FACTORY;

    private static final CompactValueFactory VALUE_FACTORY;

    static {
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
            VALUE_FACTORY = new CompactValueFactory();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    private final String bnodePrefix;

    private final AtomicLong bnodeCounter;

    private CompactValueFactory() {
        final UUID uuid = UUID.randomUUID();
        final StringBuilder builder = new StringBuilder(12);
        long num = Math.abs(uuid.getLeastSignificantBits());
        builder.append(charFor(num % 52));
        num = num / 52;
        for (int i = 0; i < 5; ++i) {
            builder.append(charFor(num % 62));
            num = num / 62;
        }
        num = Math.abs(uuid.getMostSignificantBits());
        for (int i = 0; i < 6; ++i) {
            builder.append(charFor(num % 62));
            num = num / 62;
        }
        this.bnodePrefix = builder.toString();
        this.bnodeCounter = new AtomicLong(0L);
    }

    private static char charFor(final long num) {
        if (num < 26) {
            return (char) (65 + num);
        } else if (num < 52) {
            return (char) (71 + num);
        } else if (num < 62) {
            return (char) (num - 4);
        } else {
            return 'x';
        }
    }

    public static CompactValueFactory getInstance() {
        return VALUE_FACTORY;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T normalize(@Nullable final T object) {
        if (object instanceof Statement) {
            if (!(object instanceof StatementImpl) && !(object instanceof ContextStatementImpl)) {
                final Statement s = (Statement) object;
                return s.getContext() == null ? (T) createStatement(s.getSubject(),
                        s.getPredicate(), s.getObject()) : (T) createStatement(s.getSubject(),
                        s.getPredicate(), s.getObject(), s.getContext());
            }
        } else if (object instanceof URI) {
            if (!(object instanceof URIImpl)) {
                return (T) createURI(((URI) object).stringValue());
            }
        } else if (object instanceof BNode) {
            if (!(object instanceof BNodeImpl)) {
                return (T) createBNode(((BNode) object).getID());
            }
        } else if (object instanceof Literal) {
            if (!(object instanceof StringLiteral) && !(object instanceof NumberLiteral)
                    && !(object instanceof BooleanLiteralImpl)
                    && !(object instanceof CalendarLiteralImpl)) {
                final Literal l = (Literal) object;
                return l.getLanguage() != null ? (T) createLiteral(l.getLabel(), l.getLanguage())
                        : (T) createLiteral(l.getLabel(), l.getDatatype());
            }
        }
        return object;
    }

    @Override
    public URI createURI(final String uri) {
        return new URIImpl(uri);
    }

    @Override
    public URI createURI(final String namespace, final String localName) {
        return new URIImpl(namespace + localName);
    }

    @Override
    public BNode createBNode() {
        return new BNodeImpl(this.bnodePrefix
                + Long.toString(this.bnodeCounter.getAndIncrement(), 32));
    }

    @Override
    public BNode createBNode(final String nodeID) {
        return new BNodeImpl(nodeID);
    }

    @Override
    public Literal createLiteral(final String label) {
        return new StringLiteral(label, (URI) null);
    }

    @Override
    public Literal createLiteral(final String label, final String language) {
        return new StringLiteral(label, language);
    }

    @Override
    public Literal createLiteral(final String label, final URI datatype) {
        try {
            if (datatype == null) {
                return new StringLiteral(label, (String) null);
            } else if (datatype.equals(XMLSchema.STRING)) {
                return new StringLiteral(label, XMLSchema.STRING);
            } else if (datatype.equals(XMLSchema.BOOLEAN)) {
                final boolean value = XMLDatatypeUtil.parseBoolean(label);
                return value ? BooleanLiteralImpl.TRUE : BooleanLiteralImpl.FALSE;
            } else if (datatype.equals(XMLSchema.INT)) {
                return new LongLiteral(XMLSchema.INT, XMLDatatypeUtil.parseInt(label));
            } else if (datatype.equals(XMLSchema.LONG)) {
                return new LongLiteral(XMLSchema.LONG, XMLDatatypeUtil.parseLong(label));
            } else if (datatype.equals(XMLSchema.SHORT)) {
                return new LongLiteral(XMLSchema.SHORT, XMLDatatypeUtil.parseShort(label));
            } else if (datatype.equals(XMLSchema.BYTE)) {
                return new LongLiteral(XMLSchema.BYTE, XMLDatatypeUtil.parseByte(label));
            } else if (datatype.equals(XMLSchema.DOUBLE)) {
                return new DoubleLiteral(XMLSchema.DOUBLE, XMLDatatypeUtil.parseDouble(label));
            } else if (datatype.equals(XMLSchema.FLOAT)) {
                return new DoubleLiteral(XMLSchema.FLOAT, XMLDatatypeUtil.parseFloat(label));
            } else if (datatype.equals(XMLSchema.DATETIME) || datatype.equals(XMLSchema.DATE)
                    || datatype.equals(XMLSchema.TIME) || datatype.equals(XMLSchema.GYEARMONTH)
                    || datatype.equals(XMLSchema.GMONTHDAY) || datatype.equals(XMLSchema.GYEAR)
                    || datatype.equals(XMLSchema.GMONTH) || datatype.equals(XMLSchema.GDAY)) {
                return createLiteral(XMLDatatypeUtil.parseCalendar(label));
            } else if (datatype.equals(XMLSchema.DECIMAL)) {
                return new BigDecimalLiteral(datatype, XMLDatatypeUtil.parseDecimal(label));
            } else if (datatype.equals(XMLSchema.INTEGER)
                    || datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                    || datatype.equals(XMLSchema.POSITIVE_INTEGER)
                    || datatype.equals(XMLSchema.NEGATIVE_INTEGER)) {
                return new BigIntegerLiteral(datatype, XMLDatatypeUtil.parseInteger(label));
            } else {
                return new StringLiteral(label, datatype);
            }
        } catch (final Throwable ex) {
            LOGGER.warn("Illegal literal: '" + label + "'^^<" + datatype + "> (dropping datatype)");
            return createLiteral(label);
        }
    }

    @Override
    public Literal createLiteral(final boolean value) {
        return value ? BooleanLiteralImpl.TRUE : BooleanLiteralImpl.FALSE;
    }

    @Override
    public Literal createLiteral(final byte value) {
        return new LongLiteral(XMLSchema.BYTE, value);
    }

    @Override
    public Literal createLiteral(final short value) {
        return new LongLiteral(XMLSchema.SHORT, value);
    }

    @Override
    public Literal createLiteral(final int value) {
        return new LongLiteral(XMLSchema.INT, value);
    }

    @Override
    public Literal createLiteral(final long value) {
        return new LongLiteral(XMLSchema.LONG, value);
    }

    @Override
    public Literal createLiteral(final float value) {
        return new DoubleLiteral(XMLSchema.FLOAT, value);
    }

    @Override
    public Literal createLiteral(final double value) {
        return new DoubleLiteral(XMLSchema.DOUBLE, value);
    }

    @Override
    public Literal createLiteral(final XMLGregorianCalendar calendar) {
        return new CalendarLiteralImpl(calendar);
    }

    @Override
    public Literal createLiteral(final Date date) {
        final GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        final XMLGregorianCalendar xmlCalendar = DATATYPE_FACTORY
                .newXMLGregorianCalendar(calendar);
        return new CalendarLiteralImpl(xmlCalendar);
    }

    @Override
    public Statement createStatement(final Resource subject, final URI predicate,
            final Value object) {
        return new StatementImpl(subject, predicate, object);
    }

    @Override
    public Statement createStatement(final Resource subject, final URI predicate,
            final Value object, final Resource context) {
        return context == null ? new StatementImpl(subject, predicate, object)
                : new ContextStatementImpl(subject, predicate, object, context);
    }

    private static final class StringLiteral extends LiteralImpl {

        private static final long serialVersionUID = 1L;

        StringLiteral(final String label, final String language) {
            super(label, language == null ? null : language.intern());
        }

        StringLiteral(final String label, final URI datatype) {
            super(label, datatype);
        }

    }

    private abstract static class NumberLiteral implements Literal {

        private static final long serialVersionUID = 1L;

        private final URI datatype;

        NumberLiteral(final URI datatype) {
            this.datatype = datatype;
        }

        abstract Number getNumber();

        boolean equalNumber(final Literal literal) {
            return getNumber().equals(((NumberLiteral) literal).getNumber());
        }

        @Override
        public String getLabel() {
            return stringValue();
        }

        @Override
        public String getLanguage() {
            return null;
        }

        @Override
        public URI getDatatype() {
            return this.datatype;
        }

        @Override
        public String stringValue() {
            return getNumber().toString();
        }

        @Override
        public byte byteValue() {
            return getNumber().byteValue();
        }

        @Override
        public short shortValue() {
            return getNumber().shortValue();
        }

        @Override
        public int intValue() {
            return getNumber().intValue();
        }

        @Override
        public long longValue() {
            return getNumber().longValue();
        }

        @Override
        public float floatValue() {
            return getNumber().floatValue();
        }

        @Override
        public double doubleValue() {
            return getNumber().doubleValue();
        }

        @Override
        public boolean booleanValue() {
            return XMLDatatypeUtil.parseBoolean(getLabel());
        }

        @Override
        public BigInteger integerValue() {
            return XMLDatatypeUtil.parseInteger(getLabel());
        }

        @Override
        public BigDecimal decimalValue() {
            return XMLDatatypeUtil.parseDecimal(getLabel());
        }

        @Override
        public XMLGregorianCalendar calendarValue() {
            return XMLDatatypeUtil.parseCalendar(getLabel());
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Literal)) {
                return false;
            }
            final Literal other = (Literal) object;
            if (object.getClass() == this.getClass()) {
                return this.datatype.equals(other.getDatatype()) && equalNumber(other);
            }
            if (object instanceof NumberLiteral || object instanceof BooleanLiteralImpl
                    || object instanceof CalendarLiteralImpl || object instanceof StringLiteral) {
                return false;
            }
            return other.getLabel() == null && this.datatype.equals(other.getDatatype())
                    && stringValue().equals(other.stringValue());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getNumber(), getDatatype());
        }

        @Override
        public String toString() {
            final String label = getLabel();
            final StringBuilder builder = new StringBuilder(label.length() * 2);
            builder.append('"');
            builder.append(label);
            builder.append('"');
            builder.append('^').append('^').append('<');
            builder.append(this.datatype.toString());
            builder.append('>');
            return builder.toString();
        }

    }

    private static final class LongLiteral extends NumberLiteral {

        private static final long serialVersionUID = 1L;

        private final long value;

        LongLiteral(final URI datatype, final long value) {
            super(datatype);
            this.value = value;
        }

        @Override
        Number getNumber() {
            return this.value;
        }

        @Override
        public String stringValue() {
            return Long.toString(this.value);
        }

        @Override
        public byte byteValue() {
            return (byte) this.value;
        }

        @Override
        public short shortValue() {
            return (short) this.value;
        }

        @Override
        public int intValue() {
            return (int) this.value;
        }

        @Override
        public long longValue() {
            return this.value;
        }

        @Override
        public float floatValue() {
            return this.value;
        }

        @Override
        public double doubleValue() {
            return this.value;
        }

        @Override
        public BigInteger integerValue() {
            return BigInteger.valueOf(this.value);
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(this.value);
        }
    }

    private static final class DoubleLiteral extends NumberLiteral {

        private static final long serialVersionUID = 1L;

        private final double value;

        DoubleLiteral(final URI datatype, final double value) {
            super(datatype);
            this.value = value;
        }

        @Override
        Number getNumber() {
            return this.value;
        }

        @Override
        public String stringValue() {
            return Double.toString(this.value);
        }

        @Override
        public byte byteValue() {
            return (byte) this.value;
        }

        @Override
        public short shortValue() {
            return (short) this.value;
        }

        @Override
        public int intValue() {
            return (int) this.value;
        }

        @Override
        public long longValue() {
            return (long) this.value;
        }

        @Override
        public float floatValue() {
            return (float) this.value;
        }

        @Override
        public double doubleValue() {
            return this.value;
        }

        @Override
        public BigDecimal decimalValue() {
            return BigDecimal.valueOf(this.value);
        }

    }

    private static final class BigIntegerLiteral extends NumberLiteral {

        private static final long serialVersionUID = 1L;

        private final BigInteger value;

        BigIntegerLiteral(final URI datatype, final BigInteger value) {
            super(datatype);
            this.value = value;
        }

        @Override
        Number getNumber() {
            return this.value;
        }

        @Override
        public BigInteger integerValue() {
            return this.value;
        }

        @Override
        public BigDecimal decimalValue() {
            return new BigDecimal(this.value);
        }

    }

    private static final class BigDecimalLiteral extends NumberLiteral {

        private static final long serialVersionUID = 1L;

        private final BigDecimal value;

        BigDecimalLiteral(final URI datatype, final BigDecimal value) {
            super(datatype);
            this.value = value;
        }

        @Override
        Number getNumber() {
            return this.value;
        }

        @Override
        public BigInteger integerValue() {
            return this.value.toBigInteger();
        }

        @Override
        public BigDecimal decimalValue() {
            return this.value;
        }

    }

}