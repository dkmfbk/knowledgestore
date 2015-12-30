/*
* Copyright 2015 FBK-irst.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package eu.fbk.knowledgestore.elastic;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * If the Record format is changed all (more or less) the methods in this Class must be changed.
 *
 * @author enrico
 */
public final class Utility {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);
    private static final String ID_PROPERTY_NAME = "ID"; //property name of the Record id.
    private static final char URI_ESCAPE = (char) 1; //escape character for identify URIs or normal Strings.

    private static String encodeString(URI uri, URIHandler handler) throws IOException {
        if (uri == null) {
            return null;
        }
        return handler.encode(uri);
    }

    private static Object decodeString(String str, URIHandler handler) {
        Object res = str;
        if (handler.isEncodedUri(str)) {
            res = handler.decode(str);
        }
        return res;
    }

    /**
     * checks if the 2 Records contains the same Properties : Values.
     *
     * @param exp
     * @param res
     * @return
     */
    public static boolean areEquals(Record exp, Record res) {
        if (!exp.getID().equals(res.getID())) {
            LOGGER.info("differend ids expected: " + exp.getID() + " ; found: " + res.getID());
        }
        List<URI> properties = exp.getProperties();
        List<URI> resProperties = res.getProperties();

        if (properties.size() != resProperties.size()) {
            LOGGER.info("different number of properties; expected: " + properties.size() + " found: " + resProperties
                    .size());
            return false;
        } //must have the same number of properties.

        properties.sort(Data.getTotalComparator());
        resProperties.sort(Data.getTotalComparator());

        for (int i = 0; i < properties.size(); i++) { //must have the same properties' values.
            URI prop = properties.get(i);
            URI resProp = resProperties.get(i);
            if (!prop.equals(resProp)) {
                LOGGER.debug("different property name; expected: " + prop + " found: " + resProp);
                return false;
            }

            List<Object> expValues = exp.get(prop);
            List<Object> resValues = res.get(prop);

            if (resValues == null && expValues != null) {
                LOGGER.debug("found null values expected not null");
                return false;
            }

            if (resValues != null && expValues == null) {
                LOGGER.debug("found not null values expected null");
                return false;
            }

            if (resValues != null && expValues != null) { //if both null assume that are equals.
                if (resValues.size() != expValues.size()) {
                    LOGGER.debug(
                            "different number of values under " + prop + " expected: " + expValues.size() + " found: "
                                    + resValues.size());
                    return false;
                } //same number of values.

                Iterator<Object> expIt = expValues.iterator();
                Iterator<Object> resIt = resValues.iterator();
                while (expIt.hasNext()) {
                    Object expValue = expIt.next();
                    Object resValue = resIt.next();
                    if (expValue instanceof Record) { //it's a Record or it's a Value
                        if (!areEquals((Record) expValue, (Record) resValue)) {
                            LOGGER.debug("different Records under " + prop);
                            return false;
                        }
                    } else if (expValue instanceof Value) { //Literal or URI

                        if (expValue instanceof URI) {
                            if (!((URI) expValue).equals((URI) resValue)) {
                                LOGGER.debug("differet URIs; expected: " + ((URI) expValue).toString() + " found: "
                                        + ((URI) resValue).toString());
                                return false;
                            }
                        } else { //instanceof Literal
                            if (expValue instanceof Literal) {
                                if (!((Literal) expValue).equals((Literal) resValue)) {
                                    LOGGER.debug("differet Literals; expected: " + ((Literal) expValue).toString()
                                            + " found: " + ((Literal) resValue).toString());
                                    return false;
                                }
                            } else {
                                throw new IllegalArgumentException(
                                        "unknow type in Record property, Value subclass that is neither URI nor Literal: "
                                                + expValue.getClass());
                            }
                        }

                    } else {
                        throw new IllegalArgumentException("unknow type in Record property: " + expValue.getClass());
                    }
                }
            }
        }
        return true;
    }

    /**
     * the 2 input Lists must be sorted.
     *
     * @param expResult
     * @param result
     * @return
     */
    public static boolean areEquals(List<Record> expResult, List<Record> result) {
        LOGGER.debug(
                "\n\nexpected(" + expResult.size() + "): " + expResult + "\n\nfound(" + result.size() + "): " + result);
        if (expResult.size() != result.size()) { //if they have a different number of entries.
            LOGGER.debug(
                    "the 2 List<Record> have a different number of entries expected: " + expResult.size() + " , found: "
                            + result.size());
            return false;
        }

        for (int i = 0; i < result.size(); i++) {
            if (!areEquals(expResult.get(i), result.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * maps all the XMLSchema types in the java Class that will be stored in ES.
     *
     * @param literal literal to serialize under the attribute name propStr in the builder.
     * @param propStr attribute name, if null no attribute name will be set.
     * @param builder
     * @return
     * @throws IOException
     */
    private static XContentBuilder literalSerializer(Literal literal, XContentBuilder builder) throws IOException {
        final URI datatype = literal.getDatatype();
        if (datatype == null || datatype.equals(XMLSchema.STRING)) {
            builder.value(Data.convert(literal, String.class));
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            builder.value(Data.convert(literal, Boolean.class));
        } else if (datatype.equals(XMLSchema.DATE) || datatype.equals(XMLSchema.DATETIME)) {
            builder.value(Data.convert(literal, Date.class));
        } else if (datatype.equals(XMLSchema.INT)) {
            builder.value(Data.convert(literal, Integer.class));
        } else if (datatype.equals(XMLSchema.LONG)) {
            builder.value(Data.convert(literal, Long.class));
        } else if (datatype.equals(XMLSchema.DOUBLE)) {
            builder.value(Data.convert(literal, Double.class));
        } else if (datatype.equals(XMLSchema.FLOAT)) { //ES will convert to Double
            builder.value(Data.convert(literal, Float.class));
        } else if (datatype.equals(XMLSchema.SHORT)) { //ES will convert to int
            builder.value(Data.convert(literal, Short.class));
        } else if (datatype.equals(XMLSchema.BYTE)) { //ES will convert to int
            builder.value(Data.convert(literal, Byte.class));
        } else if (datatype.equals(XMLSchema.DECIMAL)) { //saved as Binary
            String tmp = literalToString(literal);
            builder.value(tmp.getBytes(Charsets.UTF_8));
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            String tmp = literalToString(literal);
            builder.value(tmp.getBytes(Charsets.UTF_8));
        } else if (datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.NON_POSITIVE_INTEGER)
                || datatype.equals(XMLSchema.NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.POSITIVE_INTEGER)) { //BigInteger saved as Binary
            String tmp = literalToString(literal);
            builder.value(tmp.getBytes(Charsets.UTF_8)); // infrequent integer cases
        } else if (datatype.equals(XMLSchema.NORMALIZEDSTRING) || datatype.equals(XMLSchema.TOKEN)
                || datatype.equals(XMLSchema.NMTOKEN) || datatype.equals(XMLSchema.LANGUAGE)
                || datatype.equals(XMLSchema.NAME) || datatype.equals(XMLSchema.NCNAME)) {
            String tmp = literalToString(literal);
            builder.value(tmp.getBytes(Charsets.UTF_8));// infrequent string cases, saved as String
        } else {
            throw new IllegalArgumentException("can not serialize this literal: " + literal);
        }
        return builder;
    }

    /**
     * @param elKey  string that rapresents the property name (string of the URI)
     * @param item   value that the record must have in the field of the elKey
     * @param mapper mapping of the properties in ES. can not make filters on byteArray.
     * @return if the filter has not be created returns null else the created filter
     */
    public static FilterBuilder buildTermFilter(String elKey, Object item, MappingHandler mapper, URIHandler handler)
            throws IOException {
        LOGGER.debug("creating term filter: " + elKey + " = " + item);

        // Class<?> itemClass = mapper.getValueClass(elKey);
        //item = Data.convert(item, itemClass);
        if (item instanceof URI) {
            String encodedURI = encodeString((URI) item, handler);
            LOGGER.debug("item is a URI: " + encodedURI);
            return FilterBuilders.queryFilter(QueryBuilders.matchPhraseQuery(elKey, encodedURI));
            //return FilterBuilders.termFilter(elKey, encodedURI);
        }
        if (item instanceof String) { //is mapped in ES as a String -> can be a URI or a String.
            LOGGER.debug("item is a string: " + item);
            //the termFilter works only if the field is analyzed.
            //res = FilterBuilders.termFilter(elKey, item);
            return FilterBuilders.queryFilter(QueryBuilders.matchPhraseQuery(elKey, item.toString()));
        }
        if (item instanceof Date) {
            LOGGER.debug("item is a Date: " + item);
            return FilterBuilders.termFilter(elKey, (Date) item);
        }
        if (item instanceof Boolean) {
            LOGGER.debug("item is a Boolean: " + item);
            return FilterBuilders.termFilter(elKey, (boolean) item);
        }

        if (item instanceof Number) {
            if (item instanceof Short) {
                LOGGER.debug("item is a Short: " + item);
                return FilterBuilders.termFilter(elKey, (short) item);
            }
            if (item instanceof Byte) {
                LOGGER.debug("item is a Byte: " + item);
                return FilterBuilders.termFilter(elKey, (byte) item);
            }
            if (item instanceof Integer) {
                LOGGER.debug("item is a Integer: " + item);
                return FilterBuilders.termFilter(elKey, (int) item);
            }
            if (item instanceof Double) {
                LOGGER.debug("item is a Double: " + item.getClass());
                return FilterBuilders.termFilter(elKey, (double) item);
            }
            if (item instanceof Float) {
                LOGGER.debug("item is a Float: " + item);
                return FilterBuilders.termFilter(elKey, (float) item);
            }
            if (item instanceof Long) {
                LOGGER.debug("item is a Long: " + item);
                return FilterBuilders.termFilter(elKey, (long) item);
            } else {
                LOGGER.debug("found unhandled Number type in the xPath conditions: {} ; can not create the filter.",
                        item.getClass());
                return null;
            }
        }
        LOGGER.debug("found unhandled type in the xPath conditions: {} ; can not create the filter.", item.getClass());
        return null;
    }

    /**
     * creates a RangeFilter with the Range item, returns null if the filter can not be created.
     *
     * @param res  RangeFilterBuilder
     * @param item Range item to insert in the Builder
     * @return
     */
    private static RangeFilterBuilder setFromToRange(RangeFilterBuilder res, Range item, Class<?> itemClass,
            URIHandler handler) throws IOException {
        Object lower = null;
        Object upper = null;
        if (item.hasLowerBound()) { //if it has a lowerBoud set lower
            lower = item.lowerEndpoint();
            if (lower instanceof URI) {
                lower = encodeString((URI) lower, handler);
            }
            lower = Data.convert(lower, itemClass);
            LOGGER.debug("FROM: " + lower.toString() + " class: " + lower.getClass());
        }
        if (item.hasUpperBound()) { //if it has an upperBoud set upper
            upper = item.upperEndpoint();
            if (upper instanceof URI) {
                upper = encodeString((URI) upper, handler);
            }
            upper = Data.convert(upper, itemClass);
            LOGGER.debug("TO: " + upper.toString() + " class: " + upper.getClass());
        }

        if (lower == null && upper == null) {
            LOGGER.debug("upper bound and lower bound are both null");
            return null;
        }

        //Handle the Number.
        if (lower instanceof Number || upper instanceof Number) { //or becouse: null instanceof <Class> = false
            //if one of lower or upper it's an "int" (Short, Byte, Integer, Long)
            if (lower instanceof Short || lower instanceof Byte || lower instanceof Integer || lower instanceof Long
                    || upper instanceof Short || upper instanceof Byte || upper instanceof Integer
                    || upper instanceof Long) {
                LOGGER.debug("integer");
                if (lower != null) //set the from only if there is a lower bound
                {
                    res = res.from(((Number) lower).intValue());
                }
                if (upper != null) //set the to only if there is a upper bound
                {
                    res = res.to(((Number) upper).intValue());
                }
                //if it's a decimal number: float or double.
            } else if (lower instanceof Float || lower instanceof Double || upper instanceof Float
                    || upper instanceof Double) {
                LOGGER.debug("double");
                if (lower != null) {
                    res = res.from(((Number) lower).doubleValue());
                }
                if (upper != null) {
                    res = res.to(((Number) upper).doubleValue());
                }
            } else { //it's a unhandled Number type
                LOGGER.warn("can not build a Range filter with this Number class {}", item.getClass());
                return null;
            }
            //if it's not a Number.
        } else {
            if (lower instanceof String
                    || upper instanceof String) { //lexicograph order: ES does a term query for every String in the Range! Very slow.
                LOGGER.debug("string");
                if (lower != null) {  //set the from only if there is a lower bound
                    res = res.from((String) lower);
                }
                if (upper != null) { //set the to only if there is a upper bound
                    res = res.to((String) upper);
                }

            } else if (lower instanceof Date || upper instanceof Date) {
                LOGGER.debug("date");
                if (lower != null) {
                    LOGGER.debug("lower: " + lower);
                    res = res.from(((Date) lower));
                }
                if (upper != null) {
                    LOGGER.debug("upper: " + upper);
                    res = res.to(((Date) upper));
                }

            } else {
                if (lower instanceof Boolean || upper instanceof Boolean) {
                    LOGGER.debug("boolean");
                    if (lower != null) {
                        LOGGER.debug("lower: " + lower);
                        res = res.from(((boolean) lower));
                    }
                    if (upper != null) {
                        LOGGER.debug("upper: " + upper);
                        res = res.to(((boolean) upper));
                    }
                } else {
                    LOGGER.warn("can not build a Range filter with this class {}", item.getClass());
                    return null;
                }
            }
        }

        //include or exclude the bounds.
        if (lower != null) {
            LOGGER.debug("include lower: " + item.lowerBoundType().equals(BoundType.CLOSED));
            res = res.includeLower(item.lowerBoundType().equals(BoundType.CLOSED));
        }
        if (upper != null) {
            LOGGER.debug("include upper: " + item.upperBoundType().equals(BoundType.CLOSED));
            res = res.includeUpper(item.upperBoundType().equals(BoundType.CLOSED));
        }
        return res;
    }

    /**
     * Tries to build a Filter: the specified property (elKey) must match the Range item.
     * Returns null if the method wasn't able to create filter.
     *
     * @param elKey  property name.
     * @param item   Range object.
     * @param mapper
     * @return the builder.
     */
    public static FilterBuilder buildRangeFilter(String elKey, Range item, MappingHandler mapper, URIHandler handler)
            throws IOException {
        Preconditions.checkNotNull(item);
        LOGGER.debug("range filter on " + elKey);
        RangeFilterBuilder res = FilterBuilders.rangeFilter(elKey);
        LOGGER.debug("set from-to");
        return setFromToRange(res, item, mapper.getValueClass(elKey), handler);
    }

    /**
     * @param value      Object to serialize in the builder under the property name propString
     * @param propString name of the attribute, if null no attribute name will be set.
     * @param builder    XContentBuilder where to insert the data.
     * @return the builder.
     * @throws IOException
     */
    private static XContentBuilder singleObjectSerializer(Object value, XContentBuilder builder, URIHandler handler)
            throws IOException {
        if (value instanceof Value) { //Value or Record
            if (value instanceof URI) { //a Value can be a URI or a Literal
                builder.value(encodeString((URI) value, handler)); //encode of the URI as a String.
            } else if (value instanceof Literal) //a Literal can be a Object of different types.
            {
                builder = literalSerializer((Literal) value, builder);
            }

            return builder;
        }
        if (value instanceof Record) { //recursion.
            builder.startObject();
            String id = encodeString(((Record) value).getID(), handler);
            if (id != null) {
                builder.field(ID_PROPERTY_NAME, id);
            }
            builder = serialize((Record) value, builder, handler).endObject();
            return builder;
        }
        throw new UnsupportedOperationException(
                "can not serialize this class of Objects: " + value.getClass() + "  ; value: " + value.toString());
    }

    /**
     * @param values     set of values to insert under the property name propString in the builder.
     * @param propString property name
     * @param builder    XContentBuilder where to insert the data
     * @return the builder with the new data.
     * @throws IOException
     */
    private static XContentBuilder collectionSerializer(Collection<?> values, XContentBuilder builder,
            URIHandler handler) throws IOException {
        for (Object obj : values) {
            builder = singleObjectSerializer(obj, builder,
                    handler); //add every element of the collection in the builder.
        }
        return builder;
    }

    /**
     * @param record     record to serialize
     * @param propString property name of the record, null if it's not needed.
     * @param builder    builder where to insert the serialized object, if null a new one will be created.
     * @return the builder with the serialized object.
     * @throws IOException
     */
    private static XContentBuilder serialize(Record record, XContentBuilder builder, URIHandler handler)
            throws IOException {
        String propString;
        for (URI property : record.getProperties()) {
            Object value = record.get(property); //property value.
            propString = encodeString(property, handler); //name of the property as a string.

            if (((Collection) value).size() == 1) { //if it's a single object get it from the Collection.
                value = ((Collection) value).iterator().next();
            }
            if (value instanceof Collection) { //if the value is a set of Object
                builder.startArray(propString);
                builder = collectionSerializer((Collection) value, builder, handler);
                builder.endArray();

            } else { //if it's only 1 Object (item).
                builder.field(propString);
                builder = singleObjectSerializer(value, builder, handler);
            }
        }
        return builder;
    }

    /**
     * @param record  record to serialize in the XContentBuilder
     * @param handler
     * @return XContentBuilder (json) with all the data of the record.
     * @throws IOException
     */
    public static XContentBuilder serialize(Record record, URIHandler handler) throws IOException {
        if (record == null) {
            return null;
        }
        XContentBuilder builder = jsonBuilder().startObject();
        builder = serialize(record, builder, handler);
        builder.endObject();
        return builder;
    }

    /**
     * this method depends from the serializeLiteral method.
     *
     * @param value Object to deserialize
     * @return the deserialized object
     */
    private static Object deserializeSingleObject(Object value, String propName, MappingHandler mapper,
            URIHandler handler) throws IOException {
        Class<?> valueClass = mapper.getValueClass(propName);
        if (valueClass.equals(Record.class)) { //if it's a nested record, recursion.
            return deserialize((Map) value, Record.create(), mapper, handler);
        }
        if (valueClass.equals(byte[].class)) { //if it's binary, read it as String and convert to Value.
            String res;
            //if(value instanceof String){
            byte[] valueByte = Base64.getDecoder().decode(((String) value));
            res = new String(valueByte, Charsets.UTF_8);
            // }
            /*
            //this is executed only if ES does projection.
            else{
            LOGGER.debug("\tbytesArray");
            res = ((BytesArray)value).toUtf8();
            }
            */
            return Utility.stringToLiteral(res);
        }
        if (valueClass.equals(String.class)) { //can be a URI or a String
            return Utility.decodeString((String) value, handler);
        }
        //can be a Number, Boolean, Date
        return Data.convert(value, valueClass);
    }

    /**
     * calls the deserializeSingleObject on every Object in the Collection, stores the result in another Collection and then return this one.
     *
     * @param values set of values
     * @return set of converted(deserialized) values.
     */
    private static Collection<?> deserializeCollection(Collection<?> values, String propName, MappingHandler mapper,
            URIHandler handler) throws IOException {
        ArrayList<Object> res = new ArrayList<>();
        for (Object obj : values) {
            res.add(deserializeSingleObject(obj, propName, mapper, handler));
        }
        return res;
    }

    /**
     * transforms a Map in a Record
     *
     * @param source Map where to take the data
     * @param res    Record where to insert the data.
     * @return the Record.
     */
    private static Record deserialize(Map<String, Object> source, Record res, MappingHandler mapper, URIHandler handler)
            throws IOException {
        if (source == null) {
            return res;
        }

        for (String propStr : source.keySet()) { //for each property
            if (propStr.equals(ID_PROPERTY_NAME)) {
                res.setID((URI) decodeString((String) source.get(ID_PROPERTY_NAME), handler));
            } else {
                URI propName = handler.decode(propStr);
                Object value = source.get(propStr);
                if (value instanceof Collection) {
                    res.add(propName, deserializeCollection((Collection) value, propStr, mapper, handler));
                } else {
                    res.add(propName, deserializeSingleObject(value, propStr, mapper, handler));
                }
            }
        }
        return res;
    }

    /**
     * Transforms the GetResponse object into a Record.
     *
     * @param response GetResponse object that contains a document of the query's result.
     * @param mapper   mapping of the types.
     * @return the information in the response object serialized in a Record object.
     */
    public static Record deserialize(GetResponse response, MappingHandler mapper, URIHandler handler)
            throws IOException {
        if (response == null) {
            LOGGER.debug("null GetReponse");
            return null;
        }
        if (!response.isExists()) {
            LOGGER.trace("response not exists");
            return null;
        }

        Record res = Record.create();
        res.setID(handler.decode(response.getId()));
        //should enter here only if ES has performed some projection.
        if (response.isSourceEmpty()) {
            return res;
            //    throw new UnsupportedOperationException("deserialization of projected object not supported");
            /*
            //this is a example of implementation for deserialization of ES projected objects.
            Set<String> keys = response.getFields().keySet(); //set of property names
            Object value = null;
            for(String key : keys){
            List<Object> values = response.getField(key).getValues();
            
            if(values.size() == 1){
            value = deserializeSingleObject(values.iterator().next(), key, mapper);
            }else{
            value = deserializeCollection(values, key, mapper);
            }
            //add the binding key : value to the record
            res.add((URI)stringToValue(key), value);
            }
            return res;
            */
        }
        //if(!response.isSourceEmpty())
        return deserialize(response.getSource(), res, mapper, handler);
    }

    /**
     * Transforms the SearchHit object into a Record.
     *
     * @param response SearchHit object that contains a document of the query's result.
     * @param mapper
     * @return the information in the response object serialized in a Record object.
     */
    public static Record deserialize(SearchHit response, MappingHandler mapper, URIHandler handler) throws IOException {
        if (response == null) {
            return null;
        }
        Record res = Record.create();
        res.setID(handler.decode(response.getId()));
        if (response.isSourceEmpty()) {
            throw new UnsupportedOperationException("deserialization of projected object not supported");
            /*
            Set<String> keys = response.getFields().keySet(); //set of property names
            Object value = null;
            for(String key : keys){
            List<Object> values = response.field(key).getValues();
            
            if(values.size() == 1){
            value = deserializeSingleObject(values.iterator().next(), key, mapper);
            }else{
            value = deserializeCollection(values, key, mapper);
            }
            //add the binding key : value to the record
            res.add((URI)stringToValue(key), value);
            }
            return res;
            */
        }
        //if(!resposne.isSourceEmpty())
        return deserialize(response.getSource(), res, mapper, handler);
    }

    /**
     * transforms a Literal in a String
     *
     * @param obj object to transform in String
     * @return
     */
    protected static String literalToString(Literal obj) {
        // return Data.toString(obj, Data.getNamespaceMap());
        return Data.toString(obj, null);
    }

    /**
     * transforms the given String in a Value.
     *
     * @param s String to transform in a Value
     * @return
     */
    protected static Literal stringToLiteral(String s) throws IOException {
        return (Literal) Data.parseValue(s, null);
    }
}
