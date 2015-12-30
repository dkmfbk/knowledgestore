
package eu.fbk.knowledgestore.elastic;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.vocabulary.KS;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse.Failure;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * implements DataTransaction interface using ElasticSearch API.
 * does not implement transaction.
 */
public final class DataTransactionElastic implements DataTransaction{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTransactionElastic.class);
    private final Client client; //client of the cluster
    
    private BulkProcessor bulk; //for bulk operations
    
    private final ElasticConfigurations configs; //configuration parameters.
    private final MappingHandler mapper; //mapping of the types.
    private final URIHandler uriHandler; //for URI compression.
    private final AtomicReference<RuntimeException> bulkException;
    
    /**
     * @param configs the configurations
     * @param client ElasticSearch client where to send queries ecc.
     * @param mapper mapping of the types.
     */
    public DataTransactionElastic(ElasticConfigurations configs, Client client, MappingHandler mapper, URIHandler uriHandler) {
        this.configs = configs;
        this.client = client;
        this.mapper = mapper;
        this.uriHandler = uriHandler;
        bulkException = new AtomicReference<>();
        bulk = null;
    }
    
    /**
     * maps the URI type to a String, that can be used as a type for the ES index.
     * @param type
     * @return
     */
    private String getTypeAsString(URI type){
        if(type.equals(KS.MENTION)) return "mention";
        if(type.equals(KS.RESOURCE)) return "resource";
        throw new IllegalArgumentException("unknow type: " + type.toString());
    }
    
    
    /**
     * initialization of the bulk.
     * Uses the default values only if bulkSize, flushInterval, concurrentRequest are never been set.
     * Otherwise uses the values set.
     */
    private void initBulk(){
        LOGGER.trace("starting a new bulk");
        BulkProcessor.Builder builder = BulkProcessor.builder(client, new BulkProcessor.Listener(){
            @Override
            public void beforeBulk(long l, BulkRequest br) {
                client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
                LOGGER.trace("starting bulk operation with {} request", br.numberOfActions());
            }
            
            @Override
            public void afterBulk(long l, BulkRequest br, BulkResponse br1) {
                LOGGER.trace("finished bulk operation with {} request", br.numberOfActions());
                if(br1.hasFailures()){
                    String log = "errors in bulk execution:";
                    for(BulkItemResponse item : br1){
                        if(item.isFailed())
                            log += "\n\t id: " + item.getId() + " ; message: " + item.getFailureMessage();
                    }
                    LOGGER.error(log);
                    //bulkException.set(new RuntimeException("failure in response -:" + br1.buildFailureMessage()));
                }
            }
            
            @Override
            public void afterBulk(long l, BulkRequest br, Throwable thrwbl){
                LOGGER.trace("finished bulk operation with {} request and errors", br.numberOfActions());
                bulkException.set(new RuntimeException("Caught exception in bulk: " + br + ", failure: " + thrwbl, thrwbl));
            }
        });
        
        if(configs.getBulkSize() != null) builder.setBulkSize(configs.getBulkSize());
        //  if(configs.getFlushInterval() != null) builder.setFlushInterval(configs.getFlushInterval());
        if(configs.getConcurrentRequests() != -1) builder.setConcurrentRequests(configs.getConcurrentRequests());
        
        bulk = builder.build();
    }
    
    private void checkNotFailed(){
        RuntimeException exception = bulkException.get();
        if(exception != null){
            if(!(exception instanceof IllegalStateException))
                bulkException.set(new IllegalStateException("previous bulk operation failed"));
            throw exception;
        }
    }
    
    /**
     *
     * @param timeout max time that the bulk execution can take.
     * @param unit TimeUnit of timeout
     * @throws InterruptedException
     */
    private void flushBulk(TimeValue time) throws IllegalStateException{
        if(bulk != null){
            LOGGER.debug("flushing bulk");
            try{
                bulk.flush();
                if(!bulk.awaitClose(time.getMillis(), TimeUnit.MILLISECONDS))
                    throw new RuntimeException("bulk request did not completed in " + time.getMillis() + TimeUnit.MILLISECONDS);
                
                bulk = null; //for signal that there is no bulk running.
//refresh of the index for make the documets available for search.
                RefreshResponse fr = client.admin().indices().refresh(new RefreshRequest(configs.getIndexName())).actionGet();
                LOGGER.debug(String.format("Flush: %s failed,  %s successful, %s total",fr.getFailedShards(),fr.getSuccessfulShards(),fr.getTotalShards()));
            }catch(InterruptedException ex){
                throw new IllegalStateException("bulk execution interrupted", ex);
            }
        }
        checkNotFailed();
    }
    
    /**
     * wait until elastic search comes in yellow status.
     */
    private void waitYellowStatus(){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
    }
    
    /**
     * search for the specified ids in the database
     * @param type mention or resource
     * @param ids identifiers of the documents to search.
     * @param properties properties to keep. (null for all)
     * @return the documents found.
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     */
    @Override
    public Stream<Record> lookup(URI type, Set<? extends URI> ids,  @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
        LOGGER.debug("lookup, type: " + type + " ; ids: " + ids + " ; properties: " + properties);
        checkNotFailed();
        if(ids.isEmpty()){
            return Stream.create();
        }
//flush the operation buffered in the bulk.
        this.flushBulk(configs.getBulkTime());
        
        MultiGetRequestBuilder request = client.prepareMultiGet();
        //set the type name where to search the data.
        String typeName = getTypeAsString(type);
        
        for(URI id : ids){ //the projection is done after the get.
            Item item = new MultiGetRequest.Item(configs.getIndexName(), typeName, uriHandler.encode(id));
            request.add(item);
        }
        LOGGER.debug("multiGetRequest: "+ request);
        this.waitYellowStatus();
        
//convert from MultiGetItemResponse[] to Stream
        MultiGetResponse response = request.execute().actionGet();
        ArrayList<Record> records = new ArrayList<>();
        //add records to results.
        for(MultiGetItemResponse item : response) {
            if(item.isFailed()){
                Failure failure = item.getFailure();
                LOGGER.error("failed MultiGet: id: " + failure.getId() + " message: " + failure.getMessage());
            }else{
                GetResponse tmp = item.getResponse();
                Record recToAdd =  Utility.deserialize(tmp, mapper, uriHandler);
                if(recToAdd != null){
                    if(properties != null)
                        recToAdd = recToAdd.retain(Iterables.toArray(properties, URI.class));
                    records.add(recToAdd);
                }
            }
        }
        return Stream.create(records);
    }
    
    
    /**
     * @param condition condition to use for filter the Records.
     * @param partialAccept is a partial filter accepted.
     * @return the filter. null if has not managed to build a filter.
     */
    private FilterBuilder buildFilter(XPath condition, boolean partialAccept) throws IOException{
        LOGGER.debug("buildFilter condition: " + condition + " ; partialAccept: " + partialAccept);
        Map<URI, Set<Object>> map = new HashMap<>();
        XPath remain = condition.decompose(map); //splits the xPath
        
        if((!partialAccept && remain != null) || map.isEmpty()){
            LOGGER.debug("entrySet number: " + map.size());
            return null;
        }
        List<FilterBuilder> filters = new ArrayList<>();
        
        for(Map.Entry el : map.entrySet()){
            LOGGER.debug("analyze key: " + el.getKey());
            String elKey = null;
            if(el.getKey() instanceof URI)
                elKey = uriHandler.encode((URI)el.getKey());
            else{
                //something wrog, it's not a Value
                throw new IllegalArgumentException("found a key in the xPath map that is not a URI");
            }
            
            if(mapper.getValueClass(elKey).equals(byte[].class)){ //byte[] are not pre-filtered. Only post-filter
                LOGGER.debug("byte[], ignore");
                if(!partialAccept) return null;
                //else ignore.
            }else{
                LOGGER.debug("analizing key: " + elKey);
                ArrayList<FilterBuilder> tmpFilters = new ArrayList<>();
                LOGGER.debug("number of orFilters: " + ((Set<Object>)el.getValue()).size());
                for(Object item : (Set<Object>)el.getValue()){
                    FilterBuilder tmpFilter = null;
                    LOGGER.debug("analizing value: " + item.toString() + " ; class: " + item.getClass());
                    if(item instanceof Range){
                        tmpFilter = Utility.buildRangeFilter(elKey, (Range)item, mapper, uriHandler);
                    }else{
                        LOGGER.debug("term filter: " + item.getClass());
                        tmpFilter = Utility.buildTermFilter(elKey, item, mapper, uriHandler);
                    }
                    if(tmpFilter != null){ //if the creation of the filter failed.
                        tmpFilters.add(tmpFilter);
                    }else{
                        LOGGER.debug("creation of the filter on {} failed", condition);
                        if(!partialAccept) return null;
                    }
                }
                filters.add(FilterBuilders.orFilter(Iterables.toArray(tmpFilters, FilterBuilder.class))); //if a document passes 1 of the tmpFilter I keep that
            }
        }
        FilterBuilder mainFilter = null;
        if(!filters.isEmpty())
            mainFilter = FilterBuilders.andFilter(Iterables.toArray(filters, FilterBuilder.class)); //a documet has to pass all the filters.
        
        return mainFilter;
    }
    
    /**
     * returns all the documents in the database that match the conditions
     * @param type mention or resource
     * @param condition condition to match
     * @param properties properties of the documents to keep
     * @return the documents found.
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     */
    @Override
    public Stream<Record> retrieve(URI type, @Nullable XPath condition, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
        checkNotFailed();
        LOGGER.debug("retrieve type: " + type + " ; condition: " + condition + " ; properties: " + properties);
        
        this.flushBulk(configs.getBulkTime());
        
        String typeName = getTypeAsString(type);
        
        SearchRequestBuilder responseBuilder;
        QueryBuilder mainQuery;
        //if there are no conditions I take all the documents.
        if(condition == null){ //matchAll query.
            LOGGER.debug("match all query");
            mainQuery = QueryBuilders.constantScoreQuery(FilterBuilders.matchAllFilter());
            
        }else{ //if there are some conditions, take only the documents that satisfy all of them. pre-checking
            LOGGER.debug("try to build a filter of condition: " + condition);
            FilterBuilder filter = buildFilter(condition, true);
            if(filter == null){ //if the build of the filter fails, match all and post-filter.
                LOGGER.debug("prefilter failed");
                filter = FilterBuilders.matchAllFilter();
            }
            mainQuery = QueryBuilders.constantScoreQuery(filter);
        }
        
        LOGGER.debug("Query: " + mainQuery);
        responseBuilder = client.prepareSearch(configs.getIndexName()).setTypes(typeName)
                .setQuery(mainQuery).setScroll(configs.getTimeout());
        
        this.waitYellowStatus();
        
        SearchResponse response = responseBuilder.execute().actionGet();
        Stream<Record> res = new SearchResponseStream(response, client, configs.getTimeout(), mapper, uriHandler);
        
        if(condition != null){
            LOGGER.debug("post-filtering: " + condition);
            res = res.filter(condition.asPredicate(), 0); //post-checking
        }
        
        if(properties != null){ //projection
            final URI[] propURIs = Iterables.toArray(properties, URI.class);
            res = res.transform((Record r) -> {return r.retain(propURIs);}, 0);
        }        
        return res;
    }
    
    /**
     * the number of documents matching the conditions
     * @param type mention or resource
     * @param condition condition to match
     * @return the number of documents matching the conditions
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     */
    @Override
    public long count(URI type, @Nullable XPath condition) throws IOException, IllegalArgumentException, IllegalStateException {
        checkNotFailed();
        LOGGER.debug("count type: " + type + " ; condition: " + condition);
        
//flush the bulk.
        this.flushBulk(configs.getBulkTime());
        
        
        String typeName = getTypeAsString(type);
        
        CountRequestBuilder responseBuilder = null;
        //if there are no conditions I take all the documents (from, size).
        if(condition == null){ //matchAll query.
            responseBuilder = client.prepareCount(configs.getIndexName()).setTypes(typeName)
                    .setQuery(QueryBuilders.matchAllQuery());
            
        }else{ //if there are some conditions, take only the documents that satisfy all of them. pre-checking
            FilterBuilder mainFilter = buildFilter(condition, false);
            if(mainFilter != null){
                QueryBuilder mainQuery = QueryBuilders.constantScoreQuery(mainFilter);
                responseBuilder = client.prepareCount(configs.getIndexName()).setTypes(typeName)
                        .setQuery(mainQuery);
            }else{ //if we can't pre-filter all, do a retrive with 0 properties.
                this.waitYellowStatus();
                LOGGER.debug("can not prefilter all, have to do a matchall and postfilter");
                Stream<Record> stream = this.retrieve(type, condition, new HashSet<>());
                return stream.count();
            }
        }
        this.waitYellowStatus();
        CountResponse response = responseBuilder.execute().actionGet();
        return response.getCount();
    }
    
    @Override
    public Stream<Record> match(Map<URI, XPath> conditions, Map<URI, Set<URI>> ids, Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * stores the specified record in the database.
     * @param type mention or resource
     * @param record record to store
     * @throws IOException
     * @throws IllegalStateException
     */
    @Override
    public void store(URI type, Record record) throws IOException, IllegalStateException {
        checkNotFailed();
        if(bulk == null) //start a new bulk if it's not already started.
            this.initBulk();
        
        String typeName = getTypeAsString(type);

        XContentBuilder source = Utility.serialize(record, uriHandler);
        LOGGER.trace("storing\n\trecord: " + record.toString(null, true) + "\n\t-> serialized: " + source.string());
        IndexRequest indexRequest = new IndexRequest(configs.getIndexName(), typeName, uriHandler.encode(record.getID())).source(source);
        UpdateRequest updateRequest = new UpdateRequest(configs.getIndexName(), typeName,  uriHandler.encode(record.getID())).doc(source).upsert(indexRequest);
        bulk.add(updateRequest);
    }
    
    /**
     * removes a document from the database given it's id
     * @param type mention or resource
     * @param id identifier of the document to delete
     * @throws IOException
     * @throws IllegalStateException
     */
    @Override
    public void delete(URI type, URI id) throws IOException, IllegalStateException {
        checkNotFailed();
        LOGGER.trace("delete document type: " + type + " ; id: " + id);
        
        String typeName = getTypeAsString(type);
        String indexName = configs.getIndexName();
        String idStr = uriHandler.encode(id);
        DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, idStr);
        
        if(bulk == null) //start a new bulk if it's not already started.
            this.initBulk();
        bulk.add(deleteRequest);
    }
    
    /**
     * end the "transaction".
     * @param commit
     * @throws DataCorruptedException
     * @throws IOException
     * @throws IllegalStateException
     */
    @Override
    public void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException {
        checkNotFailed();
        LOGGER.debug("end");
        if(commit){
            LOGGER.debug("flushing pending operation");
            this.flushBulk(configs.getBulkTime());
            LOGGER.debug("done");
        }
    }
    
    /**
     * merges the segment of the index for reach the specified segment number
     * @param segNumber at least 1.
     */
    public void optimizeIndex(int segNumber){
        client.admin().indices().prepareOptimize(configs.getIndexName()).setMaxNumSegments(segNumber).setFlush(true).execute().actionGet();
    }
    
    public void deleteAllIndexContent(){
        this.waitYellowStatus();
        if(!client.admin().indices().prepareDelete("*").execute().actionGet().isAcknowledged())
            throw new RuntimeException("can not delete the index ("+configs.getIndexName()+") content ");
    }
    public void deleteAll(){
        this.waitYellowStatus();
        DeleteIndexResponse response = null;
        try{
            response = client.admin().indices().delete(new DeleteIndexRequest(configs.getIndexName())).actionGet();
        }catch(IndexMissingException ex){
            return;
        }
        if(!response.isAcknowledged())
            throw new RuntimeException("can not delete the index: " + configs.getIndexName());
    }
}
