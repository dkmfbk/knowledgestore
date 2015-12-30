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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 * @author enrico
 */
public class URIHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(URIHandler.class);
    private static final String indexName = "urimappingindex";
    private static final String extendedURIPropName = "ex";
    private static final TimeValue scrollTimeOut = TimeValue.timeValueMinutes(1);
    private final HashSet<URI> strongCompress;
    private static final char URI_ESCAPE = (char)1;
    private final String[] types;
    private final Client client;
    private final AtomicInteger counter;
    private final BiMap<String, URI> mapper; //<compressed, extended>, compressed is a String rapresentation of a Integer.
      
    public URIHandler(Set<URI> weakCompression, HashSet<URI> strongCompression, Client client) throws IOException{
        
        this.strongCompress = strongCompression;
        this.client = client;
        counter = new AtomicInteger(0);
        types = new String[2];
        types[0] = "s"; //strongCompressed URIs.
        types[1] = "w"; //weakCompressed URIs.
        BiMap<String, URI> map = HashBiMap.create();
        mapper = Maps.synchronizedBiMap(map);
        initMap(weakCompression);
        client.admin().indices().prepareOptimize(indexName).setMaxNumSegments(1).setFlush(true).execute().actionGet();
    }
    
    
//methods called only from the contructor
    /**
     * check if the index exists if not the index is created. 
     * The content of the index is loaded into the mapper.
     * The set of weakCompression nameSpaces is added.
     * @param nameSpaces
     * @throws IOException 
     */
    private void initMap(Set<URI> nameSpaces) throws IOException{
        if(isIndexExists()){ //if the index exists load the content.
            LOGGER.debug("index: " + indexName + " already exists");
            loadMappingFromIndex();            
        }else{ //create it.
            createIndex();
            LOGGER.debug("index: " + indexName + " created");
        }
        addWeakCompressions(nameSpaces);
    }
    
    /**
     * loads the content of the index in the Map.
     */
    private void loadMappingFromIndex(){
        SearchResponse response = client.prepareSearch(indexName).setTypes(types)
                .setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.matchAllFilter()))
                .setScroll(scrollTimeOut).execute().actionGet();
        while(true){ //will stop when the scroll doesn't return any hits.
            for(SearchHit hit : response.getHits().getHits()){
                if(hit.isSourceEmpty())
                    throw new UnsupportedOperationException("deserialization of projected object not supported, URIHandler");
                //else
                String compressedURI = hit.getId();
                Map<String, Object> document = hit.getSource();
                URI extendedURI = new URIImpl((String)document.get(extendedURIPropName));
                int uriInteger = Integer.parseInt(compressedURI);
                if(uriInteger > counter.get()) counter.set(uriInteger);
                mapper.put(compressedURI, extendedURI); //add the couple to the map.
            }
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeOut).execute().actionGet();
            if (response.getHits().getHits().length == 0){
                break;
            }
        }
        counter.incrementAndGet(); //counter is the next id available.
        
        LOGGER.debug("compression: " + mapper.toString());
    }
    
    /**
     * creates the index with name = indexName, the types with the names types and the mapping.
     * @throws IOException 
     */
    private void createIndex() throws IOException{
        CreateIndexRequestBuilder createRequest = client.admin().indices().prepareCreate(indexName)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1).put().build());
        //set the mapping for the types.
        for(String typeName : types){
            XContentBuilder mapping = jsonBuilder().startObject()
                    .field(typeName).startObject()
                    .field("dynamic", "strict")
                    .field("properties").startObject()
                    .field("_timestamp").startObject().field("enabled", false).endObject()
                .field("_all").startObject().field("enabled", false).endObject()
                    .field("norms").startObject().field("enabled", false).endObject()
                    
                    .field(extendedURIPropName).startObject()
                    .field("type", "string")
                    .field("index", "no").endObject()
                    
                    .endObject()
                    .endObject()
                    .endObject();
           
            createRequest.addMapping(typeName, mapping);
            LOGGER.trace("uriHandler index: " + indexName + "." + typeName + " mapping: " + mapping.string());
        }
        CreateIndexResponse createResponse = createRequest.execute().actionGet();
        if(!createResponse.isAcknowledged()){
             throw new RuntimeException("can not create the index: " + indexName); 
        }
    }    
    
    /**
     * adds to the index the missing weakCompressions.
     * @param nameSpaces
     * @throws IOException 
     */
    private void addWeakCompressions(Set<URI> nameSpaces) throws IOException{
        for(URI uri : nameSpaces){
            if(!mapper.containsValue(uri)){ //if the value is not already in the map (and in the index)
                store(uri, types[1]); //store the mapping as weak compression. 
            }
        }
    }
    
    
    
//methods called NOT only from constructor.
    
    /**
     * adds a mapping of the specified type (strong or weak) in the index and in the map.
     * the called must care about syncronization.
     * @param extendedUri the uri to compress.
     * @param type must be in the array types.
     * @return the id of the new mapping. (compressed string that maps the extendedUri)
     * @throws IOException 
     */
    private String store(URI extendedUri, String type) throws IOException{
        String extended = extendedUri.toString();
        XContentBuilder source = jsonBuilder().startObject().field(extendedURIPropName, extended).endObject();
        String id;
        synchronized(this){
            id = Integer.toString(counter.get());
            
            IndexRequest indexReq = new IndexRequest(indexName, type, id).source(source);
            this.waitYellowStatus();
            IndexResponse indexResponse = client.index(indexReq).actionGet();
            
            if(!indexResponse.isCreated())
                throw new IllegalStateException("failed to add the weakCompression of " + source.string() + " in: " + indexName + "." + type);
            this.waitYellowStatus();
            FlushResponse flushResponse = client.admin().indices().prepareFlush(indexName).execute().actionGet();
            
            if(flushResponse.getFailedShards() != 0){ //if there are failures.
                String errorMessage = "flush of the idex: " + indexName + " failed:\n";
                for(ShardOperationFailedException failure : flushResponse.getShardFailures()){
                    errorMessage += "\t shardId: " + failure.shardId() + " ; reason: " + failure.reason() + "\n";
                }
                throw new IllegalStateException(errorMessage); //explode
            }
            
            mapper.put(Integer.toString(counter.get()), extendedUri); //add to the map
            counter.incrementAndGet(); //increment the id.
        }
        return id;
    }
    
    /**
     * 
     * @param extended the uri to check if can be strong compressed
     * @return the strong compressed string (escaped) or null if the string must not be strong compressed.
     * @throws IOException 
     */
    private synchronized String strongCompression(URI extended) throws IOException{
       // if(extended == null) return null;
        String compressedNotEscaped = mapper.inverse().get(extended);
        if(compressedNotEscaped != null)  //if there is already a strong compression in the map.
            return String.valueOf(URI_ESCAPE) + compressedNotEscaped;

        URI namespace = new URIImpl(extended.getNamespace());
        if(!strongCompress.contains(namespace)) //if this uri has not to be strong compressed.
            return null;
        
        //the string must be strong compressed and is not already in the map (and in the index)
        
        //add the strong compression

        compressedNotEscaped =  store(extended, types[0]);
        LOGGER.debug(extended.toString() + " --S--> " + compressedNotEscaped);

        return String.valueOf(URI_ESCAPE) + compressedNotEscaped;
    }
    
    /**
     * encodes the specified uri using the mapper. (opposite of decode)
     * @param uri
     * @return the encoded rapresentation of the uri. 
     * @throws IOException
     */
    public String encode(URI uri) throws IOException{
//        LOGGER.debug("encoding of: " + uri);
        //check if the uri can be strong compressed
        String compressed = strongCompression(uri);
        if(compressed != null){ //if the string has been strongCompressed and escaped.
           // LOGGER.debug("compressed as: " + compressed);
            return compressed; 
        }
        
        
        //if in the map there is no strong compression for the uri, check for weak compression
        URI nameSpace = new URIImpl(uri.getNamespace());
        
        String encodedNotEscape = mapper.inverse().get(nameSpace);
        
        if(encodedNotEscape != null){ //if there is a weak compression
            compressed = String.valueOf(URI_ESCAPE).concat(encodedNotEscape.concat(String.valueOf(URI_ESCAPE))
                    .concat(uri.getLocalName()));
//            LOGGER.debug("compressed as: " + compressed);
            return compressed;
        }
        //if no compression can be done.
        compressed = String.valueOf(URI_ESCAPE).concat(uri.toString());
//        LOGGER.debug("compressed as: " + compressed);
        return compressed;
    }
    
    /**
     * return the extended uri rapresented by the compressed string. (opposite of encode)
     * @param compressed
     * @return 
     */
    public URI decode(String compressed){
        LOGGER.trace("decompression of: " + compressed);
           //if the string is not a URI
        if(compressed.length() < 2 || !(compressed.charAt(0) == URI_ESCAPE)){
            throw new IllegalArgumentException(compressed + " is not a valid URI for decoding. Expected <$escape><number> or <$escape><number><$escape><String> or <$escape><URI>");
        }
        compressed = compressed.substring(1); //remove the first escape character.
        //check if it's strong or weak compressed
       
        if(Character.isDigit(compressed.charAt(0))){
            int pos = compressed.indexOf(Character.toString(URI_ESCAPE), 1);
            if(pos == -1){ //strong compressed
                URI res = mapper.get(compressed);
                LOGGER.trace("decompressed as: " + res);
                return res;
            }
            //it's weak compressed
            String number = compressed.substring(0, pos);
            String nameSpaceStr = mapper.get(number).toString();
            String localNameStr = compressed.substring(pos+1); //discard the escape char.
            URI res = new URIImpl(nameSpaceStr + localNameStr);
            LOGGER.trace("decompressed as: " + res);
            return res;
            }
        //if it's not compressed
        URI res = new URIImpl(compressed);
        LOGGER.trace("decompressed as: " + res);
        return res;
    }
    
    public boolean isEncodedUri(String str){
        return str!= null && str.length()>1 && str.charAt(0) == URI_ESCAPE;
    }
     
    private void waitYellowStatus(){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
    }
    private boolean isIndexExists(){
        this.waitYellowStatus();
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        return response.isExists();
    }
    
    public void printMapping(){
        LOGGER.debug("uri mapping: " + mapper.toString());
    }
}
