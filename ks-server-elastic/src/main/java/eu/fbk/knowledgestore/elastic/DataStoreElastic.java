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
import com.google.common.io.Files;
import com.google.common.io.Resources;
import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.ParseException;
import eu.fbk.knowledgestore.datastore.DataStore;
import eu.fbk.knowledgestore.datastore.DataTransaction;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class DataStoreElastic implements DataStore{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreElastic.class);
    private Node node; //this will be null if it's a TransportClient (-> not a local node, but connected to a remote node)
    private Client client;
    private final ElasticConfigurations configs;
    private MappingHandler mapper;
    private URIHandler uriHandler;
    
    public DataStoreElastic(String path){
        configs = new ElasticConfigurations(path);
        mapper = null;
        node = null;
        client = null;
    }
    
    @Override
    public void init() throws IOException, IllegalStateException {
        LOGGER.debug("dataStore init");
        TransportAddress[] addresses = configs.getAddresses();
        if(addresses == null || addresses.length == 0){
            node = nodeBuilder().settings(configs.getNodeSettings()).node();
            
            this.client = node.client();
            LOGGER.info("starting a local node");
        }else{
            node = null;
            this.client = new TransportClient(configs.getNodeSettings()).addTransportAddresses(addresses);
            LOGGER.info("starting transportClient");
        }
        try{
        mapper = createIndex(client);
        uriHandler.printMapping();
        LOGGER.debug("init done; node: " + node + " ; client: " + client + " ; mapper: " + mapper);
        }catch(Exception ex){
            LOGGER.error("errore nella createIndex: " + ex);
        }
    }
    
    @Override
    public DataTransaction begin(boolean readOnly) throws DataCorruptedException, IOException, IllegalStateException {
        LOGGER.debug("dataStore begin");
        if(configs == null || client == null || mapper == null || uriHandler == null)
            throw new IllegalStateException("can not start a transaction object with null values, have you called the init?\n" +
                    "configs:" + configs + " ; client: " + client + " ; mapper: " + mapper + " ; uriHandler: " + uriHandler);
        return new DataTransactionElastic(configs, client, mapper, uriHandler);
    }
    
    /**
     * merge all the segments in 1. If a segment becomes too large, it may cause problems.
     */
    public void optimize(){
        client.admin().indices().prepareOptimize(configs.getIndexName()).setMaxNumSegments(1).setFlush(true).execute().actionGet();
    }
    
    
    @Override
    public void close() {
        if(node != null){
            LOGGER.debug("close local node");
            node.close();
        }else{
            LOGGER.debug("close transportClient");
            ((TransportClient)client).close();
        }
        node = null;
    }
    
    private void waitYellowStatus(){
        LOGGER.debug("wait yellow status...");
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        LOGGER.debug("done");
    }
    
    private boolean isIndexExists(String indexName){
        this.waitYellowStatus();
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        return response.isExists();
    }
    
    /**
     * creates an index with the name indexName, and sets the proper mapping for the different types.
     * it takes the mappings from the specified files. return the mapping of the properties.
     * @param client client of the elasticsearch node.
     */
    private synchronized MappingHandler createIndex(Client client) throws IOException{
        String[] types = new String[2];
        types[0] = "resource";
        types[1] = "mention";
        
        HashSet<URI> setWeakCompr = new HashSet<>();
        //add the user defined weak compressed namespaces.
        String weakCompressionPath = configs.getWeakCompressionPath(); 
        if(weakCompressionPath != null)
            setWeakCompr = readNamespaceSetFromFile(configs.getWeakCompressionPath(), setWeakCompr);
        HashSet<URI> setStrongCompr = new HashSet<>();
        
        if(!isIndexExists(configs.getIndexName())){
            
            String[] jsons = new String[2];
            jsons[0] = readMappingFromFile(configs.getResourceMapping());
            
            jsons[1] = readMappingFromFile(configs.getMentionMapping());
            
            //compress the jsons.
            XContentParser parser0 = XContentFactory.xContent(XContentType.JSON).createParser(jsons[0]);
            Map<String, Object> map0 = parser0.map();
            
            XContentParser parser1 = XContentFactory.xContent(XContentType.JSON).createParser(jsons[1]);
            Map<String, Object> map1 = parser1.map();
            //read the properties name from the mapping. These URI are always strong compressed.
            //properties of the sources
            setStrongCompr = extractLeafProperties(map0, setStrongCompr);
            //properties of the mentions
            setStrongCompr = extractLeafProperties(map1, setStrongCompr);
            //add the user defined namespaces to the set.
            String strongCompressionPath = configs.getStrongCompressionPath();
            if(strongCompressionPath != null)
                setStrongCompr = readNamespaceSetFromFile(strongCompressionPath, setStrongCompr);
            LOGGER.debug("strongSet: " + setStrongCompr);
                      
//init the uriHandler for uri management.
            uriHandler = new URIHandler(setWeakCompr, setStrongCompr, client);

            //apply the uriHandler compression to the mapping properties.
            LOGGER.debug("compressing mappings");
            long startTime = System.currentTimeMillis();
            map0 = compressMapping(map0);
            map1 = compressMapping(map1);
            LOGGER.debug("mapping compressed in: " + (System.currentTimeMillis()-startTime) + " ; result:\n\n");
            LOGGER.debug("resource mapping map: " + map0);
            LOGGER.debug("mention mapping map: " + map1 +"\n\n\n");
            jsons[0] = XContentFactory.jsonBuilder().map(map0).string();
            jsons[1] = XContentFactory.jsonBuilder().map(map1).string();
            try{
                CreateIndexRequestBuilder request = client.admin().indices().prepareCreate(configs.getIndexName());
                setMappings(request, jsons, types);
                CreateIndexResponse response = request.execute().actionGet();
                if(!response.isAcknowledged()){
                    throw new RuntimeException("can not create the index: " + configs.getIndexName());
                }
                LOGGER.info("index {} created", configs.getIndexName());
            }catch(IndexAlreadyExistsException ex){
                LOGGER.debug("index {} already exists, isIndexExists failed", configs.getIndexName());
            }
        }else{
            uriHandler =  uriHandler = new URIHandler(setWeakCompr, setStrongCompr, client);
            LOGGER.info("index {} already exists", configs.getIndexName());
        }
        
        return new MappingHandler(client.admin().indices().prepareGetMappings(configs.getIndexName())
                .setTypes(types).execute().actionGet());
    }
    
    private void setMappings(CreateIndexRequestBuilder req, String[] jsons, String[] types){
        for(int i=0; i< types.length; i++){
            LOGGER.debug(types[i] + " mapping:\n" + jsons[i]);
        }
        int size = jsons.length;
        if(size > types.length) size = types.length;
        for(int i=0; i<size; i++){
            req.addMapping(types[i], jsons[i]);
        }
    }
    
    private void setMappings(String[] types, String[] jsons){
        int size = jsons.length;
        if(types.length < jsons.length)
            size = types.length;
        
        for(int i=0; i<size; i++){
            LOGGER.info("setting {} mapping", types[i]);
            PutMappingResponse mappingMentionResponse = client.admin().indices().preparePutMapping(configs.getIndexName())
                    .setType(types[i]).setSource(jsons[i]).execute().actionGet();
            if(!mappingMentionResponse.isAcknowledged())
                throw new RuntimeException("can not set the mapping for the type " + types[i]);
        }
        
    }
    
    private String readMappingFromFile(String fileName) throws IOException{
        String json;
        URL url = DataTransactionElastic.class.getClassLoader().getResource(fileName);
        if(url != null){
            try {
                json = Resources.toString(url, Charsets.UTF_8);
            }catch (IOException ex){
                throw new IllegalArgumentException("can not find file: " + fileName + " in the ClassPath", ex);
            }
        }else{
            try {
                json = Files.toString(new File(fileName), Charsets.UTF_8);
            } catch (IOException ex) {
                throw new IllegalArgumentException("can not find the specified file: " + fileName, ex);
            }
        }
        
        return json;
    }
    
    private HashSet<URI> extractLeafProperties(Map<String, Object> map, HashSet<URI> set){
        Set<String> propStrings = map.keySet();
        for(String propStr : propStrings){
            try{
                URI propUri = (URI)Data.parseValue(propStr, null);
                URI propNamespace = new URIImpl(propUri.getNamespace());
                set.add(propNamespace); //add the property name to the set.
            }catch(ParseException ex){
                //do nothing.
            }
            Object value = map.get(propStr);
            if(value instanceof Map){ //if it's not a leaf
                set = extractLeafProperties((Map)value, set);
            }
        }
        return set;
    }
    
    private HashSet<URI> readNamespaceSetFromFile(String fileName, HashSet<URI> set){
        List<String> propStrings;
        URL url = DataTransactionElastic.class.getClassLoader().getResource(fileName);
        if(url != null){
            try {
                propStrings = Resources.readLines(url, Charsets.UTF_8);
            }catch (IOException ex){
                throw new IllegalArgumentException("can not find file: " + fileName + " in the ClassPath", ex);
            }
        }else{
            try {
                propStrings = Files.readLines(new File(fileName), Charsets.UTF_8);
            } catch (IOException ex) {
                throw new IllegalArgumentException("can not find the specified file: " + fileName, ex);
            }
        }
        //add to the set.
        for(String propStr : propStrings){
            URI propUri = (URI)Data.parseValue(propStr, null);
            set.add(propUri);
        }
        return set;
    }
    
    private Map<String, Object> compressMapping(Map<String, Object> map) throws IOException{
        HashMap<String, Object> destMap = new HashMap<>();
        return compressMapping(destMap, map);
    }
    private Map<String, Object> compressMapping(Map<String, Object> destMap, Map<String, Object> sourceMap) throws IOException{
        for(String key : sourceMap.keySet()){
            Object value = sourceMap.get(key);
            if(value instanceof Map){
                value = compressMapping((Map)value);
            }
            try{ //if it's a URI
                URI uriKey = (URI)Data.parseValue(key, null);
                key = uriHandler.encode(uriKey);
                destMap.put(key, value);
            }catch(ParseException ex){
                //else put the couple in as it is.
                destMap.put(key, value);
            }
        }
        return destMap;
    }
}

