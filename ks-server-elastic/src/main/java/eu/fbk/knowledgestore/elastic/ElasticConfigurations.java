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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author enrico
 */
public class ElasticConfigurations {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticConfigurations.class);
    //path where to find the file with all the configurations.
    
//settings handled by ES directly.
    private final Settings nodeSettings;
    //path where to find 2 files: the mapping of the resource type and the one for the mention type
    private final String resourceMapping;
    private final String mentionMapping;

    private final String indexName; //name of the index.
    
 //only for TransportClient.
    private final TransportAddress[] addresses;  //addresses for TrasportClient.
//for scroll
    private final TimeValue timeout; //timeout between the request of a scroll page and the next one.
    
//for bulk
    private final TimeValue bulkTime; //for bulk timeout
    private final ByteSizeValue bulkSize; //threshold size for flush the bulk
    private final TimeValue flushInterval; //time interval for flush the bulk
    private final int concurrentRequests; //number of concurrent request that the bulk can execute
    
    //configuration for URIHandler.
    private final String weakCompressionPath;
    private final String strongCompressionPath;
    
    ElasticConfigurations(String path){
        //load configurations from the file.
        if(path != null){
            URL url = DataTransactionElastic.class.getClassLoader().getResource(path);
            if(url != null){
                try{
                    LOGGER.debug("loading configuration from classPath");
                    nodeSettings = ImmutableSettings.settingsBuilder().loadFromClasspath(path).build();
                }catch(SettingsException ex){
                    throw new IllegalArgumentException("failed to load settings from classpath", ex);
                }
            }else{
                try{
                    LOGGER.debug("loading configuration from source");
                    InputStream input = null;
                    try {
                        input = new FileInputStream(path);
                    } catch (FileNotFoundException ex) {
                        throw new IllegalArgumentException("file" + path + "not found", ex);
                    }
                    nodeSettings = ImmutableSettings.settingsBuilder().loadFromStream(path, input).build();
                }catch(SettingsException ex){
                    throw new IllegalArgumentException("failed to load settings from source", ex);
                }
            }
            LOGGER.debug("nodeSettings: " + nodeSettings.names());
            mentionMapping = nodeSettings.get("index.mapping.mention");
            resourceMapping = nodeSettings.get("index.mapping.resource");
            LOGGER.debug("mention mapping url: " + mentionMapping + " ; resource mapping url: " + resourceMapping);
            indexName = nodeSettings.get("index.name");
            if(indexName == null){
                throw new IllegalArgumentException("no index name found in the configuration file: " + path);
            }
            LOGGER.debug("index name: " + indexName);
            bulkTime = new TimeValue(nodeSettings.getAsLong("bulk.timeout", 60000L), TimeUnit.MILLISECONDS);
            bulkSize = new ByteSizeValue(getNodeSettings().getAsLong("bulk.size", 1024L));
            concurrentRequests = nodeSettings.getAsInt("bulk.concurrent_request", 0);
            flushInterval = new TimeValue(getNodeSettings().getAsLong("bulk.interval", 60000L), TimeUnit.MILLISECONDS);
            
            timeout = new TimeValue(getNodeSettings().getAsLong("scroll.timeout", 600000L), TimeUnit.MILLISECONDS);
            
            addresses = parseAddresses(nodeSettings.getAsArray("transport.client.initial_nodes"));
            
            weakCompressionPath = nodeSettings.get("uri_handler.weakcompression_path");
            strongCompressionPath = nodeSettings.get("uri_handler.strongcompression_path");
            
        }else{//if there are no settings don't start.
            throw new IllegalArgumentException("can not load the settings from path: " + path);
            /*
            LOGGER.warn("loading default settings");
            nodeSettings = null;
            indexName = "eu.fkb.dkm.elasticindex";
            bulkTime = new TimeValue(1, TimeUnit.MINUTES);
            timeout = new TimeValue(1, TimeUnit.MINUTES);
            LOGGER.info("loaded the default settings");
            mentionMapping = null;
            resourceMapping = null;
            addresses = null;
            bulkSize = null;
            flushInterval = null;
            concurrentRequests = -1;
            weakCompressionPath = null;
            strongCompressionPath = null;
            */
        }
        
        LOGGER.debug("elasticsearch configuration loaded");
    }
    
    private TransportAddress[] parseAddresses(String[] addresses){
        LOGGER.debug("addresses string: " + Arrays.toString(addresses));
        
        if(addresses == null || addresses.length == 0) return null;
        
        TransportAddress[] res = new InetSocketTransportAddress[addresses.length];
        for(int i=0; i< addresses.length; i++){
            String[] splitted = addresses[i].split(":", 2); //192.168.0.1:1024
            InetAddress address = null;
            int port = -1;
            try {
                address = InetAddress.getByName(splitted[0]);
                port = Integer.parseInt(splitted[1]);
                LOGGER.debug("adress: " + address + " : " + port);
                res[i] = new InetSocketTransportAddress(address, port);
            } catch (UnknownHostException | NumberFormatException ex) {
                LOGGER.error("can not find the host with IP: " + splitted[0] + " and port: " + port);
            }
        }
        return res;
    }
    

    /**
     * @return the nodeSettings
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * @return the resourceMapping
     */
    public String getResourceMapping() {
        return resourceMapping;
    }

    /**
     * @return the mentionMapping
     */
    public String getMentionMapping() {
        return mentionMapping;
    }

    /**
     * @return the indexName
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * @return the addresses
     */
    public TransportAddress[] getAddresses() {
        return addresses;
    }

    /**
     * @return the timeout
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * @return the bulkTime
     */
    public TimeValue getBulkTime() {
        return bulkTime;
    }

    
    /**
     * @return the bulkSize
     */
    public ByteSizeValue getBulkSize() {
        return bulkSize;
    }

    /**
     * @return the flushInterval
     */
    public TimeValue getFlushInterval() {
        return flushInterval;
    }

    /**
     * @return the concurrentRequests
     */
    public int getConcurrentRequests() {
        return concurrentRequests;
    }
    
    /**
     * @return the weakCompressionPath
     */
    public String getWeakCompressionPath(){
        return weakCompressionPath;
    }
    
    /**
     * @return the strongCompressionPath
     */
    public String getStrongCompressionPath(){
        return strongCompressionPath;
    }
}