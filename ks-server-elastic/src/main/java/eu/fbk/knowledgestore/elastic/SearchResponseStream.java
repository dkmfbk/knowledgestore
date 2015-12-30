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

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.openrdf.model.URI;

/**
 *
 * @author enrico
 */
public class SearchResponseStream extends Stream<Record>{
    SearchResponse response;
    Client client;
    TimeValue timeout;
    MappingHandler mapper;
    URIHandler uriHandler;
    URI[] properties;
    
    /**
     * 
     * @param res the SearchResponse has to be created from a SearchRequest with a setScroll.
     * @param client client where to execute
     * @param timeout duration of the scrollId
     */
    SearchResponseStream(SearchResponse res, Client client, TimeValue timeout,  MappingHandler mapper, URIHandler handler){
        response = res;
        this.client = client;
        this.timeout = timeout;
        this.mapper = mapper;
        this.uriHandler = handler;
    }
    
    @Override
    protected void doToHandler(final Handler<? super Record> handler) throws Throwable {
        while(true){            
            for(SearchHit hit : response.getHits().getHits()){
                Record rec;
                rec = Utility.deserialize(hit, mapper, uriHandler);
                if(Thread.interrupted()){
                    handler.handle(null);
                    return;
                }
                handler.handle(rec);
            }
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(timeout).execute().actionGet();
            //Break condition: No hits are returned
            if (response.getHits().getHits().length == 0){
                handler.handle(null);
                break;
            }
        }
    }
}
