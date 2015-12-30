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

import eu.fbk.knowledgestore.data.Record;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * saves the mapping and allows to query what kind of data is behind a given property name.
 * @author enrico
 */
public class MappingHandler{
    private static final Logger LOGGER = LoggerFactory.getLogger(MappingHandler.class);    
    HashMap<String, Class<?>> mapper;
   
    /**
     * saves the mappings: property name : class of the value.
     * @param mappings object that contains the mappings of all the types in all the indexes.
     * @throws IOException 
     */
    MappingHandler(GetMappingsResponse mappings) throws IOException{
        mapper = new HashMap<>();
        
        for(ObjectCursor<String> keyRoot : mappings.getMappings().keys()){
            ImmutableOpenMap<String, MappingMetaData> openMap = mappings.getMappings().get(keyRoot.value);
            for(ObjectCursor<String> key : openMap.keys()){
                addMappings(openMap.get(key.value).getSourceAsMap());
            }
        }
    }
    /**
     * return the Class of the value for the specified property.
     * @param propName name of the property
     * @return 
     */
     public Class<?> getValueClass(String propName){
         Class<?> res = mapper.get(propName);
         if(res == null){
             LOGGER.debug("undefined property");
             throw new IllegalArgumentException("undefined property: " + propName);
         }
        return res;
    }
    
    private void addMappings(Map<String, Object> map){
        Map typeMapping = (Map)map.get("properties"); //get the properties.
        for(Object key : typeMapping.keySet()){ //for every property
            Map typeMappingValue = (Map)typeMapping.get(key); //set of settings of the property.
            if(typeMappingValue.keySet().contains("type")){ //if contains type it's not a nested record.
                Class<?> propertyValueClass = fromStringToClass(typeMappingValue.get("type"));
//               LOGGER.debug("\tadding: " + key + " -> " + propertyValueClass);
                mapper.put((String)key, propertyValueClass);
            }else{ //if it's a nested record.
                mapper.put((String)key, Record.class);
                addMappings(typeMappingValue);
            }
        }
    }
    
    public Set<String> getKeys(){
        return mapper.keySet();
    }
    
    private Class<?> fromStringToClass(Object str){
        Class<?> res = null;
        String stringClass = ((String)str).toLowerCase();
        switch((String)str){
            case "string":
                res = String.class;
                break;
            case "integer":
                res = Integer.class;
                break;
            case "byte":
                res = Byte.class;
                break;
            case "short":
                res = Short.class;
                break;
            case "long":
                res = Long.class;
                break;
            case "float":
                res = Float.class;
                break;
            case "double":
                res = Double.class;
                break;
            case "date":
                res = Date.class;
                break;
            case "boolean":
                res = Boolean.class;
                break;
            case "binary":
                res = byte[].class;
                break;
            default:
                throw new IllegalArgumentException("unknown class: " + stringClass + " in the mapping");
        }
        return res;
    }
}