/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apache.kafka.connect.solr.converter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class SolrJsonDocConverter implements Converter {
	
	private static final Logger log = LoggerFactory.getLogger(SolrJsonDocConverter.class);

    private final JsonConverter valueConverter;
    private final JsonConverter keyConverter;

    private final ObjectMapper mapper;
    private final ObjectReader reader;
    
    
    public SolrJsonDocConverter() {
    	
    	valueConverter = new JsonConverter();
        keyConverter = new JsonConverter();
        
        mapper = new ObjectMapper();
        reader = mapper.reader();
        
        Map<String, String> props = new HashMap<String, String>();
        props.put("schemas.enable", Boolean.FALSE.toString());
        valueConverter.configure(props, false);
        keyConverter.configure(props, true);
        
       
    }

	public byte[] serialize(SinkRecord record) {
		
		Map<String, Object> jsonRecord = new HashMap<String, Object>();
		byte[] valueStream = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
	    byte[] keyStream = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
	    byte[] jsonByte = null;
	    JsonNode valueJson = null;  
	    JsonNode keyJson = null;
	    Map keyJsonMap = (keyJson != null) ? mapper.convertValue(keyJson, Map.class) : null;
        Map valueJsonMap = (valueJson != null) ? mapper.convertValue(valueJson, Map.class) : null;
	    
	    if (keyJson != null) {
            try {
            	keyJson = reader.readTree(new ByteArrayInputStream(keyStream));
            } catch (IOException e) {
                    log.error("Error when converting key to Json Object", e);
            }
        }

        
        if (valueStream != null) {
            try {
            	valueJson = reader.readTree(new ByteArrayInputStream(valueStream));
            } catch (IOException e) {
                
                    log.error("Error when converting key to Json Object", e);
            }
        }
	        
        if (keyJsonMap != null) {
        	jsonRecord.putAll(keyJsonMap);
        }
        if (valueJsonMap != null) {
        	jsonRecord.putAll(valueJsonMap);
        }
	    
        try {
        	jsonByte = mapper.writeValueAsBytes(jsonRecord);
        } catch (JsonProcessingException e) {
            if (log.isErrorEnabled()) {
                log.error("Error writing json map as bytes", e);
            }
        }
	        
	     return jsonByte;
	    }
	
	}


