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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrAvroJsonDocConverter implements Converter {
	
	private static final Logger log = LoggerFactory.getLogger(SolrJsonDocConverter.class);
	
	private final ObjectMapper mapper;
	
	public SolrAvroJsonDocConverter() {
    	
        mapper = new ObjectMapper();
       
     }

	public byte[] serialize(SinkRecord record) {
		
		Map<String, Object> jsonMap = toJsonMap((Struct) record.value());
		byte[] jsonByte = null;
		try {
        	jsonByte = mapper.writeValueAsBytes(jsonMap);
        } catch (JsonProcessingException e) {
                log.error("Error writing json map as bytes", e);
        }
	        
	     return jsonByte;
	}
	
	private Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case BOOLEAN:
                    jsonMap.put(fieldName, struct.getBoolean(fieldName));
                    break;
                case INT8:
                    jsonMap.put(fieldName, struct.getInt8(fieldName));
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT32:
                    jsonMap.put(fieldName, struct.getInt32(fieldName));
                    break;
                case INT64:
                    jsonMap.put(fieldName, struct.getInt64(fieldName));
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case FLOAT64:
                    jsonMap.put(fieldName, struct.getFloat64(fieldName));
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }

}
