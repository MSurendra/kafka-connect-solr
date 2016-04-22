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

package com.apache.kafka.connect.solr.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apache.kafka.connect.solr.converter.Converter;

public class SolrConnectorConfig extends AbstractConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(SolrConnectorConfig.class);
	
	
	
	public static final String SOLR_CLUSTER_URL = "solr.cluster.url";
	private static final String SOLR_CLUSTER_URL_DESC = "Solr cluster url : http://localhost:8983/solr/<collection_name>/update/json?commit=true";
	
	public static final String SOLR_CLUSTER_NAME = "solr.cluster.name";
	private static final String SOLR_CLUSTER_NAME_DESC = "Solr cluster name";
	
	public static final String BULK_SIZE = "bulk.size";
	private static final String BULK_SIZE_DOC = "The number of messages to be bulk indexed into Solr. Default is: 500";

	public static final String ACTION_TYPE = "action.type";
	private static final String ACTION_TYPE_DOC = "The action type  messages should be processed. Default is: insert. The following options are available: "
	            + "insert : Creates documents in Solr with the value field set to the received message. "
	            + "delete : Deletes documents from Solr based on id field set in the received message. ";
	
	public static final String SOLR_CONVERTER_CLASS = "solr.doc.converter";
    private static final String SOLR_CONVERTER_CLASS_DOC = "Converter class for Kafka record to Solr document.";
    
	static ConfigDef configDef = new ConfigDef().define(SOLR_CLUSTER_URL, Type.STRING, "http://localhost:8983/solr/kafka-solr/update/json?commit=true", Importance.HIGH, SOLR_CLUSTER_URL_DESC)
            .define(SOLR_CLUSTER_NAME, Type.STRING, "solr-connector", Importance.HIGH, SOLR_CLUSTER_NAME_DESC)
            .define(BULK_SIZE, Type.INT, 500, Importance.HIGH, BULK_SIZE_DOC)
            .define(ACTION_TYPE, Type.STRING, "index", Importance.HIGH, ACTION_TYPE_DOC)
            .define(SOLR_CONVERTER_CLASS, Type.CLASS,  Importance.HIGH, SOLR_CONVERTER_CLASS_DOC);;
	
	public SolrConnectorConfig( Map<String, String> properties) {
		super(configDef, properties);
		
	}
	
	 public String getClusterUrl() {
	        return getString(SOLR_CLUSTER_URL);
	    }
	 
	 public String getClusterName() {
	        return getString(SOLR_CLUSTER_NAME);
	    }
	 
	 public String getActionType() {
	        return getString(ACTION_TYPE);
	  }

	 public Integer getBulkSize() {
	        return getInt(BULK_SIZE);
	   }
	 
	 public Converter getConverter() {
		 
		 Converter converter = null;
	        try {
	            Class<? extends Converter> klass = SolrConnectorConfig.this.getClass(SOLR_CONVERTER_CLASS).asSubclass(Converter.class);
	            if (klass != null) {
	                converter = klass.newInstance();
	            }
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	        return converter;
	 }

}
