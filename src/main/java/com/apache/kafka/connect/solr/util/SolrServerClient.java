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

package com.apache.kafka.connect.solr.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apache.kafka.connect.solr.config.SolrConnectorConfig;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrServerClient {
	
	private static final Logger logger = LoggerFactory.getLogger(SolrServerClient.class);
	
	private final SolrConnectorConfig _config;
	private LinkedBlockingQueue<HashMap<String,Object>> _batchQueue = null;
	private TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
	private  ObjectMapper mapper;
	public SolrServerClient (final SolrConnectorConfig _config) {
		this._config = _config;
		this._batchQueue = new LinkedBlockingQueue<HashMap<String,Object>>(_config.getBulkSize());
		mapper = new ObjectMapper();
	}
	
	public void write(byte[] data) {
		
		try {
			this._batchQueue.add((HashMap<String,Object>)mapper.readValue(data, typeRef));
		} catch (JsonParseException e) {
			logger.error("Json processing EXception" + e);
		} catch (JsonMappingException e) {
			logger.error("Json processing EXception" + e);
		} catch (IOException e) {
			
			logger.error("Json IO EXception" + e);
		}
		 if(this._batchQueue.remainingCapacity() == 0) {
			 ArrayList<HashMap<String,Object>>  drainToArray = new ArrayList<HashMap<String,Object>>();
			 this._batchQueue.drainTo(drainToArray);
			 try {
				writeToSolr(mapper.writeValueAsBytes(drainToArray));
			} catch (JsonProcessingException e) {
				logger.error("Json processing EXception" + e);
			}
		 }
	}
	
	private void writeToSolr(byte[] data) {
		
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost post = new HttpPost(this._config.getClusterUrl());
	 
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8");
		
		try {
			EntityBuilder eb = EntityBuilder.create();
			eb.setBinary(data);
			post.setEntity(eb.build());
			HttpResponse response = client.execute(post);
			//if( logger.isInfoEnabled() )
			logger.info( "[Response] :: " +EntityUtils.toString(response.getEntity()));
			
			
		} catch (ClientProtocolException clientException) {
			logger.error(clientException.getMessage());
			
		} catch (IOException ioException) {
			logger.error(ioException.getMessage());
		} finally {
		}
	}
	
}
