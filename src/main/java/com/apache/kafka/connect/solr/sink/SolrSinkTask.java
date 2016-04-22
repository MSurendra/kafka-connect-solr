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
package com.apache.kafka.connect.solr.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apache.kafka.connect.solr.config.SolrConnectorConfig;
import com.apache.kafka.connect.solr.converter.Converter;
import com.apache.kafka.connect.solr.util.SolrServerClient;


public class SolrSinkTask extends SinkTask{
	
	private static final Logger logger = LoggerFactory.getLogger(SolrSinkTask.class);

	private SolrServerClient solrClient;
	
	private Converter converter;

	public String version() {
		
		return AppInfoParser.getVersion();
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		//No operation.
		
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		
		for (SinkRecord record : records) {
			byte[] data = converter.serialize(record);
			if(data !=null)
				solrClient.write(data);
		}
			
		
	}

	@Override
	public void start(Map<String, String> props) {
		
		final SolrConnectorConfig config = new SolrConnectorConfig(props);
		this.solrClient = new SolrServerClient(config);
		this.converter = config.getConverter();
		
	}

	@Override
	public void stop() {
		
		//No operation.
	}
	
	

}
