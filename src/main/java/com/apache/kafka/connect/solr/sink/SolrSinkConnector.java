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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apache.kafka.connect.solr.config.SolrConnectorConfig;

public class SolrSinkConnector extends SinkConnector {

	private static final Logger logger = LoggerFactory.getLogger(SolrSinkConnector.class);
	
	private Map<String, String> props;
	private SolrConnectorConfig config;
	
	@Override
	public void start(Map<String, String> props) {
		this.props = props;
		try {
			config = new SolrConnectorConfig(props);
		} catch (ConfigException e) {
			logger.error("Exception occured in starting of connector.");
			throw new ConnectException("Exception in Start ::: " + getClass().getName() + " due to configuration error.", e);
			
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return SolrSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		
		List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>();
		Map<String, String> taskProps = new HashMap<String, String>();
		taskProps.putAll(props);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}
	
	@Override
	public void stop() {
		//No operation.
	}


}
