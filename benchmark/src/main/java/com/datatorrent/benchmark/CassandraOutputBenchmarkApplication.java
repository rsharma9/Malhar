/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="CassandraOutputBenchmark")
public class CassandraOutputBenchmarkApplication implements StreamingApplication {

	private final Locality locality = null;
	
	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		
		int maxValue = 30000;

	    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
	    rand.setMinvalue(0);
	    rand.setMaxvalue(maxValue);
	    
	    CassandraOutputOperator cassandra = dag.addOperator("cassandra", new CassandraOutputOperator());
	    CassandraTransactionalStore store = new CassandraTransactionalStore();
	    store.setKeyspace("test");
	    store.setNode("127.0.0.1");
	    cassandra.setStore(store);
	    
	    dag.addStream("rand_cass", rand.integer_data, cassandra.input).setLocality(locality);
	}

}
