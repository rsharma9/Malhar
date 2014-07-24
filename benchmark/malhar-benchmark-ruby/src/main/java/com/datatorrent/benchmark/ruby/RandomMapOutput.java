package com.datatorrent.benchmark.ruby;

import java.util.HashMap;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class RandomMapOutput extends BaseOperator {

	public final transient DefaultOutputPort<HashMap<String, Object>> map_data = new DefaultOutputPort<HashMap<String, Object>>();
	public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
	{
		@Override
		public void process(Integer tuple)
		{
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put(key, tuple);
			RandomMapOutput.this.process(map);
		}
	};
	
	private String key;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void process(HashMap<String, Object> tuple) {
		
		if (map_data.isConnected()) {
			map_data.emit(tuple);
		}
	}
}
