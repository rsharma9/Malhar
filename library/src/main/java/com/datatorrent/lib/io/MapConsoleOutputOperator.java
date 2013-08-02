package com.datatorrent.lib.io;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

/**
 * 
 * Writes tuples to standard out of the container
 * <p>
 * This is for specific use case for map where I want to print each key value
 * pair in different line <br>
 * Mainly to be used for debugging. Users should be careful to not have this
 * node listen to a high throughput stream<br>
 * <br>
 * 
 * @since 0.3.4
 */
public class MapConsoleOutputOperator<K, V> extends BaseOperator {
	private boolean debug = false;

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	private static final Logger logger = LoggerFactory
			.getLogger(MapConsoleOutputOperator.class);
	public final transient DefaultInputPort<Map<K,V>> input = new DefaultInputPort<Map<K,V>>() {
		@Override
		public void process(Map<K,V> t) {
			
			Set<Map.Entry<K, V>>  set = t.entrySet();			
			Iterator<Map.Entry<K, V>> itr = set.iterator();

			while (itr.hasNext()) {
				Map.Entry<K,V> entry = (Map.Entry<K,V>) itr.next();
				if (!silent) {
					System.out.println(entry.getKey().toString() + "="
							+ entry.getValue().toString());
				}
				if (debug)
					logger.info(entry.getKey().toString() + "="
							+ entry.getValue().toString());

			}
		}
	};

	boolean silent = false;

	private String stringFormat;

	public String getStringFormat() {
		return stringFormat;
	}

	public void setStringFormat(String stringFormat) {
		this.stringFormat = stringFormat;
	}

}
