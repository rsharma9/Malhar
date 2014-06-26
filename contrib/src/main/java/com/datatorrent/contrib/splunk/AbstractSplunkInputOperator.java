package com.datatorrent.contrib.splunk;

import java.io.InputStream;
import javax.validation.constraints.NotNull;
import com.splunk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

/**
 * Base Splunk input adapter operator, which reads data from Splunk through its own API
 * and writes into output port(s).
 *
 * <p>
 * This is an abstract class. Sub-classes need to implement {@link #queryToRetrieveData()} and {@link #getTuple(Row)}.
 * </p>
 */

public abstract class AbstractSplunkInputOperator<T> extends AbstractStoreInputOperator<T, SplunkStore> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractSplunkInputOperator.class);
	@NotNull
	protected String earliestTime;
	protected String latestTime;


	public void setEarliestTime(@NotNull String earliestTime) {
		this.earliestTime = earliestTime;
	}

	public void setLatestTime(@NotNull String latestTime) {
		this.latestTime = latestTime;
	}

	/**
	 * Any concrete class has to override this method to convert a Value of an Event into Tuple.
	 *
	 * @param value a single value that has been read from a splunk event.
	 * @return Tuple a tuples created from row which can be any Java object.
	 */
	public abstract T getTuple(String value);

	/**
	 * Any concrete class has to override this method to return the query string which will be used to
	 * retrieve data from splunk.
	 *
	 * @return Query string
	 */
	public abstract String queryToRetrieveData();

	/**
	 * The output port that will emit tuple into DAG.
	 */
	@OutputPortFieldAnnotation(name = "outputPort")
	public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

	/**
	 * This executes the search query to retrieve result from splunk.
	 * It then converts each event's value into tuple and emit that into output port.
	 */
	@Override
	public void emitTuples()
	{
		String query = queryToRetrieveData();
		logger.debug(String.format("select statement: %s", query));

		JobExportArgs exportArgs = new JobExportArgs();
		exportArgs.setEarliestTime(earliestTime);
		exportArgs.setLatestTime(latestTime);

		exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL); 
		try {
			InputStream exportSearch = store.getService().export(queryToRetrieveData(), exportArgs);
			MultiResultsReaderXml multiResultsReader;
			multiResultsReader = new MultiResultsReaderXml(exportSearch);

			for (SearchResults searchResults : multiResultsReader)
			{
				for (Event event : searchResults) {
					for (String key: event.keySet()){
						if(key.contains("raw")){
							T tuple = getTuple(event.get(key));
							outputPort.emit(tuple);
						}
					}
				}
			}
			multiResultsReader.close();
		} catch (Exception e) {
			store.disconnect();
			throw new RuntimeException(String.format("Error while running query: %s", query), e);
		}
	}
}
