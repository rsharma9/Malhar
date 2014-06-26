package com.datatorrent.contrib.splunk;

import org.junit.Assert;
import org.junit.Test;
import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class SplunkInputOperatorTest
{
	public static final String HOST = "127.0.0.1";
	public static final int PORT = 8089;

	private static final String USER_NAME = "admin";
	private static final String PASSWORD = "rohit";
	private static String APP_ID = "SplunkTest";
	private static int OPERATOR_ID = 0;


	private static class TestInputOperator extends AbstractSplunkInputOperator<String>
	{
		private static final String retrieveQuery = "search * | head 100";

		@Override
		public String getTuple(String value)
		{
			return value;
		}

		@Override
		public String queryToRetrieveData()
		{
			return retrieveQuery;
		}

	}
	
	@Test
	public void TestCassandraInputOperator()
	{
		SplunkStore store = new SplunkStore();
		store.setHost(HOST);
		store.setPassword(PASSWORD);
		store.setPort(PORT);
		store.setUserName(USER_NAME);

		AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
		attributeMap.put(DAG.APPLICATION_ID, APP_ID);
		OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

		TestInputOperator inputOperator = new TestInputOperator();
		inputOperator.setStore(store);
		inputOperator.setEarliestTime("-1000h");
		inputOperator.setLatestTime("now");
		CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
		inputOperator.outputPort.setSink(sink);

		inputOperator.setup(context);
		inputOperator.beginWindow(0);
		inputOperator.emitTuples();
		inputOperator.endWindow();

		Assert.assertEquals("rows from splunk", 100, sink.collectedTuples.size());
	}

}
