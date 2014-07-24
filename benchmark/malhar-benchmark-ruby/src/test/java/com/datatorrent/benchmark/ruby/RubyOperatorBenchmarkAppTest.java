package com.datatorrent.benchmark.ruby;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class RubyOperatorBenchmarkAppTest {

	@Test
	public void testApplication() throws Exception
	{
		LocalMode lma = LocalMode.newInstance();
		new RubyOperatorBenchmarkApplication().populateDAG(lma.getDAG(), new Configuration(false));
		LocalMode.Controller lc = lma.getController();
		lc.run(10000);
	}
}
