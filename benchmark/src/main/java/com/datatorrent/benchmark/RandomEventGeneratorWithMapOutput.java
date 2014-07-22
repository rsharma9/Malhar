package com.datatorrent.benchmark;

import java.util.HashMap;
import java.util.Random;
import javax.validation.constraints.Min;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

public class RandomEventGeneratorWithMapOutput extends BaseOperator implements InputOperator {

	public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<Integer> integer_data = new DefaultOutputPort<Integer>();
	public final transient DefaultOutputPort<HashMap<String, Object>> map_data = new DefaultOutputPort<HashMap<String, Object>>();
	private int maxCountOfWindows = Integer.MAX_VALUE;
	@Min(1)
	private int tuplesBlast = 1000;
	@Min(1)
	private int tuplesBlastIntervalMillis = 10;
	private int min_value = 0;
	private int max_value = 100;
	private String key;
	private final Random random = new Random();

	public int getMaxvalue()
	{
		return max_value;
	}

	public int getMinvalue()
	{
		return min_value;
	}

	@Min(1)
	public int getTuplesBlast()
	{
		return tuplesBlast;
	}

	@Min(1)
	public int getTuplesBlastIntervalMillis()
	{
		return this.tuplesBlastIntervalMillis;
	}

	public void setMaxvalue(int i)
	{
		max_value = i;
	}

	public void setMinvalue(int i)
	{
		min_value = i;
	}

	public void setTuplesBlast(int i)
	{
		tuplesBlast = i;
	}

	public void setTuplesBlastIntervalMillis(int tuplesBlastIntervalMillis) {
		this.tuplesBlastIntervalMillis = tuplesBlastIntervalMillis;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public void setup(OperatorContext context)
	{
		if (max_value <= min_value) {
			throw new IllegalArgumentException(String.format("min_value (%d) should be < max_value(%d)", min_value, max_value));
		}
	}

	public void setMaxcountofwindows(int i)
	{
		maxCountOfWindows = i;
	}

	@Override
	public void endWindow()
	{
		if (--maxCountOfWindows == 0) {
			//Thread.currentThread().interrupt();
			throw new RuntimeException(new InterruptedException("Finished generating data."));
		}
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void emitTuples()
	{
		int range = max_value - min_value + 1;
		int i = 0;
		while (i < tuplesBlast) {
			int rval = min_value + random.nextInt(range);
			if (integer_data.isConnected()) {
				integer_data.emit(rval);
			}
			if (string_data.isConnected()) {
				string_data.emit(Integer.toString(rval));
			}
			if (map_data.isConnected()) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put(key, rval);
				map_data.emit(map);
			}
			i++;
		}

		if (tuplesBlastIntervalMillis > 0) {
			try {
				Thread.sleep(tuplesBlastIntervalMillis);
			} catch (InterruptedException e) {
			}
		}
	}
}
