package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.lib.db.AbstractTransactionableStoreOutputOperator;

public abstract class AbstractCassandraAtLeastOnceOutputOperator<T> extends AbstractTransactionableStoreOutputOperator<T, CassandraTransactionalStore> {
	
	public AbstractCassandraAtLeastOnceOutputOperator() {
		super();
	}

	/**
	 * Sets the parameter of the insert/update statement with values from the tuple.
	 *
	 * @param tuple     tuple
   * @return statement The statement to excecute
	 * @throws DriverException
	 */
	protected abstract Statement getUpdateStatement(T tuple) throws DriverException;

	@Override
	public void processTuple(T tuple) {
		
		store.getSession().execute(getUpdateStatement(tuple));
	}
}
