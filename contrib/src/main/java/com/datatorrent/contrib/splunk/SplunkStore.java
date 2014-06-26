package com.datatorrent.contrib.splunk;

import com.splunk.*; 

import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.lib.db.Connectable;

/**
 * A {@link Connectable} that uses splunk to connect to stores.
 *
 */
public class SplunkStore implements Connectable{

	protected static final Logger logger = LoggerFactory.getLogger(SplunkStore.class);
	private String userName;
	private String password;
	@NotNull
	protected String host;
	@NotNull
	protected int port;

	protected transient Service service = null;

	/**
	 * Sets the user name.
	 *
	 * @param userName user name.
	 */
	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	/**
	 * Sets the password.
	 *
	 * @param password password
	 */
	public void setPassword(String password)
	{
		this.password = password;
	}

	/**
	 * Sets the host.
	 *
	 * @param host host
	 */
	public void setHost(@NotNull String host) {
		this.host = host;
	}

	/**
	 * Sets the port.
	 *
	 * @param port port
	 */
	public void setPort(@NotNull int port) {
		this.port = port;
	}
	
	public Service getService() {
		return service;
	}

	/**
	 * Create connection with the splunk server.
	 */
	@Override
	public void connect()
	{
		try{
		ServiceArgs loginArgs = new ServiceArgs();
		loginArgs.setUsername(userName);
		loginArgs.setPassword(password);
		loginArgs.setHost(host);
		loginArgs.setPort(port);
		
		service = Service.connect(loginArgs);
		}
		catch(Exception e){
			throw new RuntimeException("closing connection", e);
		}
	}
	
	/**
	 * Close connection.
	 */
	@Override
	public void disconnect()
	{
		try{
			service.logout();
		}
		catch(Exception e){
			throw new RuntimeException("closing connection", e);
		}
		
	}

	@Override
	public boolean connected()
	{
		if(service.getToken() == null)
			return false;
		else
			return true;
	}
}
