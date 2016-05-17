/**
 * MongoDb JDBC Driver
 * Copyright (C) 2016, Slemma.
 * <p/>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * MongoDriver - This class implements the java.sql.Driver interface
 * <p/>
 * The driver URL is:
 * <p/>
 * Url format: jdbc:mongodb:mql://<MONGO URI without prefix>
 * <p/>
 * <p/>
 * Any Java program can use this driver for JDBC purpose by specifying this URL
 * format.
 * </p>
 */

package com.slemma.jdbc;

import com.mongodb.MongoClientURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * This Class implements the java.sql.Driver interface
 *
 * @author Igor Shestakov
 */
public class MongoDriver implements java.sql.Driver
{
	private final static Logger logger = LoggerFactory.getLogger(MongoDriver.class.getName());

	private static Driver registeredDriver;
	/**
	 * Url prefix for using this driver
	 */
	private static final String PREFIX = "jdbc:mongodb:mql:";
	private static final String PROTOCOL = "mongodb";
	private static final String SUB_PROTOCOL = "mql";
	/**
	 * MAJOR version of the driver
	 */
	private static final int MAJOR_VERSION = 1;
	/**
	 * Minor version of the driver
	 */
	private static final int MINOR_VERSION = 0;
	/**
	 * Properties
	 **/
	private Properties props = null;

	/** Registers the driver with the drivermanager */
	static
	{
		try
		{
			register();
			Logger logger = LoggerFactory.getLogger(MongoDriver.class.getName());
			logger.debug("Registered the driver");

		}
		catch (Exception e)
		{
			throw new ExceptionInInitializerError(e);
		}

	}

	public static void register() throws SQLException
	{
		if (isRegistered())
		{
			throw new IllegalStateException(
					  "Driver is already registered. It can only be registered once.");
		}

		MongoDriver registeredDriver = new MongoDriver();
		DriverManager.registerDriver(registeredDriver);
		MongoDriver.registeredDriver = registeredDriver;

	}

	public static boolean isRegistered()
	{
		return registeredDriver != null;
	}

	public static void deregister() throws SQLException
	{
		if (!isRegistered())
		{
			throw new IllegalStateException(
					  "Driver is not registered (or it has not been registered using Driver.register() method)");
		}
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	/**
	 * Gets Major Version of the Driver as static
	 *
	 * @return Major Version of the Driver as static
	 */
	public static int getMajorVersionAsStatic()
	{
		return MongoDriver.MAJOR_VERSION;
	}

	/**
	 * Gets Minor Version of the Driver as static
	 *
	 * @return Minor Version of the Driver as static
	 */
	public static int getMinorVersionAsStatic()
	{
		return MongoDriver.MINOR_VERSION;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		return url != null && url.toLowerCase().startsWith(PREFIX);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * This method create a new MongoConnection and then returns it
	 * </p>
	 */
	@Override
	public Connection connect(String url, Properties info)
			  throws SQLException
	{
		if (!acceptsURL(url)) return null;

		url = url.trim();

//		Properties mergedProps = this.parseURL(url, info);
//		url = url.replace(this.PREFIX, "mongodb:");
//		return new MongoConnection(url, mergedProps);

		url = url.replace(this.PREFIX, "mongodb:");
		return new MongoConnection(url, info);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMajorVersion()
	{
		return MongoDriver.MAJOR_VERSION;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinorVersion()
	{
		return MongoDriver.MINOR_VERSION;
	}

	public static String getName() {
		return "com.slemma.mongo-jdbc  JDBC driver";
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Gets information about the possible properties for this driver.
	 * </p>
	 *
	 * @return a default DriverPropertyInfo
	 */
	@Override
	public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{

		if (props == null)
		{
			props = new Properties();
		}

		DriverPropertyInfo host =
				  new DriverPropertyInfo("host", props.getProperty("host"));
		host.required = true;
		host.description = "Host";

		DriverPropertyInfo port =
				  new DriverPropertyInfo("host", props.getProperty("port"));
		host.required = true;
		host.description = "Port";

		DriverPropertyInfo database =
				  new DriverPropertyInfo("database", props.getProperty("database"));
		host.required = true;
		host.description = "Database";

		DriverPropertyInfo user =
				  new DriverPropertyInfo("user", info.getProperty("user"));
		user.required = false;
		user.description = "Username to authenticate as";

		DriverPropertyInfo password =
				  new DriverPropertyInfo("password", info.getProperty("password"));
		password.required = false;
		password.description = "Password to use for authentication";

		DriverPropertyInfo[] dpi = {
				  host,
				  port,
				  database,
				  user,
				  password,
		};
		return dpi;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns false, since the driver is not jdbcCompliant
	 * </p>
	 */
	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	/**
	 * Constructs a new DriverURL, splitting the specified URL into its
	 * component parts
	 *
	 * @param url      JDBC URL to parse
	 * @param defaults Default properties
	 * @return Properties with elements added from the url
	 * @throws java.sql.SQLException
	 */


	Properties parseURL(String url, Properties defaults) throws java.sql.SQLException
	{
		Properties urlProps = new Properties(defaults);

//		String modifiedUrl = url.replace("jdbc:mongodb:mql://","mongodb://");
//		MongoClientURI mU =  new MongoClientURI(modifiedUrl);
	/*
	  * Parse parameters after the ? in the URL and remove
	 * them from the original URL.
	 */

		int index = url.indexOf("?");

		if (index != -1)
		{
			String ParamString = url.substring(index + 1, url.length());
			url = url.substring(0, index);

			StringTokenizer queryParams = new StringTokenizer(ParamString, "&");

			while (queryParams.hasMoreTokens())
			{

				StringTokenizer vp = new StringTokenizer(queryParams.nextToken(), "=");

				String param = "";

				if (vp.hasMoreTokens())
					param = vp.nextToken();

				String value = "";

				if (vp.hasMoreTokens())
					value = vp.nextToken();

				if (value.length() > 0 && param.length() > 0)
					urlProps.put(param, value);
			}
		}

		StringTokenizer st = new StringTokenizer(url, ":/", true);

		if (st.hasMoreTokens())
		{
			String protocol = st.nextToken();
			if (protocol != null)
			{
				if (!protocol.toLowerCase().equals("jdbc"))
					return null;
			}
			else
				return null;
		}
		else
			return null;

		// Look for the colon following 'jdbc'
		if (st.hasMoreTokens())
		{
			String Colon = st.nextToken();
			if (Colon != null)
			{
				if (!Colon.equals(":"))
					return null;
			}
			else
				return null;
		}
		else
			return null;

		// Look for the colon following 'jdbc'
		if (st.hasMoreTokens())
		{
			String proto = st.nextToken();
			if (proto != null)
			{
				if (!proto.toLowerCase().equals(MongoDriver.PROTOCOL))
					return null;
			}
			else
				return null;
		}
		else
			return null;

		// Look for the colon following 'mongodb'
		if (st.hasMoreTokens())
		{
			String Colon = st.nextToken();
			if (Colon != null)
			{
				if (!Colon.equals(":"))
					return null;
			}
			else
				return null;
		}
		else
			return null;

		// Look for sub-protocol to be mql
		if (st.hasMoreTokens())
		{
			String subProto = st.nextToken();
			if (subProto != null)
			{
				if (!subProto.toLowerCase().equals(MongoDriver.SUB_PROTOCOL))
					return null; // We only handle MongoDriver sub-protocol
			}
			else
				return null;
		}
		else
			return null;

		// Look for the colon following sub protocol
		if (st.hasMoreTokens())
		{
			String colon = st.nextToken();
			if (colon != null)
			{
				if (!colon.equals(":"))
					return null;
			}
			else
				return null;
		}
		else
			return null;

		// Look for the "host" of the URL
		if (st.hasMoreTokens())
		{
			String host = st.nextToken();
			if (host != null)
			{
				if (host.equals("/"))
				{
					if (st.hasMoreTokens())
					{
						host = st.nextToken();
						if (host != null)
						{
							if (host.equals("/"))
							{
								if (st.hasMoreTokens())
								{
									host = st.nextToken();
									if (host == null)
										return null;
								}
								else
									return null;
							}
							else
								return null;
						}
						else
							return null;
					}
					else
						return null;
				}
				urlProps.put("host", host);
			}
			else
				return null;
		}
		else
			return null;

		// Look for the "port" of the URL
		if (st.hasMoreTokens())
		{
			String afterHostToken = st.nextToken();
			if (afterHostToken.equals(":"))
			{
				//extract port
				if (st.hasMoreTokens())
				{
					String portStr = st.nextToken();
					try
					{
						urlProps.put("port", Integer.parseInt(portStr));
					}
					catch (NumberFormatException ex)
					{
						return null;
					}
					if (st.hasMoreTokens()){
						String afterPortToken = st.nextToken();
						if (!afterPortToken.equals("/"))
							return null;
					}
					else
						return null;
				}
				else if (!afterHostToken.equals("/"))
				{
					return null;
				}
			}
		}
		else
			return null;

		// Look for the "database" of the URL
		if (st.hasMoreTokens())
		{
			String database = st.nextToken();
			urlProps.put("database", database);
		}
		else
			return null;

		return urlProps;
	}

	//------------------------- for Jdk1.7 -----------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		return null;
	}
}