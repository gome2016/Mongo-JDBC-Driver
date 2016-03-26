package com.slemma.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

/**
 * @author Igor Shestakov.
 */
public class MongoSQLException extends SQLException
{
	Logger logger = LoggerFactory.getLogger(MongoDriver.class.getName());

	public MongoSQLException() {
		super();
		StringWriter sw = new StringWriter();
		super.printStackTrace(new PrintWriter(sw));
		String stacktrace = sw.toString();
		this.logger.debug("SQL exception " + stacktrace);
	}

	public MongoSQLException(String reason) {
		super(reason);
		StringWriter sw = new StringWriter();
		super.printStackTrace(new PrintWriter(sw));
		String stacktrace = sw.toString();
		this.logger.debug(reason + stacktrace);
	}

	public MongoSQLException(String reason, String sqlState) {
		super(reason, sqlState);
		StringWriter sw = new StringWriter();
		super.printStackTrace(new PrintWriter(sw));
		String stacktrace = sw.toString();
		this.logger.debug("SQL exception " + reason + " ;; " + sqlState + " ;; "
				  + stacktrace);
	}

	public MongoSQLException(String reason, String sqlState, int vendorCode) {
		super(reason, sqlState, vendorCode);
		StringWriter sw = new StringWriter();
		super.printStackTrace(new PrintWriter(sw));
		String stacktrace = sw.toString();
		this.logger.debug("SQL exception " + reason + " " + sqlState + " "
				  + String.valueOf(vendorCode) + stacktrace);
	}
	
	public MongoSQLException(String reason, String sqlState, int vendorCode,
								 Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
		this.logger.debug("SQL exception " + reason + " " + sqlState + " "
				  + String.valueOf(vendorCode), cause);
	}

	public MongoSQLException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
		this.logger.debug("SQL exception " + reason + " " + sqlState, cause);
	}

	public MongoSQLException(String reason, Throwable cause) {
		super(reason, cause);
		this.logger.debug("SQL exception " + reason, cause);
	}

	public MongoSQLException(Throwable cause) {
		super(cause);
		this.logger.debug("SQL exception ", cause);
	}
	
}
