package com.slemma.jdbc;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Igor Shestakov.
 */
public class MongoStatement extends AbstractMongoStatement implements java.sql.Statement
{
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(MongoConnection.class.getName());

	/**
	 * Constructor for BQStatement object just initializes local variables
	 *
	 * @param mongoConnection
	 */
	public MongoStatement(MongoConnection mongoConnection) {
		this.connection = mongoConnection;
		//this.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
		this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
		this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
	}

	/**
	 * Constructor for BQStatement object just initializes local variables
	 *
	 * @param mongoConnection
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @throws MongoSQLException
	 */
	public MongoStatement(MongoConnection mongoConnection,
							 int resultSetType, int resultSetConcurrency) throws MongoSQLException {
		if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
			throw new MongoSQLException(
					  "The Resultset Concurrency can't be ResultSet.CONCUR_UPDATABLE");
		}

		this.connection = mongoConnection;
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;

	}

	/** {@inheritDoc} */

	@Override
	public ResultSet executeQuery(String query) throws SQLException
	{
		if (this.isClosed()) {
			throw new MongoSQLException("This Statement is Closed");
		}

		this.starttime = System.currentTimeMillis();

//		Job referencedJob;
//		// ANTLR Parsing
//		BQQueryParser parser = new BQQueryParser(querySql, this.connection);
//		querySql = parser.parse();
//		try {
//			// Gets the Job reference of the completed job with give Query
//			referencedJob = BQSupportFuncts.startQuery(
//					  this.connection.getBigquery(),
//					  this.ProjectId.replace("__", ":").replace("_", "."), querySql);
//			this.logger.debug("Executing Query: " + querySql);
//		}
//		catch (IOException e) {
//			throw new MongoSQLException("Something went wrong with the query:\n" + querySql,e);
//		}
//		try {
//			do {
//				if (BQSupportFuncts.getQueryState(referencedJob,
//						  this.connection.getBigquery(),
//						  this.ProjectId.replace("__", ":").replace("_", ".")).equals(
//						  "DONE")) {
//					if(resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
//						return new BQScrollableResultSet(BQSupportFuncts.getQueryResults(
//								  this.connection.getBigquery(),
//								  this.ProjectId.replace("__", ":").replace("_", "."),
//								  referencedJob), this);
//					} else {
//						return new BQForwardOnlyResultSet(
//								  this.connection.getBigquery(),
//								  this.ProjectId.replace("__", ":").replace("_", "."),
//								  referencedJob, this);
//					}
//				}
//				// Pause execution for half second before polling job status
//				// again, to
//				// reduce unnecessary calls to the BigQUery API and lower
//				// overall
//				// application bandwidth.
//				Thread.sleep(500);
//				this.logger.debug("slept for 500" + "ms, querytimeout is: "
//						  + this.querytimeout + "s");
//			}
//			while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
//			// it runs for a minimum of 1 time
//		}
//		catch (IOException e) {
//			throw new MongoSQLException("Something went wrong with the query:\n" + querySql,e);
//		}
//		catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		// here we should kill/stop the running job, but bigquery doesn't
//		// support that :(
//		throw new MongoSQLException(
//				  "Query run took more than the specified timeout");
//
		MongoDatabase database = this.connection.getMongoClient().getDatabase(this.connection.getDatabase());
		Document command = Document.parse(query);
		MongoResult mongoResult =  new MongoResult(database.runCommand(command), database);


		if(resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
			return new MongoScrollableResultSet(mongoResult, this);
		} else {
			return new MongoForwardOnlyResultSet(mongoResult, this);
		}
	}

	//------------------------- for Jdk1.7 -----------------------------------


	@Override
	public void closeOnCompletion() throws SQLException {

	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return false;
	}
}
