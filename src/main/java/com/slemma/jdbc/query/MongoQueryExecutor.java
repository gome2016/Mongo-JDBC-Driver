package com.slemma.jdbc.query;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.AbstractMongoStatement;
import com.slemma.jdbc.MongoForwardOnlyResultSet;
import com.slemma.jdbc.MongoSQLException;
import com.slemma.jdbc.MongoScrollableResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Igor Shestakov.
 */
public class MongoQueryExecutor
{
	private final MongoDatabase database;

	public MongoQueryExecutor(MongoDatabase database)
	{
		this.database = database;
	}

	public java.sql.ResultSet run(MongoQuery query, AbstractMongoStatement statement) throws SQLException
	{
		MongoResult mongoResult =  null;

		try
		{
			if (query instanceof CountMembersMixedQuery)
			{
				mongoResult = new MongoCountMembersResult((CountMembersMixedQuery) query, database.runCommand(query.getMqlCommand()), database);
			} else if (query instanceof GetMembersMixedQuery) {
				mongoResult = new MongoGetMembersResult((GetMembersMixedQuery) query, database.runCommand(query.getMqlCommand()), database);
			} else if (query instanceof MongoQuery) {
				mongoResult = new MongoBasicResult(database.runCommand(query.getMqlCommand()), database);
			} else {
				throw new UnsupportedOperationException("Unhandled query type.");
			}
		}
		catch (MongoCommandException e)
		{
			throw new MongoSQLException(e);
		}

		if(statement.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE) {
			return new MongoScrollableResultSet(mongoResult, statement);
		} else {
			return new MongoForwardOnlyResultSet(mongoResult, statement);
		}

	}
}
