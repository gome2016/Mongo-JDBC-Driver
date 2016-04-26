package com.slemma.jdbc.query;

import com.slemma.jdbc.MongoSQLException;
import org.bson.Document;

/**
 * @author Igor Shestakov.
 */
public class MongoQuery
{

	private final String mqlQueryString;
	private final Document mqlCommand;

	public MongoQuery(String mqlQuery) throws MongoSQLException
	{
		this.mqlQueryString = mqlQuery;

		try
		{
			this.mqlCommand = Document.parse(mqlQuery);
		}
		catch (Exception e)
		{
			throw new MongoSQLException("Invalid query: " + e.getMessage() + "\n Query: " + mqlQuery);
		}
	}

	public String getMqlQueryString()
	{
		return mqlQueryString;
	}

	public Document getMqlCommand()
	{
		return mqlCommand;
	}
}
