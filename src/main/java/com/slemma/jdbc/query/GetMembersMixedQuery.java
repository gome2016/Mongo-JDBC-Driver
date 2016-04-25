package com.slemma.jdbc.query;

import com.slemma.jdbc.MongoSQLException;

/**
 * @author igorshestakov.
 */
public class GetMembersMixedQuery extends MongoQuery
{
	private String sourceKeyField;
	private String resultKeyField;
	private String sourceNameField;
	private String resultNameField;

	public GetMembersMixedQuery(String sourceKeyField, String resultKeyField, String sourceNameField, String resultNameField, String mqlQuery) throws MongoSQLException
	{
		super(mqlQuery);
		this.sourceKeyField =  sourceKeyField;
		this.resultKeyField = resultKeyField;
		this.sourceNameField =  sourceNameField;
		this.resultNameField = resultNameField;
	}

	public String getSourceKeyField()
	{
		return sourceKeyField;
	}

	public String getResultKeyField()
	{
		return resultKeyField;
	}

	public String getSourceNameField()
	{
		return sourceNameField;
	}

	public String getResultNameField()
	{
		return resultNameField;
	}
}
