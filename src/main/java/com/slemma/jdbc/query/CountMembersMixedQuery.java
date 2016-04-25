package com.slemma.jdbc.query;

import com.slemma.jdbc.MongoSQLException;

/**
 * @author igorshestakov.
 */
public class CountMembersMixedQuery extends MongoQuery
{

	private String sourceFieldName;
	private String resultFieldName;

	public CountMembersMixedQuery(String sourceFieldName, String resultFieldName, String mqlQuery) throws MongoSQLException
	{
		super(mqlQuery);
		this.sourceFieldName =  sourceFieldName;
		this.resultFieldName = resultFieldName;
	}

	public String getSourceFieldName()
	{
		return sourceFieldName;
	}

	public String getResultFieldName()
	{
		return resultFieldName;
	}
}
