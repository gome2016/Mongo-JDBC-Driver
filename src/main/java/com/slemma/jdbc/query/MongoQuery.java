package com.slemma.jdbc.query;

import java.util.List;

/**
 * @author Igor Shestakov.
 */
public class MongoQuery
{
	private List<String> queryParts;
	private String collectionName;

	public MongoQuery(List<String> queryParts, String collectionName)
	{
		this.queryParts = queryParts;
		this.collectionName = collectionName;
	}

	public List<String> getQueryParts()
	{
		return queryParts;
	}

	public String getCollectionName()
	{
		return collectionName;
	}
}
