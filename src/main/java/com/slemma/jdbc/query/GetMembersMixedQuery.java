package com.slemma.jdbc.query;

import com.slemma.jdbc.MongoSQLException;

import java.util.List;

/**
 * @author igorshestakov.
 */
public class GetMembersMixedQuery extends MongoQuery
{
	private List<FieldExprDef> selectedFields;

	public GetMembersMixedQuery(List<FieldExprDef> selectedFields, String mqlQuery) throws MongoSQLException
	{
		super(mqlQuery);
		this.selectedFields =  selectedFields;
	}

	public List<FieldExprDef> getSelectedFields()
	{
		return selectedFields;
	}
}
