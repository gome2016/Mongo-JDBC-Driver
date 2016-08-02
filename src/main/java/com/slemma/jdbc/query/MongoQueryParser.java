package com.slemma.jdbc.query;

import com.slemma.jdbc.MongoSQLException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Igor Shestakov.
 */
public class MongoQueryParser
{
	private static final Pattern countMembersPattern = Pattern.compile("^(?i)select\\s+count\\s*\\(\\s*distinct\\s+(.+?)\\.(.+)\\s*\\)\\s+as\\s+(.+)\\s+from\\s+\\((\\{(.)*\\})\\)\\s+as\\s+.*$");
	private static final Pattern dimMembersPattern = Pattern.compile("^(?i)select\\s+(.+?)\\.(.+)\\s+as\\s+(.+)\\s*,\\s*(.+?)\\.(.+)\\s+as\\s+(.+)\\s*from\\s\\((\\{(.)*\\})\\)\\sas.*$");

	public static MongoQuery parse(String query) throws MongoSQLException
	{
		String cleanedQuery = query.trim().replace("\n", " ");

		Matcher m;

		//try match count query
		m = countMembersPattern.matcher(cleanedQuery);
		if (m.matches()) {
			String sourceField = m.group(2);
			String resultField = m.group(3);
			String mqlQuery = m.group(4);
			return new CountMembersMixedQuery(sourceField, resultField, mqlQuery);
		}

		//try get dimension members query
		m = dimMembersPattern.matcher(cleanedQuery);
		if (m.matches()) {
			String sourceKeyField = m.group(2);
			String resultKeyField = m.group(3);
			String sourceNameField = m.group(5);
			String resultNameField = m.group(6);
			String mqlQuery = m.group(7);
			return new GetMembersMixedQuery(sourceKeyField, resultKeyField,sourceNameField, resultNameField, mqlQuery);
		}

		return new MongoQuery(query);
	}
}
