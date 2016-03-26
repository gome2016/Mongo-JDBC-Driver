package com.slemma.jdbc.query;

import com.slemma.jdbc.query.MongoQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Igor Shestakov.
 */
public class MongoQueryParser
{

	private Pattern findPattern = Pattern.compile("\\.find\\(\\{.*?\\}\\)");
	private Pattern findContentPattern = Pattern.compile("\\.find\\((\\{.*?\\})\\)");

	private Pattern plainFindPattern = Pattern.compile("\\.find\\(\\)");

	private Pattern sortPattern = Pattern.compile("\\.sort\\(\\{.*?\\}\\)");
	private Pattern sortContentPattern = Pattern.compile("\\.sort\\((\\{.*?\\})\\)");

	private Pattern skipPattern = Pattern.compile("\\.skip\\(.*?\\)");
	private Pattern skipContentPattern = Pattern.compile("\\.skip\\((.*?)\\)");

	private Pattern limitPattern = Pattern.compile("\\.limit\\(.*?\\)");
	private Pattern limitContentPattern = Pattern.compile("\\.limit\\((.*?)\\)");

	private Pattern aggregatePattern = Pattern.compile("\\.aggregate\\(\\[?\\{.*?\\}\\]?\\)");
	private Pattern aggregateContentPattern = Pattern.compile("\\.aggregate\\((\\[?\\{.*?\\}\\]?)\\)");

	private Pattern collectionNamePattern = Pattern.compile("db\\.(.*?)(?:\\.find\\(.*?\\))?(?:\\.aggregate\\(.*?\\))?");

	public MongoQuery parse(String queryString, LinkedHashMap<String,String> queryParameters)
	{
		PlaceholderLookup placeholderLookup = new PlaceholderLookup(queryParameters);
		String formattedQueryString = new MongoQueryCleaner().clean(queryString);

//		val findQueries: List[Query] = findPattern.findAllIn(formattedQueryString).toList.map(q => {
//			  val findContentPattern(findQueryString) = q;
//		FindQuery(findQueryString, placeholderLookup)
//		})
		Matcher m = findPattern.matcher(formattedQueryString);
		List<String> findQueries = new ArrayList<String>();
		while (m.find()) {
			findQueries.add(m.group());
		}

//		val plainFindQueries: List[Query] = plainFindPattern.findAllIn(formattedQueryString).toList.map(q => {
//			  FindQuery("", placeholderLookup)
//	})

		m = plainFindPattern.matcher(formattedQueryString);
		List<String> plainFindQueries = new ArrayList<String>();
		while (m.find()) {
			plainFindQueries.add(m.group());
		}

//		val sortQueries: List[Query] = sortPattern.findAllIn(formattedQueryString).toList.map(q => {
//			  val sortContentPattern(sortQueryString) = q;
//		SortQuery(sortQueryString, placeholderLookup)
//		})

		m = sortPattern.matcher(formattedQueryString);
		List<String> sortQueries = new ArrayList<String>();
		while (m.find()) {
			sortQueries.add(m.group());
		}

//		val skipQueries: List[Query] = skipPattern.findAllIn(formattedQueryString).toList.map(q => {
//			  val skipContentPattern(skipQueryString) = q;
//		SkipQuery(skipQueryString, placeholderLookup)
//		})

		m = skipPattern.matcher(formattedQueryString);
		List<String> skipQueries = new ArrayList<String>();
		while (m.find()) {
			skipQueries.add(m.group());
		}

//		val limitQueries: List[Query] = limitPattern.findAllIn(formattedQueryString).toList.map(q => {
//			  val limitContentPattern(limitQueryString) = q;
//		LimitQuery(limitQueryString, placeholderLookup)
//		})

		m = limitPattern.matcher(formattedQueryString);
		List<String> limitQueries = new ArrayList<String>();
		while (m.find()) {
			limitQueries.add(m.group());
		}

//		val aggregationQueries: List[Query] = aggregatePattern.findAllIn(formattedQueryString).toList.map(q => {
//			  val aggregateContentPattern(aggregationQueryString) = q;
//		AggregateQuery(aggregationQueryString, placeholderLookup)
//		})

		m = aggregatePattern.matcher(formattedQueryString);
		List<String> aggregationQueries = new ArrayList<String>();
		while (m.find()) {
			aggregationQueries.add(m.group());
		}

//		val collectionNamePattern(collectionName) = formattedQueryString

		String collectionName;
		m = collectionNamePattern.matcher(formattedQueryString);
		if (m.find( )) {
			collectionName = m.group(0);
		} else {
			throw new RuntimeException("Undefined collection name");
		}

//		val queryParts: List[Query] = findQueries ::: plainFindQueries ::: sortQueries ::: skipQueries ::: limitQueries ::: aggregationQueries
//		if (!queryParts.isEmpty) new MongoQuery(queryParts, collectionName) else throw new RuntimeException("invalid query")

		List<String> queryParts = new ArrayList<String>();
		queryParts.addAll(findQueries);
		queryParts.addAll(plainFindQueries);
		queryParts.addAll(sortQueries);
		queryParts.addAll(skipQueries);
		queryParts.addAll(limitQueries);
		queryParts.addAll(aggregationQueries);

		if (queryParts.size()>0)
			return new MongoQuery(queryParts, collectionName);
		else
			throw new RuntimeException("Invalid query");
	}

}
