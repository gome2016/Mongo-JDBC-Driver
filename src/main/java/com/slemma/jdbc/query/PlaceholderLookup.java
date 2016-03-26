package com.slemma.jdbc.query;

import com.mongodb.BasicDBObject;
import org.bson.BSONObject;

import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * @author Igor Shestakov.
 */
public class PlaceholderLookup
{
	private final PlaceholderResolver placeholderResolver;

	private Pattern placeholderPattern = Pattern.compile("'?\"?(.*?)'?\"?");

	public PlaceholderLookup(LinkedHashMap<String,String> params)
	{
		this.placeholderResolver = new PlaceholderResolver(params);
	}

	public BasicDBObject traverse(BasicDBObject basicDBObject)
	{
		BasicDBObject result = new BasicDBObject();

		for (String k : basicDBObject.keySet())
		{
			Object value = basicDBObject.get(k);
			if (value instanceof BasicDBObject){
				result.putAll((BSONObject) traverse((BasicDBObject)value));
			}
			else if (value instanceof String)
			{
				Object resolvedValue = placeholderResolver.resolve((String)value);
				result.put(k, resolvedValue);
			} else {
				result.put(k, value);
			}
		}

		return result;
	}

	public int substitute(String placeholder)
	{
//		val placeholderPattern = "'?\"?(.*?)'?\"?".r
//		val placeholderPattern(placeholderWOQuotes) = placeholder
//		placeholderResolver.resolve(placeholderWOQuotes).toString.toInt

		throw new UnsupportedOperationException("Not implemented yet");
	}


}
