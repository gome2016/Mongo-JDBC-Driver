package com.slemma.jdbc.query;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Igor Shestakov.
 */
public class PlaceholderResolver
{
	private final LinkedHashMap<String,String> params;

	private Pattern supportedDataTypes = Pattern.compile("^.*?#(?:String|Boolean|Integer|Long|Double|ObjectId)$");
	private Pattern placeholderArrayWithHardCodedValuesOrOneParam = Pattern.compile("^\\[(.+?(?:,.+?)*)\\]$");
	private Pattern placeholderArrayWithType = Pattern.compile("^\\[.*?#(?:String|Boolean|Integer|Long|Double|ObjectId)\\]$");

	private Pattern stringArrayPattern = Pattern.compile("(?:(?:\"|\'|')(?:.+?)(?:\"|\'|')(?:,(?:\"|\'|')(?:.+?)(?:\"|\'|'))*)");
	private Pattern numberArrayPattern = Pattern.compile("(?:\\d+?(?: *, *\\d+? *)*)");
	private Pattern doubleArrayPattern = Pattern.compile("(?:(?:\\d+?\\.\\d+?)(?: *, *\\d+?\\.\\d+? *)*)");
	private Pattern booleanArrayPattern = Pattern.compile("(?:(?:true|false)(?: *, *(?:true|false) *)*)");

	public PlaceholderResolver(LinkedHashMap<String,String> params)
	{
		this.params = params;
	}

	private String arrayPlaceholder(String stringValue)
	{
		return stringValue.substring(1, stringValue.length() - 1);
	}

	private Object arrayValues(String arrayValuesString)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}

	private Pair<String, Class> placeholderKeyWithType(String stringVal)
	{
		int hashIndex = stringVal.indexOf('#');
		String placeholderKey = stringVal.substring(0, hashIndex);
		String placeholderTypeString = stringVal.substring(hashIndex + 1, stringVal.length());
		Class  placeholderTypeClass;

		switch (placeholderTypeString)
		{
			case "String":
				placeholderTypeClass = String.class;
				break;
			case "Boolean":
				placeholderTypeClass = Boolean.class;
				break;
			case "Integer":
				placeholderTypeClass = Integer.class;
				break;
			case "Long":
				placeholderTypeClass = Long.class;
				break;
			case "Double":
				placeholderTypeClass = Double.class;
				break;
			case "ObjectId":
				placeholderTypeClass = ObjectId.class;
				break;
			default:
				throw new UnsupportedOperationException("datatype not supported. Supported datatypes are String|Boolean|Integer|Double|ObjectId");
		}

		return new ImmutablePair(placeholderKey, placeholderTypeClass);
	}

	public Object resolve(String placeholder)
	{
		Object resolvedValue = null;

//		Matcher m = placeholderArrayWithType.matcher(placeholder);
//		if (m.find()) {
//		}

		return resolvedValue;
	}


}
