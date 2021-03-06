package com.slemma.jdbc;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/**
 * @author Igor Shestakov.
 */

public class MongoField
{
	/**
	 * java.sql.Types
	 */
	private int type;
	private Class clazz;
	private ArrayList<String> path;

	public static final String FIELD_LVL_DELIMETER = ".";
	public static final String FIELD_LVL_DELIMETER_REGEX_FOR_SPLIT = "\\.";

	public MongoField(int type, Class clazz, ArrayList<String> path)
	{
		this.type = type;
		this.clazz = clazz;
		this.path = path;
	}

	public int getType()
	{
		return type;
	}

	public void setType(int type)
	{
		this.type = type;
	}

	public String getTypeName()
	{
		return ConversionHelper.getSqlTypeName(type);
	}

	public String getName()
	{
		return StringUtils.join(path, this.FIELD_LVL_DELIMETER);
	}

	public ArrayList<String> getPath()
	{
		return path;
	}

	public String getClassName()
	{
		return this.clazz.getName();
	}
}
