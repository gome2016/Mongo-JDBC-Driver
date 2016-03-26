package com.slemma.jdbc;

import com.sun.deploy.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;

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

	public String getName()
	{
		return StringUtils.join(path, ".");
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
