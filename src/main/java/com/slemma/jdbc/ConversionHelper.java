package com.slemma.jdbc;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * ConversionHelper defines mappings from common Java types to
 * corresponding SQL types.
 */
public class ConversionHelper
{
	private static final Map<Class, Integer> javaToSqlRules;

	static
	{
		javaToSqlRules = new HashMap<Class, Integer>();
		javaToSqlRules.put(String.class, Types.VARCHAR);
		javaToSqlRules.put(byte[].class, Types.BINARY);
		javaToSqlRules.put(boolean.class, Types.BOOLEAN);
		javaToSqlRules.put(Boolean.class, Types.BOOLEAN);
		javaToSqlRules.put(char.class, Types.CHAR);
		javaToSqlRules.put(Character.class, Types.CHAR);
		javaToSqlRules.put(short.class, Types.SMALLINT);
		javaToSqlRules.put(Short.class, Types.SMALLINT);
		javaToSqlRules.put(int.class, Types.INTEGER);
		javaToSqlRules.put(Integer.class, Types.INTEGER);
		javaToSqlRules.put(long.class, Types.BIGINT);
		javaToSqlRules.put(Long.class, Types.BIGINT);
		javaToSqlRules.put(float.class, Types.REAL);
		javaToSqlRules.put(Float.class, Types.REAL);
		javaToSqlRules.put(double.class, Types.DOUBLE);
		javaToSqlRules.put(Double.class, Types.DOUBLE);
		javaToSqlRules.put(java.sql.Date.class, Types.DATE);
		javaToSqlRules.put(Time.class, Types.TIME);
		javaToSqlRules.put(Timestamp.class, Types.TIMESTAMP);
	}

	public static boolean sqlTypeExists(Class javaClass)
	{
		return (javaToSqlRules.get(javaClass) != null) ? true : false;
	}

	public static int lookup(Class javaClass)
	{
		return javaToSqlRules.get(javaClass);
	}

	public static String getSqlTypeName(int type)
	{
		switch (type)
		{
			case Types.BIT:
				return "BIT";
			case Types.TINYINT:
				return "TINYINT";
			case Types.SMALLINT:
				return "SMALLINT";
			case Types.INTEGER:
				return "INTEGER";
			case Types.BIGINT:
				return "BIGINT";
			case Types.FLOAT:
				return "FLOAT";
			case Types.REAL:
				return "REAL";
			case Types.DOUBLE:
				return "DOUBLE";
			case Types.NUMERIC:
				return "NUMERIC";
			case Types.DECIMAL:
				return "DECIMAL";
			case Types.CHAR:
				return "CHAR";
			case Types.VARCHAR:
				return "VARCHAR";
			case Types.LONGVARCHAR:
				return "LONGVARCHAR";
			case Types.DATE:
				return "DATE";
			case Types.TIME:
				return "TIME";
			case Types.TIMESTAMP:
				return "TIMESTAMP";
			case Types.BINARY:
				return "BINARY";
			case Types.VARBINARY:
				return "VARBINARY";
			case Types.LONGVARBINARY:
				return "LONGVARBINARY";
			case Types.NULL:
				return "NULL";
			case Types.OTHER:
				return "OTHER";
			case Types.JAVA_OBJECT:
				return "JAVA_OBJECT";
			case Types.DISTINCT:
				return "DISTINCT";
			case Types.STRUCT:
				return "STRUCT";
			case Types.ARRAY:
				return "ARRAY";
			case Types.BLOB:
				return "BLOB";
			case Types.CLOB:
				return "CLOB";
			case Types.REF:
				return "REF";
			case Types.DATALINK:
				return "DATALINK";
			case Types.BOOLEAN:
				return "BOOLEAN";
			case Types.ROWID:
				return "ROWID";
			case Types.NCHAR:
				return "NCHAR";
			case Types.NVARCHAR:
				return "NVARCHAR";
			case Types.LONGNVARCHAR:
				return "LONGNVARCHAR";
			case Types.NCLOB:
				return "NCLOB";
			case Types.SQLXML:
				return "SQLXML";
		}

		return "?";
	}

}