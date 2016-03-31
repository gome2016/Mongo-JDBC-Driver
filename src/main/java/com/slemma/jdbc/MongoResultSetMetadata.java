package com.slemma.jdbc;

import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Igor Shestakov.
 */

class MongoResultsetMetaData implements ResultSetMetaData
{
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(MongoResultsetMetaData.class.getName());

	MongoResult mongoResult = null;

	/**
	 * Constructor
	 *
	 * @param mongoResult
	 */
	public MongoResultsetMetaData(MongoResult mongoResult) {
		this.mongoResult = mongoResult;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Returns the database name
	 * </p>
	 *
	 * @return projectID
	 */
	@Override
	public String getCatalogName(int column) throws SQLException
	{
		return this.mongoResult.getDatabase().getName();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not implemented yet.
	 * </p>
	 *
	 * @throws SQLException
	 */
	@Override
	public String getColumnClassName(int column) throws SQLException {
		try {
			return this.mongoResult.getFields().get(column - 1).getClassName();
		}
		catch (IndexOutOfBoundsException e) {
			throw new SQLException(e);
		}
		catch (NullPointerException e) {
			throw new SQLException(e);
		}

	}

	/** {@inheritDoc} */
	@Override
	public int getColumnCount() throws SQLException {
		return this.mongoResult.getColumnCount();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * returns 64*1024
	 * </p>
	 *
	 */
	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return 1024 * 64;
		// TODO Check the maximum lenght of characters contained in this
	}

	/** {@inheritDoc} */
	@Override
	public String getColumnLabel(int column) throws SQLException {
		return this.getColumnName(column);
	}

	/** {@inheritDoc} */
	@Override
	public String getColumnName(int column) throws SQLException {
		if (this.getColumnCount() == 0) {
			throw new SQLException("getColumnName(int)",
					  new IndexOutOfBoundsException());
		}
		try {
			return this.mongoResult.getFields().get(column-1).getName();
		}
		catch (IndexOutOfBoundsException e) {
			throw new SQLException("getColumnName(int)", e);
		}
	}

	/**
	 * {@inheritDoc} <br>
	 * note: This Can only Return due to bigquery:<br>
	 * java.sql.Types.FLOAT<br>
	 * java.sql.Types.BOOLEAN<br>
	 * java.sql.Types.INTEGER<br>
	 * java.sql.Types.VARCHAR
	 * */
	@Override
	public int getColumnType(int column) throws SQLException {
		if (this.getColumnCount() == 0) {
			throw new SQLException("getColumnType(int)",
					  new IndexOutOfBoundsException());
		}
		try {
			return this.mongoResult.getFields().get(column-1).getType();
		}
		catch (IndexOutOfBoundsException e) {
			throw new SQLException("getColumnType(int)", e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public String getColumnTypeName(int column) throws SQLException {
		if (this.getColumnCount() == 0) {
			throw new SQLException("getColumnTypeName(int)",
					  new IndexOutOfBoundsException());
		}
		try {
			return ConversionHelper.getSqlTypeName(this.mongoResult.getFields().get(column-1).getType());
		}
		catch (IndexOutOfBoundsException e) {
			throw new SQLException("getColumnTypeName(int)", e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public int getPrecision(int column) throws SQLException {
		if (this.getColumnCount() == 0) {
			return 0;
		}
		String columnType = "";
		try {
			columnType = this.mongoResult.getFields().get(column - 1).getTypeName();
		}
		catch (IndexOutOfBoundsException e) {
			throw new SQLException("getPrecision(int)", e);
		}

		if (columnType.equals("FLOAT")) {
			return Float.MAX_EXPONENT;
		}
		else
		if (columnType.equals("BOOLEAN")) {
			return 1; // A boolean is 1 bit length, but it asks for byte, so
			// 1
		}
		else
		if (columnType.equals("INTEGER")) {
			return Integer.SIZE;
		}
		else
		if (columnType.equals("STRING")) {
			return 64 * 1024;
		}
		else {
			return 0;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not implemented yet.
	 * </p>
	 *
	 * @throws SQLException
	 */
	@Override
	public int getScale(int column) throws SQLException {
		//TODO; implement method

//		if (this.getColumnType(column) == java.sql.Types.FLOAT) {
//			int max = 0;
//			for (int i = 0; i < this.mongoResult.getRows().size(); i++) {
//				String rowdata = (String) this.mongoResult.getRows().get(i).getF().get(column - 1).getV();
//				if (rowdata.contains(".")) {
//					int pointback = rowdata.length() - rowdata.indexOf(".");
//					if (pointback > max) {
//						pointback = max;
//					}
//				}
//			}
//			return max;
//		}
//		else {
//			return 0;
//		}

		return 0;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * No support in bigquery to get the schema for the column.
	 * </p>
	 *
	 * @return ""
	 */
	@Override
	public String getSchemaName(int column) throws SQLException {
		logger.debug("Function call getSchemaName(" + column +
				  ") will return empty string ");
		return "";
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * No option in BigQuery api to get the Table name from column.
	 * </p>
	 *
	 * @return ""
	 */
	@Override
	public String getTableName(int column) throws SQLException {
		logger.debug("Function call getTableName(" + column +
				  ") will return empty string ");
		return "";
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * No ato increment option in bigquery.
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns false
	 * </p>
	 *
	 * @return true
	 */
	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * We store everything as string.
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns false
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns ResultSetMetaData.columnNullable
	 * </p>
	 *
	 * @return ResultSetMetaData.columnNullable
	 */
	@Override
	public int isNullable(int column) throws SQLException {
		return ResultSetMetaData.columnNullable;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns true
	 * </p>
	 *
	 * @return true
	 */
	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Everything can be in Where.
	 * </p>
	 *
	 * @return true
	 */
	@Override
	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Partly implemented.
	 * </p>
	 *
	 * @return false;
	 */
	@Override
	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns false
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns false
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always throws SQLExceptionL
	 * </p>
	 *
	 * @throws SQLException
	 *             always
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Not found");
	}
}