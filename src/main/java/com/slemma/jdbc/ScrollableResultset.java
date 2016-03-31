package com.slemma.jdbc;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * @author Igor Shestakov.
 */

abstract class ScrollableResultset<T> implements java.sql.ResultSet {

	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(ScrollableResultset.class.getName());

	/** Reference for holding the current InputStream given back by get methods */
	protected InputStream Strm = null;

	/** The boolean that holds if the last get has given back null or not */
	protected boolean wasnull = false;

	/** The Array which get iterated with cursor */
	protected T[] RowsofResult;

	/** This holds the current position of the cursor */
	protected int Cursor = -1;
	/** This holds if the resultset is closed or not */
	protected Boolean Closed = false;


	protected void throwIfClosed() throws SQLException
	{
		if (this.isClosed()) {
			throw new SQLException("This Resultset is Closed");
		}
	}

	protected void throwIfClosedOrInvalid() throws SQLException
	{
		throwIfClosed();
		if (this.RowsofResult == null) throw new SQLException("Invalid position!");
	}

	protected void throwIfInvalidIndex(int columnIndex) throws SQLException
	{
		if (this.getMetaData().getColumnCount() < columnIndex
				  || columnIndex < 1) {
			throw new SQLException("ColumnIndex is not valid");
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean absolute(int row) throws SQLException
	{
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null) {
			return false;
		}
		if (row > 0) {
			if (row <= this.RowsofResult.length) {
				this.Cursor = row - 1;
				return true;
			}
			else {
				// An attempt to position the cursor beyond the first/last row
				// in the result set leaves the cursor before the first row or
				// after the last row.
				this.Cursor = this.RowsofResult.length;
				// false if the cursor is before the first row or after the last
				// row
				return false;
			}
		}
		// If the given row number is negative, the cursor moves to an absolute
		// row position with respect to the end of the result set.
		else
		if (row < 0) {
			if (Math.abs(row) <= this.RowsofResult.length) {
				this.Cursor = this.RowsofResult.length + row;
				return true;
			}
			else {
				// An attempt to position the cursor beyond the first/last
				// row
				// in the result set leaves the cursor before the first row
				// or
				// after the last row.
				this.Cursor = -1;
				// false if the cursor is before the first row or after the
				// last
				// row
				return false;
			}
		}
		// if 0
		else {
                /*
                 * //Check if cursor is before first of after last row if
                 * (this.Cursor == RowsofResult.size() || this.Cursor == -1)
                 * return
                 * false; else return true;
                 */
			if (this.Cursor == this.RowsofResult.length
					  || this.Cursor == -1) {
				return false;
			}
			else {
				this.Cursor = -1;
			}
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public void afterLast() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null) {
			return;
		}
		if (this.RowsofResult.length > 0) {
			this.Cursor = this.RowsofResult.length;
		}
	}

	/** {@inheritDoc} */
	@Override
	public void beforeFirst() throws SQLException {
		throwIfClosed();
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		if (this.RowsofResult == null) {
			return;
		}
		if (this.RowsofResult.length > 0) {
			this.Cursor = -1;
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
	public void cancelRowUpdates() throws SQLException {
		throw new UnsupportedOperationException("cancelWorUpdates()");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Currently its a noop
	 * </p>
	 */
	@Override
	public void clearWarnings() throws SQLException {
		throwIfClosed();
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws SQLException {
		// TODO free occupied resources
		this.Closed = true;
		this.RowsofResult = null;
	}

	/**
	 * If the Strm reference is not null it closes the underlying stream, if an
	 * error occurs throws SQLException</p>
	 *
	 * @throws SQLException
	 *             if error occurs while trying to close the stream
	 */
	protected void closestrm() throws SQLException {
		if (this.Strm != null) {
			try {
				this.Strm.close();
			}
			catch (IOException e) {
				throw new SQLException(e);
			}
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * We support read only functions in the current version.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void deleteRow() throws SQLException {
		throw new UnsupportedOperationException("deleteRow()");

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
	public int findColumn(String columnLabel) throws SQLException {
		throw new SQLException("Not implemented." + "findColumn(string)");
	}

	/** {@inheritDoc} */
	@Override
	public boolean first() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null || this.RowsofResult.length == 0) {
			return false;
		}
		else {
			this.Cursor = 0;
			return true;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Arrays are not supported in the current version
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getArray(int)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Arrays are not supported in the current version
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getArray(string)");
	}

	/** {@inheritDoc} */
	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		this.closestrm();
		java.io.InputStream inptstrm;
		String Value = this.getString(columnIndex);
		if (Value == null) {
			this.wasnull = true;
			this.Strm = null;
			return this.Strm;
		}
		else {
			this.wasnull = false;
			try {
				inptstrm = new java.io.ByteArrayInputStream(
						  Value.getBytes("US-ASCII"));
			}
			catch (UnsupportedEncodingException e) {
				throw new SQLException(e);
			}
			this.Strm = inptstrm;
			return this.Strm;
		}
	}

	/** {@inheritDoc} */
	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		this.closestrm();
		java.io.InputStream inptstrm;
		String Value = this.getString(columnLabel);
		if (Value == null) {
			this.wasnull = true;
			this.Strm = null;
			return this.Strm;
		}
		else {
			this.wasnull = false;
			try {
				inptstrm = new java.io.ByteArrayInputStream(
						  Value.getBytes("US-ASCII"));
			}
			catch (UnsupportedEncodingException e) {
				throw new SQLException(e);
			}
			this.Strm = inptstrm;
			return this.Strm;
		}
	}

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {

		String coltype = this.getMetaData().getColumnTypeName(columnIndex);
		if (coltype.equals("STRING")) {
			String Value = this.getString(columnIndex);
			if (this.wasNull()) {
				return null;
			}
			else {
				try {
					return new java.math.BigDecimal(Value);
				}
				catch (NumberFormatException e) {
					throw new SQLException(e);
				}
			}
		}
		else
		if (coltype.equals("INTEGER")) {
			int Value = this.getInt(columnIndex);
			if (this.wasNull()) {
				return null;
			}
			else {
				return new java.math.BigDecimal(Value);
			}

		}
		else
		if (coltype.equals("FLOAT")) {
			Float Value = this.getFloat(columnIndex);
			if (this.wasNull()) {
				return null;
			}
			else {
				return new java.math.BigDecimal(Value);
			}
		}
		else
		if (coltype.equals("BOOLEAN")) {
			throw new NumberFormatException(
					  "Cannot format Boolean to BigDecimal");
		}
		else {
			throw new NumberFormatException("Undefined format");
		}
	}

	// Implemented Get functions Using Cursor

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			  throws SQLException {
		return this.getBigDecimal(columnIndex).setScale(scale);
	}

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getBigDecimal(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			  throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getBigDecimal(columnIndex, scale);
	}

	/** {@inheritDoc} */
	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		this.closestrm();
		java.io.InputStream inptstrm;
		String Value = this.getString(columnIndex);
		if (Value == null) {
			this.wasnull = true;
			this.Strm = null;
			return this.Strm;
		}
		else {
			this.wasnull = false;
			inptstrm = new java.io.ByteArrayInputStream(Value.getBytes());
			this.Strm = inptstrm;
			return this.Strm;
		}
	}

	/** {@inheritDoc} */
	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		this.closestrm();
		java.io.InputStream inptstrm;
		String Value = this.getString(columnLabel);
		if (Value == null) {
			this.wasnull = true;
			this.Strm = null;
			return this.Strm;
		}
		else {
			this.wasnull = false;
			inptstrm = new java.io.ByteArrayInputStream(Value.getBytes());
			this.Strm = inptstrm;
			return this.Strm;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getBlob(int)");

	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getBlob(string)");
	}

	/** {@inheritDoc} */
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return false;
		}
		else {
			return Boolean.parseBoolean(Value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getBoolean(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Byte.parseByte(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public byte getByte(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getByte(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return Value.getBytes();
		}
	}

	/** {@inheritDoc} */
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getBytes(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		this.closestrm();
		String Value = this.getString(columnIndex);
		if (Value == null) {
			this.wasnull = true;
			return null;
		}
		else {
			this.wasnull = false;
			Reader rdr = new StringReader(Value);
			return rdr;
		}
	}

	/** {@inheritDoc} */
	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		this.closestrm();
		String Value = this.getString(columnLabel);
		if (Value == null) {
			this.wasnull = true;
			return null;
		}
		else {
			this.wasnull = false;
			Reader rdr = new StringReader(Value);
			return rdr;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getClob(int)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getClob(string)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always Returns ResultSet.CONCUR_READ_ONLY
	 * </p>
	 *
	 * @return ResultSet.CONCUR_READ_ONLY
	 */
	@Override
	public int getConcurrency() throws SQLException {
		throwIfClosed();
		return ResultSet.CONCUR_READ_ONLY;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public String getCursorName() throws SQLException {
		throw new UnsupportedOperationException("getCursorName()");
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Date(value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Date(value + cal.getTimeZone().getRawOffset());
		}
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getDate(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getDate(columnIndex, cal);
	}

	/** {@inheritDoc} */
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Double.parseDouble(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getDouble(columnIndex);
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
	public int getFetchDirection() throws SQLException {
		throw new SQLException("Not implemented." + "getFetchDirection()");
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
	public int getFetchSize() throws SQLException {
		throw new SQLException("Not implemented." + "getfetchSize()");
	}

	/** {@inheritDoc} */
	@Override
	public float getFloat(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Float.parseFloat(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getFloat(columnIndex);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Read only mode, no commits.
	 * </p>
	 *
	 * @return CLOSE_CURSORS_AT_COMMIT
	 */
	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	/** {@inheritDoc} */
	@Override
	public int getInt(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Integer.parseInt(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public int getInt(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getInt(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public long getLong(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Long.parseLong(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public long getLong(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getLong(columnIndex);
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
	public ResultSetMetaData getMetaData() throws SQLException {
		logger.debug("ResultSetMetaData getMetaData()" + "Not implemented");
		throw new SQLException("Not implemented." + "getMetaData()");
		// TODO Implement
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Returns the value of getCharacterStream(columnIndex)
	 * </p>
	 *
	 * @see #getCharacterStream(int)
	 */
	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return this.getCharacterStream(columnIndex);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Returns the value of getCharacterStream(columnLabel)
	 * </p>
	 *
	 * @see #getCharacterStream(String)
	 */
	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return this.getCharacterStream(columnLabel);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getNClob(int");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getNClob(string)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Returns the value getString(columnIndex)
	 * </p>
	 *
	 * @see #getString(int)
	 */
	@Override
	public String getNString(int columnIndex) throws SQLException {
		return this.getString(columnIndex);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Returns the value getString(columnLabel)
	 * </p>
	 *
	 * @see #getString(String)
	 */
	@Override
	public String getNString(String columnLabel) throws SQLException {
		return this.getString(columnLabel);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			  throws SQLException {
		throw new UnsupportedOperationException("getObject(int,Map)");
		// TODO Implement TypeMaps
	}

	/** {@inheritDoc} */
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getObject(columnIndex);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not s.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			  throws SQLException {
		throw new UnsupportedOperationException("getObject(string,Map)");
		// TODO Implement TypeMaps
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getRef(int)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getref(String)");
	}

	/** {@inheritDoc} */
	@Override
	public int getRow() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null || this.RowsofResult.length == 0
				  || this.Cursor == -1
				  || this.Cursor > this.RowsofResult.length - 1) {
			return 0;
		}
		else {
			return this.Cursor + 1;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("getRowId(int)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("getRowId(String)");
	}

	/** {@inheritDoc} */
	@Override
	public short getShort(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return 0;
		}
		else {
			try {
				return Short.parseShort(Value);
			}
			catch (NumberFormatException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public short getShort(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getShort(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("SQLXML not supported yet");
	}

	/** {@inheritDoc} */
	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getSQLXML(columnIndex);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always Returns null
	 * </p>
	 *
	 * @return null
	 */
	@Override
	public Statement getStatement() throws SQLException {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public String getString(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getString(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Time(value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        /*
         * Select STRFTIME_UTC_USEC(NOW(),'%x-%X%Z') AS One,
         * FORMAT_UTC_USEC(NOW()) as Two"; Result: One Two 08/21/12-15:40:45GMT
         * 2012-08-21 15:40:45.703908
         */
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Time(cal.getTimeZone().getRawOffset() + value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getTime(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getTime(columnIndex, cal);
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Timestamp(value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			  throws SQLException {
		Long value = this.getLong(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			return new java.sql.Timestamp(cal.getTimeZone().getRawOffset()
					  + value);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getTimestamp(columnIndex);
	}

	/** {@inheritDoc} */
	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			  throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
		return this.getTimestamp(columnIndex, cal);
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns ResultSet.TYPE_SCROLL_INSENSITIVE
	 * </p>
	 *
	 * @return ResultSet.TYPE_SCROLL_INSENSITIVE
	 */
	@Override
	public int getType() throws SQLException {
		throwIfClosed();
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				  "Deprecated. use getCharacterStream in place of getUnicodeStream");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(
				  "Deprecated. use getCharacterStream in place of getUnicodeStream");
	}

	/** {@inheritDoc} */
	@Override
	public URL getURL(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if (this.wasNull()) {
			return null;
		}
		else {
			try {
				return new URL(Value);
			}
			catch (MalformedURLException e) {
				throw new SQLException(e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public URL getURL(String columnLabel) throws SQLException {
		String Value = this.getString(columnLabel);
		if (this.wasNull()) {
			return null;
		}
		else {
			try {
				return new URL(Value);
			}
			catch (MalformedURLException e) {
				throw new SQLException(e);
			}
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Always returns null
	 * </p>
	 *
	 * @return null
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO implement error handling
		return null;
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void insertRow() throws SQLException {
		throw new UnsupportedOperationException("insertRow()");
	}

	/** {@inheritDoc} */
	@Override
	public boolean isAfterLast() throws SQLException {
		throwIfClosed();
		if (this.RowsofResult != null
				  && this.Cursor == this.RowsofResult.length
				  && this.RowsofResult.length != 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean isBeforeFirst() throws SQLException {
		throwIfClosed();
		if (this.RowsofResult != null && this.Cursor == -1
				  && this.RowsofResult.length != 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean isClosed() throws SQLException {
		return this.Closed;
	}

	/** {@inheritDoc} */
	@Override
	public boolean isFirst() throws SQLException {
		throwIfClosed();
		if (this.Cursor == 0 && this.RowsofResult != null
				  && this.RowsofResult.length != 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean isLast() throws SQLException {
		throwIfClosed();
		if (this.RowsofResult != null
				  && this.Cursor == this.RowsofResult.length - 1
				  && this.RowsofResult.length - 1 >= 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not implemented yet.
	 * </p>
	 *
	 * @return false
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public boolean last() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null || this.RowsofResult.length == 0) {
			return false;
		}
		else {
			this.Cursor = this.RowsofResult.length - 1;
			return true;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new UnsupportedOperationException("moveToCurrentRow()");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void moveToInsertRow() throws SQLException {
		throw new UnsupportedOperationException("moveToInsertRow()");
	}

	/** {@inheritDoc} */
	@Override
	public boolean next() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null) {
			return false;
		}
		if (this.RowsofResult.length > this.Cursor + 1) {
			this.Cursor++;
			return true;
		}
		else {
			this.Cursor = this.RowsofResult.length;
			return false;
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean previous() throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null) {
			return false;
		}
		if (this.Cursor > 0) {
			this.Cursor--;
			return true;
		}
		else {
			this.Cursor = -1;
			return false;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not supported.
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void refreshRow() throws SQLException {
		throw new UnsupportedOperationException("refreshRow()");
	}

	/** {@inheritDoc} */
	@Override
	public boolean relative(int rows) throws SQLException {
		if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException(
					  "The Type of the Resultset is TYPE_FORWARD_ONLY");
		}
		throwIfClosed();
		if (this.RowsofResult == null) {
			return false;
		}
		if (rows == 0) {
			if (this.RowsofResult.length != 0
					  && this.Cursor < this.RowsofResult.length
					  && this.Cursor > -1) {
				return true;
			}
			else {
				return false;
			}
		}
		else
		if (rows < 0) {
			if (this.Cursor + rows < 0) {
				this.Cursor = -1;
				return false;
			}
			else {
				this.Cursor = this.Cursor + rows;
				return true;
			}
		}
		else
		if (rows + this.Cursor > (this.RowsofResult.length - 1)) {
			this.Cursor = this.RowsofResult.length;
			return false;
		}
		else {
			this.Cursor += rows;
			return true;
		}
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public boolean rowDeleted() throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public boolean rowInserted() throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public boolean rowUpdated() throws SQLException {
		throw new UnsupportedOperationException();
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
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("Not implemented." + "setFetchDirection(int)");
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
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException("Not implemented." + "setFetchSize(int)");
	}

	/**
	 * @throws SQLException
	 *             if Cursor is not in a valid Position
	 */
	public void ThrowIfCursorNotValidException() throws SQLException {
		if (this.RowsofResult == null || this.RowsofResult.length == 0) {
			throw new SQLException("There are no rows in this Resultset"
					  + String.valueOf(this.Cursor) + "RowsofResult.length"
					  + String.valueOf(this.RowsofResult.length));
		}
		else
		if (this.Cursor >= this.RowsofResult.length || this.Cursor <= -1) {
			throw new SQLException(
					  "Cursor is not in a valid Position. Cursor Position is:"
								 + String.valueOf(this.Cursor)
								 + "RowsofResult.length"
								 + String.valueOf(this.RowsofResult.length));
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
	@SuppressWarnings("hiding")
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Not implemented." + "unwrap(Class<T>)");
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
											 long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
								  long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBoolean(String columnLabel, boolean x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
												 int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
												 long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Not impel
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
												  long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNString(int columnIndex, String nString)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNString(String columnLabel, String nString)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * <p>
	 * <h1>Implementation Details:</h1><br>
	 * Throws UnsupportedOperationException
	 * </p>
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			  throws SQLException {
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	@Override
	public boolean wasNull() throws SQLException {
		return this.wasnull;
	}
}