package com.slemma.jdbc;

import com.slemma.jdbc.query.MongoResult;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class MongoScrollableResultSet extends ScrollableResultset<Object> implements
		  java.sql.ResultSet
{
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(MongoScrollableResultSet.class.getName());

	/**
	 * to set the maxFieldSize
	 */
	private int maxFieldSize = 0;

	private MongoResult mongoResult = null;
	/**
	 * This Reference is for storing the Reference for the Statement which
	 * created this Resultset
	 */
	private MongoStatement Statementreference = null;

	public MongoScrollableResultSet(MongoResult mongoResult,
										  MongoPreparedStatement mongoPreparedStatement) {
		logger.debug("Created Scrollable resultset TYPE_SCROLL_INSENSITIVE");
		this.mongoResult = mongoResult;

		try {
			maxFieldSize = mongoPreparedStatement.getMaxFieldSize();
		} catch (SQLException e) {
			// Should not happen.
		}

		if (this.mongoResult == null) {
			this.RowsofResult = null;
		} else {
			this.RowsofResult = this.mongoResult.asArray();
		}
	}

	/**
	 * Constructor of MongoResultset, that initializes all private variables
	 *
	 * @param mongoResultResponse
	 * @param mongoStatementRoot
	 */
	public MongoScrollableResultSet(MongoResult mongoResultResponse,
											  AbstractMongoStatement mongoStatementRoot) {
		logger.debug("Created Scrollable resultset TYPE_SCROLL_INSENSITIVE");
		this.mongoResult = mongoResultResponse;

		try {
			maxFieldSize = mongoStatementRoot.getMaxFieldSize();
		} catch (SQLException e) {
			// Should not happen.
		}

		if (this.mongoResult == null) {
			this.RowsofResult = null;
		} else {
			this.RowsofResult = this.mongoResult.asArray();
		}

		if (mongoStatementRoot instanceof MongoStatement) {
			this.Statementreference = (MongoStatement) mongoStatementRoot;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		if (this.isClosed()) {
			throw new SQLException("This Resultset is Closed");
		}
		int columnCount = this.getMetaData().getColumnCount();
		for (int i = 1; i <= columnCount; i++)
		{
			if (this.getMetaData().getColumnLabel(i).equals(columnLabel))
			{
				return i;
			}
		}
		throw new SQLException("No Such column labeled: " + columnLabel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if (this.isClosed()) {
			throw new SQLException("This Resultset is Closed");
		}
		return new MongoResultsetMetaData(this.mongoResult);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		this.closestrm();

		throwIfClosedOrInvalid();
		throwIfInvalidIndex(columnIndex);

		Document doc = (Document) this.RowsofResult[this.Cursor];
		MongoField field = ((MongoResultsetMetaData) this.getMetaData()).mongoResult.getFields().get(columnIndex - 1);
		ArrayList<String> path = field.getPath();
		for (int i = 0; i < path.size() - 1; i++)
		{
			doc = (Document) doc.get(path.get(i));
		}

		return ConversionHelper.getValueAsObject(field.getType(), doc.get(path.get(path.size() - 1)));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Statement getStatement() throws SQLException {
		return this.Statementreference;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getString(int columnIndex) throws SQLException {
		this.closestrm();

		throwIfClosedOrInvalid();
		throwIfInvalidIndex(columnIndex);

		Document doc = (Document) this.RowsofResult[this.Cursor];
		MongoField field = ((MongoResultsetMetaData) this.getMetaData()).mongoResult.getFields().get(columnIndex - 1);
		ArrayList<String> path = field.getPath();
		for (int i = 0; i < path.size() - 1; i++)
		{
			doc = (Document) doc.get(path.get(i));
		}

		Object data = doc.get(path.get(path.size() - 1));

		if (data == null)
		{
			this.wasnull = true;
			return null;
		}
		else
		{
			this.wasnull = false;
			return data.toString();
		}
	}

	//------------------------- for Jdk1.7 -----------------------------------

	/** {@inheritDoc} */
	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return null;
	}
}
