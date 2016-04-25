import com.slemma.jdbc.MongoConnection;
import com.slemma.jdbc.MongoSQLException;
import com.slemma.jdbc.query.MongoQuery;
import com.slemma.jdbc.query.MongoQueryParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * @author Igor Shestakov.
 */


public class TestMixedQuery
{
	private static java.sql.Connection con = null;
	private final static Logger logger = LoggerFactory.getLogger(TestMixedQuery.class.getName());

	@Before
	public void checkConnection() {
		try {
			if (this.con == null || !this.con.isValid(0))
			{
				this.logger.info("Testing the JDBC driver");
				try {
					Class.forName("com.slemma.jdbc.MongoDriver");

					this.con = DriverManager.getConnection("jdbc:mongodb:mql://192.168.99.100:27017/test");
				}
				catch (Exception e) {
					e.printStackTrace();
					this.logger.error("Error in connection" + e.toString());
					Assert.fail("Exception:" + e.toString());
				}
				this.logger.info(((MongoConnection) this.con).getUrl());
			}
		}
		catch (SQLException e) {
			logger.debug("Oops something went wrong", e);
		}
	}


	@Test
	public void countDimensionMembersQuery() {
		String query = "select count(DISTINCT factdata_view._id) as c0 from ({\n" +
				  "\"aggregate\":\"zips\"\n" +
				  ", \"pipeline\":[\n" +
				  "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
				  "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
				  "  ]\n" +
				  "}) as factdata_view";

		MongoQuery mixedQuery = null;
		try
		{
			mixedQuery = MongoQueryParser.parse(query);
		}
		catch (MongoSQLException e)
		{
			Assert.fail(e.getMessage());
		}

		Assert.assertNotNull(mixedQuery);
		if (mixedQuery != null) {
			Assert.assertEquals("{\n" +
					  "\"aggregate\":\"zips\"\n" +
					  ", \"pipeline\":[\n" +
					  "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
					  "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
					  "  ]\n" +
					  "}", mixedQuery.getMqlQueryString());
		}
	}

	@Test
	public void getDimensionMembersQuery() {
		String query = "select factdata_view._id as c0, factdata_view._id as c1 from ({\n" +
				  "\"aggregate\":\"zips\"\n" +
				  ", \"pipeline\":[\n" +
				  "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
				  "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
				  "  ]\n" +
				  "}) as factdata_view group by c0, c1 order by c0 ASC";

		MongoQuery mixedQuery = null;
		try
		{
			mixedQuery = MongoQueryParser.parse(query);
		}
		catch (MongoSQLException e)
		{
			Assert.fail(e.getMessage());
		}
		Assert.assertNotNull(mixedQuery);
		if (mixedQuery != null) {
			Assert.assertEquals("{\n" +
					  "\"aggregate\":\"zips\"\n" +
					  ", \"pipeline\":[\n" +
					  "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
					  "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
					  "  ]\n" +
					  "}", mixedQuery.getMqlQueryString());
		}
	}

	@Test
	public void mixedQueryDistinctCount() {
		ResultSet rs;
		final String query =
				  "select count(DISTINCT factdata_view._id) as c0 from ({\n" +
							 "\"aggregate\":\"zips\"\n" +
							 ", \"pipeline\":[\n" +
							 "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
							 "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
							 "  ]\n" +
							 "}) as factdata_view"
				  ;
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			int rowCnt = Utils.printResultSet(rs);
			Assert.assertEquals(1,rowCnt);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}

	@Test
	public void mixedQueryGetDimensionMembers() {
		ResultSet rs;
		final String query =
				  "select factdata_view._id as c0, factdata_view._id as c1 from ({\n" +
							 "\"aggregate\":\"zips\"\n" +
							 ", \"pipeline\":[\n" +
							 "  { \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } }\n" +
							 "  ,{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }\n" +
							 "  ]\n" +
							 "}) as factdata_view group by c0, c1 order by c0 ASC"
				  ;
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			int rowCnt = Utils.printResultSet(rs);
			Assert.assertEquals(7,rowCnt);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}
}
