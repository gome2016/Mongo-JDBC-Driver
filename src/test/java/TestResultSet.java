import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.slemma.jdbc.MongoConnection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author Igor Shestakov.
 */
public class TestResultSet
{

	private static java.sql.Connection con = null;
	private final static Logger logger = LoggerFactory.getLogger(TestResultSet.class.getName());

	@Before
	public void checkConnection() {
		try {
			if (this.con == null || !this.con.isValid(0))
			{
				this.logger.info("Testing the JDBC driver");
				try {
					Class.forName("com.slemma.jdbc.MongoDriver");
//					this.con = DriverManager.getConnection("jdbc:mongodb:mql://192.168.99.100:27017/test");
					this.con = DriverManager.getConnection("jdbc:mongodb:mql://test:test@127.0.0.1:27017/test?&authMechanism=SCRAM-SHA-1");
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
			logger.debug("Oops something went wrong",e);
		}
	}

	@Test
	public void buildInfo() {
		ResultSet rs;
		String query = "{\"buildInfo\": 1}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			Utils.printResultSet(rs);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}

	@Test
	public void nativeFindTest() {
		FindIterable<Document> iterable = ((MongoConnection)con).getNativeDatabase().getCollection("restaurants").find(
				  Document.parse("{ $and: [{'borough': { $ne: 'Bronx'} }, {'cuisine':'Irish'}]}")
		);

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
	}

	@Test
	public void findTest() {
		ResultSet rs;
		String query = "{ " +
				  "\"find\" : \"restaurants\"" +
				  ", \"filter\" : { \"$and\" : [{ \"borough\" : { \"$ne\" : \"Bronx\" } }, { \"cuisine\" : \"Irish\" }] } " +
				  "}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			Utils.printResultSet(rs);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}


	@Test
	public void findWithDateTest() {
		ResultSet rs;
		String query = "{ \"find\" : \"bios\"}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			ResultSetMetaData rsMetadata = rs.getMetaData();
			System.out.println("Columns metadata:");
			for (int i=1; i <= rsMetadata.getColumnCount(); i++) {
				System.out.println("Label: " + rsMetadata.getColumnLabel(i) + "; Data type: " + rsMetadata.getColumnTypeName(i));
			}

			Assert.assertEquals(12, rsMetadata.getColumnType(1));

			Utils.printResultSet(rs);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}


	@Test
	public void findTestMoreOptions() {
		ResultSet rs;
		String query = "{ " +
				  "\"find\" : \"restaurants\"" +
				  ", \"filter\" : { \"$and\" : [{ \"borough\" : { \"$ne\" : \"Bronx\" } }, { \"cuisine\" : \"Irish\" }] } " +
				  ", \"limit\" : 150" +
				  ", \"batchSize\" : 150" +
				  ", \"maxTimeMS\" : 5000" +
				  "}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			int rowCnt = Utils.printResultSet(rs);
			Assert.assertEquals(150,rowCnt);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}


	@Test
	public void findTestLimit() {
		ResultSet rs;
		String query = "{ " +
				  "\"find\" : \"restaurants\"" +
				  ", \"filter\" : { \"$and\" : [{ \"borough\" : { \"$ne\" : \"Bronx\" } }, { \"cuisine\" : \"Irish\" }] } " +
				  ", \"limit\" : 10" +
				  ", \"batchSize\" : 150" +
				  "}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			int rowCnt = Utils.printResultSet(rs);
			Assert.assertEquals(10,rowCnt);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}


	@Test
	public void findTestLimitResultWithArray() {
		ResultSet rs;
		String query = "{ \"find\" : \"zips\", \"filter\" : { \"$and\" : [{ \"state\" : { \"$ne\" : \"MA\" } }, { \"cuisine\" : {\"$ne\":\"BARRE\"} }] } , \"limit\" : 10, \"batchSize\" : 150}";
		try {
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			ResultSetMetaData mRs = rs.getMetaData();
			Assert.assertEquals(4,mRs.getColumnCount());

			int rowCnt = Utils.printResultSet(rs);
			Assert.assertEquals(10,rowCnt);
		}
		catch (SQLException e) {
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}


	@Test
	public void aggregateTest1() {
		ResultSet rs;
		final String query =
				  "{" +
							 "\"aggregate\":\"zips\"" +
							 ", \"pipeline\":[" +
							 "{ \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } },\n" +
							 "{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }" +
							 "]" +
							 "}"
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

	@Test
	public void aggregateTest2() {
		ResultSet rs;
		final String query =
				  "{" +
							 "\"aggregate\":\"zips\"" +
							 ", \"pipeline\":[" +
							 "{ \"$group\": { \"_id\": \"$state\", \"totalPop\": { \"$sum\": \"$pop\" } } },\n" +
							 "{ \"$match\": { \"totalPop\": { \"$gte\": 10000000 } } }" +
							 "]" +
							 "}"
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


	@Test
	public void nativeProjectionTest() {
		FindIterable<Document> iterable = ((MongoConnection)con).getNativeDatabase().getCollection("restaurants").find(
				  Document.parse("{ $and: [{'borough': { $ne: 'Bronx'} }, {'cuisine':'Irish'}]}")
		);

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
	}

}
