import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.slemma.jdbc.MongoConnection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

					this.con = DriverManager.getConnection("jdbc:mongodb:mql://192.168.99.100:27017/test");
				}
				catch (Exception e) {
					e.printStackTrace();
					this.logger.error("Error in connection" + e.toString());
					Assert.fail("Exception:" + e.toString());
				}
				this.logger.info(((MongoConnection) this.con)
						  .getURLPART());
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
		String query = "{ \"find\" : \"restaurants\", \"filter\" : { \"$and\" : [{ \"borough\" : { \"$ne\" : \"Bronx\" } }, { \"cuisine\" : \"Irish\" }] } }";
//		String query = "{ \"find\" : \"restaurants\", \"filter\" : { \"$and\" : [{ \"borough\" : { \"$ne\" : \"Bronx\" } }, { \"cuisine\" : \"Irish\" }] } }";
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

}
