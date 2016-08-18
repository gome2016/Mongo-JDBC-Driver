import com.slemma.jdbc.MongoConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author igorshestakov.
 */
public class TestPredictor
{
	private static java.sql.Connection con = null;
	private final static Logger logger = LoggerFactory.getLogger(TestResultSet.class.getName());

	@Before
	public void checkConnection()
	{
		try
		{
			if (this.con == null || !this.con.isValid(0))
			{
				this.logger.info("Testing the JDBC driver");
				try
				{
					Class.forName("com.slemma.jdbc.MongoDriver");
//					this.con = DriverManager.getConnection("jdbc:mongodb:mql://192.168.99.100:27017/test");
					this.con = DriverManager.getConnection("jdbc:mongodb:mql://test:test@127.0.0.1:27017/test?&authMechanism=SCRAM-SHA-1");
				}
				catch (Exception e)
				{
					e.printStackTrace();
					this.logger.error("Error in connection" + e.toString());
					Assert.fail("Exception:" + e.toString());
				}
				this.logger.info(((MongoConnection) this.con).getUrl());
			}
		}
		catch (SQLException e)
		{
			logger.debug("Oops something went wrong", e);
		}
	}

	@Test
	public void testFieldsPredictionWithNullsInFirstRows()
	{
		ResultSet rs;
		String query = "{ \"find\" : \"bios2\"}";
		try
		{
			Statement stmt = this.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(query);
			Assert.assertNotNull(rs);

			ResultSetMetaData rsMetadata = rs.getMetaData();
			Assert.assertEquals(11, rsMetadata.getColumnCount());

			Utils.printResultSet(rs);
		}
		catch (SQLException e)
		{
			this.logger.error("Exception: " + e.toString());
			Assert.fail("Exception: " + e.toString());
		}
		Assert.assertTrue(true);
	}
}
