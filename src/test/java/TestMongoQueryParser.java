import com.slemma.jdbc.MongoSQLException;
import com.slemma.jdbc.query.MongoQuery;
import com.slemma.jdbc.query.MongoQueryParser;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author igorshestakov.
 */
public class TestMongoQueryParser
{
	@Test
	public void regex1() {
		Pattern dimMembersPattern = Pattern.compile("^(?i)select\\s+(.+?)\\.(.+)\\s+as\\s+(.+)\\s*,\\s*(.+?)\\.(.+)\\s+as\\s+(.+)\\s*from\\s\\((\\{(.)*\\})\\)\\sas\\s.*\\sorder.*$",Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		String query = "select factdata_view.travelDate as c0, factdata_view.travelDate as c1 from ({\"aggregate\": \"bookings\",\n" +
				  "\n" +
				  "    \"pipeline\":\n" +
				  "    [\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                \"user.email\": {$nin: [\"brian+test@tesloop.com\",\"joseph@tesloop.com\",\"jordan@tesloop.com\",\"Brian@tesloop.com\",\"brian@tesloop.com\",\"rahul@tesloop.com\",\"joncbeckwith@gmail.com\",\"remyburnaugh@yahoo.com\",\"jenn@terosv.net\"]}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                routeId: { $nin: [\"RofeSJrjsFP7S3Hht\",\"ztz9u54tLWZbN5o2T\"] }\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                \"bookingDateTime\": { $lt: new Date()}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $project: {\n" +
				  "                bookingDateTime: 1,\n" +
				  "                travelDate: {\n" +
				  "                    \"$subtract\": [\n" +
				  "                        \"$bookingDateTime\",\n" +
				  "                        {\n" +
				  "                            \"$add\": [\n" +
				  "                                {\n" +
				  "                                    \"$millisecond\": \"$bookingDateTime\"\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$second\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$minute\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        60,\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$hour\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        60,\n" +
				  "                                        60,\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                }\n" +
				  "                            ]\n" +
				  "                        }\n" +
				  "                    ]\n" +
				  "                },\n" +
				  "                numberOfSeats: {$size: \"$seats\"}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $group: {\n" +
				  "                _id: \"$travelDate\",\n" +
				  "                travelDate: { \"$first\": \"$travelDate\" },\n" +
				  "                seats: { $sum: \"$numberOfSeats\"},\n" +
				  "                trips: { $sum: 1}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $sort: {\n" +
				  "                \"travelDate\": 1\n" +
				  "            }\n" +
				  "        }\n" +
				  "    ]\n" +
				  "}) as factdata_view group by c0, c1 order by c0 ASC";

		Matcher m = dimMembersPattern.matcher(query.replace("\n"," "));
		if (!m.matches()) {
			Assert.fail("regex1");
		} else {
			System.out.print("Match!!!");
		}

	}

	@Test
	public void parseDimMembersQuery1() {

		String query = "select factdata_view.travelDate as c0, factdata_view.travelDate as c1 from ({\"aggregate\": \"bookings\",\n" +
				  "\n" +
				  "    \"pipeline\":\n" +
				  "    [\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                \"user.email\": {$nin: [\"brian+test@tesloop.com\",\"joseph@tesloop.com\",\"jordan@tesloop.com\",\"Brian@tesloop.com\",\"brian@tesloop.com\",\"rahul@tesloop.com\",\"joncbeckwith@gmail.com\",\"remyburnaugh@yahoo.com\",\"jenn@terosv.net\"]}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                routeId: { $nin: [\"RofeSJrjsFP7S3Hht\",\"ztz9u54tLWZbN5o2T\"] }\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $match: {\n" +
				  "                \"bookingDateTime\": { $lt: new Date()}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $project: {\n" +
				  "                bookingDateTime: 1,\n" +
				  "                travelDate: {\n" +
				  "                    \"$subtract\": [\n" +
				  "                        \"$bookingDateTime\",\n" +
				  "                        {\n" +
				  "                            \"$add\": [\n" +
				  "                                {\n" +
				  "                                    \"$millisecond\": \"$bookingDateTime\"\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$second\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$minute\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        60,\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                },\n" +
				  "                                {\n" +
				  "                                    \"$multiply\": [\n" +
				  "                                        {\n" +
				  "                                            \"$hour\": \"$bookingDateTime\"\n" +
				  "                                        },\n" +
				  "                                        60,\n" +
				  "                                        60,\n" +
				  "                                        1000\n" +
				  "                                    ]\n" +
				  "                                }\n" +
				  "                            ]\n" +
				  "                        }\n" +
				  "                    ]\n" +
				  "                },\n" +
				  "                numberOfSeats: {$size: \"$seats\"}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $group: {\n" +
				  "                _id: \"$travelDate\",\n" +
				  "                travelDate: { \"$first\": \"$travelDate\" },\n" +
				  "                seats: { $sum: \"$numberOfSeats\"},\n" +
				  "                trips: { $sum: 1}\n" +
				  "            }\n" +
				  "        },\n" +
				  "        {\n" +
				  "            $sort: {\n" +
				  "                \"travelDate\": 1\n" +
				  "            }\n" +
				  "        }\n" +
				  "    ]\n" +
				  "}) as factdata_view group by c0, c1 order by c0 ASC";

		try
		{
			MongoQuery mQuery =  MongoQueryParser.parse(query);
			System.out.println(mQuery.getMqlQueryString());
		}
		catch (MongoSQLException e)
		{
			Assert.fail(e.getMessage());
		}
	}

}
