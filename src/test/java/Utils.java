import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Igor Shestakov.
 */
public class Utils
{
	public static String AdjustString(String str ,int symbolsCount)
	{
		if (str == null)
			str = "null";

		if (str.length()>symbolsCount){
			return str.substring(0,symbolsCount);
		}
		else {
			char[] chars = new char[symbolsCount-str.length()];
			Arrays.fill(chars, ' ');
			String emptyText = new String(chars);

			return str + emptyText;
		}
	}

	public static void printResultSet(ResultSet rs){

		final int columnWidth = 10;

		ArrayList<String> columnNames = new ArrayList<>();
		try
		{
			//HEADER
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
			{
				columnNames.add(rs.getMetaData().getColumnName(i));
				System.out.print(AdjustString(columnNames.get(i - 1), columnWidth) + " | ");
			}

			System.out.print("\n");
			char[] chars = new char[(columnWidth+3)*columnNames.size()];
			Arrays.fill(chars, '-');
			System.out.println(new String(chars));

			//BODY
			while (rs.next())
			{
				String rowStr = "";
				for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
				{
					rowStr += AdjustString(rs.getString(columnNames.get(i)), columnWidth) + " | ";
				}
				System.out.println(rowStr);
			}

		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

}
