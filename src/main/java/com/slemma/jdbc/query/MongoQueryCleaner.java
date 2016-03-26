package com.slemma.jdbc.query;

/**
 * @author Igor Shestakov.
 */
public class MongoQueryCleaner
{
	protected String cleans(String queryString, int startIndex)
	{
		int openingDoubleQuoteIndex = queryString.indexOf('"', startIndex);
		int openingSingleQuoteIndex = queryString.indexOf('\'', startIndex);

//		val openingQuote = quotePosition(openingDoubleQuoteIndex, openingSingleQuoteIndex)
//		val openingQuoteIndex = openingQuote._1
//		val char = openingQuote._2
//
//		if (openingQuoteIndex != -1) {
//			val closingQuoteIndex = mongoQuery.indexOf(char, openingQuoteIndex + 1)
//			if (closingQuoteIndex != -1) {
//				val pureString = mongoQuery.substring(startIndex, openingQuoteIndex)
//				val quotedString = mongoQuery.substring(openingQuoteIndex, closingQuoteIndex + 1)
//
//				val formattedString: String = pureString.replaceAll(" ", "") + quotedString + cleans(mongoQuery, closingQuoteIndex + 1)
//				formattedString
//			}
//			else {
//				mongoQuery.substring(startIndex, mongoQuery.length)
//			}
//		}
//		else {
//			mongoQuery.substring(startIndex, mongoQuery.length).replaceAll(" *", "")
//		}

		return queryString;
	}

//	protected def quotePosition(openingDoubleQuoteIndex: Int, openingSingleQuoteIndex: Int): (Int, Char) = {
//	val quotePos = (openingDoubleQuoteIndex, openingSingleQuoteIndex) match {
//		case (-1, -1) => (-1, '\'')
//		case (-1, s) => (s, '\'')
//		case (d, -1) => (d, '"')
//		case (d, s) => if (d < s) (d, '"') else (s, '\'')
//	}
//	quotePos
//	}

	public String clean(String queryString)
	{
		String withoutNewLine = queryString.replaceAll("\n", "").trim();
		return cleans(withoutNewLine, 0);
	}
}
