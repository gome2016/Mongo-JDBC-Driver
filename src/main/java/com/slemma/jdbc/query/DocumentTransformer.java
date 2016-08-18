package com.slemma.jdbc.query;

import org.bson.Document;

/**
 * @author igorshestakov.
 */
public interface DocumentTransformer
{
	Document transform(Document sourceDocument);
}
