package com.slemma.jdbc.query;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.MongoField;
import com.slemma.jdbc.MongoFieldPredictor;
import com.slemma.jdbc.MongoSQLException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Wrapper for mongo result
 *
 * @author Igor Shestakov.
 */
public abstract class MongoAbstractResult implements MongoResult
{
	private final int DEFAULT_BATCH_SIZE = 1000;
	protected MongoDatabase database;
	protected final Document result;
	protected final int maxRows;

	protected ArrayList<Document> documentList;
	protected ArrayList<MongoField> fields;

	public MongoAbstractResult(Document result, MongoDatabase database, int maxRows) throws MongoSQLException
	{
		this.result = result;
		this.maxRows = maxRows;
		if (this.result.containsKey("result"))
			this.documentList = (ArrayList<Document>) this.result.get("result");
		else if (this.result.containsKey("cursor"))
		{
			Document cursor = (Document) this.result.get("cursor");

			MongoNamespace namespace = new MongoNamespace((String) cursor.get("ns"));

			if (cursor.containsKey("firstBatch"))
			{
				this.documentList = (ArrayList<Document>) cursor.get("firstBatch");
			}
			else
				throw new UnsupportedOperationException("Not implemented yet. Cursors without firstBatch.");

			//receive other batches
			Long nextBatch = cursor.getLong("id");
			Boolean stopFetch = (nextBatch == null || nextBatch == 0 || this.documentList.size() >= maxRows);

			while (!stopFetch)
			{
				int nextBatchSize = this.documentList.size() + DEFAULT_BATCH_SIZE < maxRows ? DEFAULT_BATCH_SIZE : (maxRows - this.documentList.size());
				String getMoreCommandString = "{\n" +
						  "   \"getMore\": " + nextBatch + ",\n" +
						  "   \"collection\": \"" + namespace.getCollectionName() + "\",\n" +
						  "   \"batchSize\": " + nextBatchSize + "\n" +
						  "}\n";

				Document docCommand;
				try
				{
					docCommand = Document.parse(getMoreCommandString);
					Document nextBatchData = database.runCommand(docCommand);
					Document nextCursor = (Document) nextBatchData.get("cursor");
					nextBatch = nextCursor.getLong("id");
					this.documentList.addAll((ArrayList<Document>) nextCursor.get("nextBatch"));

					stopFetch = (nextBatch == null || nextBatch == 0 || this.documentList.size() >= maxRows);
				}
				catch (Exception e)
				{
					throw new MongoSQLException("Error: " + e.getMessage() + "\n Query: " + getMoreCommandString);
				}

			}
		}
		else
			this.documentList = new ArrayList<Document>(Arrays.asList(this.result));

		if (this.documentList.size() > maxRows)
			this.documentList.subList(maxRows, this.documentList.size()).clear();

		this.database = database;

		MongoFieldPredictor predictor = new MongoFieldPredictor(this.getDocumentList());
		this.fields = predictor.getFields();
	}

	public ArrayList<Document> getDocumentList()
	{
		return this.documentList;
	}

	public Object[] asArray()
	{
		return this.getDocumentList().toArray();
	}

	public int getDocumentCount()
	{
		return this.getDocumentList().size();
	}

	public int getColumnCount()
	{
		return fields.size();
	}

	public ArrayList<MongoField> getFields()
	{
		return fields;
	}

	public MongoDatabase getDatabase()
	{
		return database;
	}
}
