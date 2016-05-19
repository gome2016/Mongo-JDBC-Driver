package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
import com.slemma.jdbc.MongoFieldPredictor;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Wrapper for mongo result
 * @author Igor Shestakov.
 */
public class MongoBasicResult implements MongoResult
{
	private MongoDatabase database;
	private final Document result;

	private ArrayList<Document> documentList;
	private final ArrayList<MongoField> fields;

	public MongoBasicResult(Document result, MongoDatabase database)
	{
		this.result = result;
		if (this.result.containsKey("result"))
			this.documentList = (ArrayList<Document>) this.result.get("result");
		else if (this.result.containsKey("cursor")) {
			Document cursor = (Document)this.result.get("cursor");
			if (cursor.containsKey("firstBatch")){
				this.documentList = (ArrayList<Document>) cursor.get("firstBatch");
			}
			else
				throw new UnsupportedOperationException("Not implemented yet. Cursors without firstBatch.");
		}
		else
			this.documentList = new ArrayList<Document>(Arrays.asList(this.result));

		this.database = database;
		MongoFieldPredictor predictor = new MongoFieldPredictor(this.getDocumentList());
		this.fields = predictor.getFields();
	}

	public ArrayList<Document> getDocumentList(){
		return this.documentList;
	}

	public Object[] asArray(){
		return this.getDocumentList().toArray();
	}

	public int getDocumentCount(){
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
