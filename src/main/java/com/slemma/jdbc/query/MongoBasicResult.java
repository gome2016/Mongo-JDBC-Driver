package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
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

	private void sampleMetadata(Document sampleDocument, ArrayList<String> levelPath)
	{
		for (Map.Entry<String, Object> entry : sampleDocument.entrySet())
		{
			ArrayList<String> path = (ArrayList<String>) levelPath.clone();
			path.add(entry.getKey());
			if (entry.getValue().getClass() == Document.class) {
				sampleMetadata((Document)entry.getValue(),path);
			}
			else {
				if (ConversionHelper.sqlTypeExists(entry.getValue().getClass()))
					fields.add(new MongoField(ConversionHelper.lookup(entry.getValue().getClass()), entry.getValue().getClass(), path));
			}
		}
	}

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
				throw new UnsupportedOperationException("Not implemented yet");
		}
		else
			this.documentList = new ArrayList<Document>(Arrays.asList(this.result));


		this.database = database;
		fields = new ArrayList<>();
		if (this.getDocumentCount()>0) {
			Document sampleDoc = this.getDocumentList().get(0);
			sampleMetadata(sampleDoc, new ArrayList<String>());
		}
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
