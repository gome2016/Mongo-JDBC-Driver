package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
import org.bson.Document;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Wrapper for mongo result
 * @author Igor Shestakov.
 */
public class MongoCountMembersResult implements MongoResult
{
	private MongoDatabase database;
	private final Document result;
	private CountMembersMixedQuery query;

	private ArrayList<Document> documentList;
	private final ArrayList<MongoField> fields;

	public MongoCountMembersResult(final CountMembersMixedQuery query, Document result, MongoDatabase database)
	{
		this.query = query;
		this.result = result;
		this.database = database;
		fields = new ArrayList<>();
		fields.add(new MongoField(Types.BIGINT, Long.class, new ArrayList<String>(){{add(query.getResultFieldName());}}));

		//create fake document
		if (this.result.containsKey("result")) {
			Document fakeDoc = new Document(
					  query.getResultFieldName()
					  , ((ArrayList)this.result.get("result")).size()
			);
			this.documentList = new ArrayList<Document>();
			this.documentList.add(fakeDoc);
		}
		else if (this.result.containsKey("cursor")) {
			Document cursor = (Document)this.result.get("cursor");
			if (cursor.containsKey("firstBatch")){
				Document fakeDoc = new Document(
						  query.getResultFieldName()
						  , ((ArrayList)cursor.get("firstBatch")).size()
				);
				this.documentList = new ArrayList<Document>();
				this.documentList.add(fakeDoc);
			}
			else
				throw new UnsupportedOperationException("Not implemented yet. Cursors without firstBatch.");
		}
		else
			this.documentList = new ArrayList<Document>(Arrays.asList(this.result));
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
