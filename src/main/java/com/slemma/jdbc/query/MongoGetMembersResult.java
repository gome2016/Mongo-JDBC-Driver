package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Wrapper for mongo result
 *
 * @author Igor Shestakov.
 */
public class MongoGetMembersResult implements MongoResult
{
	private MongoDatabase database;
	private final Document result;
	private GetMembersMixedQuery query;

	private ArrayList<Document> documentList;
	private final ArrayList<MongoField> fields;

	private void sampleMetadata(Document sampleDocument, ArrayList<String> levelPath)
	{
		for (Map.Entry<String, Object> entry : sampleDocument.entrySet())
		{
			ArrayList<String> path = (ArrayList<String>) levelPath.clone();
			path.add(entry.getKey());
			if (entry.getValue().getClass() == Document.class)
			{
				sampleMetadata((Document) entry.getValue(), path);
			}
			else
			{
				if (ConversionHelper.sqlTypeExists(entry.getValue().getClass()))
					fields.add(new MongoField(ConversionHelper.lookup(entry.getValue().getClass()), entry.getValue().getClass(), path));
			}
		}
	}

	private Object GetFieldValueFromDocument(Document doc, List<String> path)
	{
		if (path.size() > 1)
		{
			Document docProp = (Document) doc.get(path.get(0));
			path.remove(0);
			return GetFieldValueFromDocument(docProp, path);
		}
		else
		{
			return doc.get(path.get(0));
		}
	}

	public MongoGetMembersResult(final GetMembersMixedQuery query, Document result, MongoDatabase database)
	{
		this.query = query;
		this.result = result;

		ArrayList<Document> sourceDocumentList;
		if (this.result.containsKey("result"))
			sourceDocumentList = (ArrayList<Document>) this.result.get("result");
		else if (this.result.containsKey("cursor"))
		{
			Document cursor = (Document) this.result.get("cursor");
			if (cursor.containsKey("firstBatch"))
			{
				sourceDocumentList = (ArrayList<Document>) cursor.get("firstBatch");
			}
			else
				throw new UnsupportedOperationException("Not implemented yet");
		}
		else
			sourceDocumentList = new ArrayList<Document>(Arrays.asList(this.result));

		//create transformed DocumentList
		this.documentList = new ArrayList<Document>();
		for (int i = 0; i < sourceDocumentList.size(); i++)
		{
			Document sourceDoc = sourceDocumentList.get(i);
			Document transformedDoc = new Document();

			//key field
			List<String> sourceKeyFieldPath;
			if (this.query.getSourceKeyField().indexOf(MongoField.FIELD_LVL_DELIMETER) == -1) {
				sourceKeyFieldPath = new ArrayList<>();
				sourceKeyFieldPath.add(this.query.getSourceKeyField());
			}
			else	{
				sourceKeyFieldPath = Arrays.asList(this.query.getSourceKeyField().split(MongoField.FIELD_LVL_DELIMETER));
			}
			transformedDoc.append(this.query.getResultKeyField(), GetFieldValueFromDocument(sourceDoc, sourceKeyFieldPath));

			//name field
			List<String> sourceNameFieldPath;
			if (this.query.getSourceNameField().indexOf(MongoField.FIELD_LVL_DELIMETER) == -1) {
				sourceNameFieldPath = new ArrayList<>();
				sourceNameFieldPath.add(this.query.getSourceNameField());
			}
			else	{
				sourceNameFieldPath = Arrays.asList(this.query.getSourceNameField().split(MongoField.FIELD_LVL_DELIMETER));
			}
			transformedDoc.append(this.query.getResultNameField(), GetFieldValueFromDocument(sourceDoc, sourceNameFieldPath));

			this.documentList.add(transformedDoc);
		}
		this.database = database;
		fields = new ArrayList<>();
		if (this.getDocumentCount() > 0)
		{
			Document sampleDoc = this.getDocumentList().get(0);
			sampleMetadata(sampleDoc, new ArrayList<String>());
		}
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
