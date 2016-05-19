package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
import com.slemma.jdbc.MongoFieldPredictor;
import org.bson.Document;

import java.util.*;

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

	private Object GetFieldValueFromDocument(Document doc, List<String> path)
	{
		List<String> pathClone = new LinkedList<String>(path);
		if (pathClone.size() > 1)
		{
			Document docProp = (Document) doc.get(pathClone.get(0));
			pathClone.remove(0);
			return GetFieldValueFromDocument(docProp, pathClone);
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
				throw new UnsupportedOperationException("Not implemented yet. Cursors without firstBatch.");
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
				sourceKeyFieldPath = Arrays.asList(this.query.getSourceKeyField().split(MongoField.FIELD_LVL_DELIMETER_REGEX_FOR_SPLIT));
			}
			transformedDoc.append(this.query.getResultKeyField(), GetFieldValueFromDocument(sourceDoc, sourceKeyFieldPath));

			//name field
			List<String> sourceNameFieldPath;
			if (this.query.getSourceNameField().indexOf(MongoField.FIELD_LVL_DELIMETER) == -1) {
				sourceNameFieldPath = new ArrayList<>();
				sourceNameFieldPath.add(this.query.getSourceNameField());
			}
			else	{
				sourceNameFieldPath = Arrays.asList(this.query.getSourceNameField().split(MongoField.FIELD_LVL_DELIMETER_REGEX_FOR_SPLIT));
			}
			transformedDoc.append(this.query.getResultNameField(), GetFieldValueFromDocument(sourceDoc, sourceNameFieldPath));

			this.documentList.add(transformedDoc);
		}
		this.database = database;
		if (this.getDocumentCount() > 0) {
			MongoFieldPredictor predictor = new MongoFieldPredictor(this.getDocumentList());
			fields = predictor.getFields();
		} else {
			fields = new ArrayList<>();
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
