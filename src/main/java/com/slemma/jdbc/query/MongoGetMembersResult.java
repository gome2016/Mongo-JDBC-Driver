package com.slemma.jdbc.query;

import com.mongodb.client.MongoDatabase;
import com.slemma.jdbc.ConversionHelper;
import com.slemma.jdbc.MongoField;
import com.slemma.jdbc.MongoFieldPredictor;
import com.slemma.jdbc.MongoSQLException;
import org.bson.Document;

import java.util.*;

/**
 * Wrapper for mongo result
 *
 * @author Igor Shestakov.
 */
public class MongoGetMembersResult extends MongoAbstractResult implements MongoResult
{
	private GetMembersMixedQuery query;

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

	public MongoGetMembersResult(final GetMembersMixedQuery query, Document result, MongoDatabase database, int maxRows) throws MongoSQLException
	{
		super(result, database, maxRows);
		this.query = query;

		ArrayList<Document> sourceDocumentList = this.documentList;

		//create transformed DocumentList
		this.documentList = new ArrayList<Document>();
		ArrayList<Integer> documentHashList = new ArrayList<>();
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

			if (documentHashList.indexOf(transformedDoc.hashCode()) == -1) {
				documentHashList.add(transformedDoc.hashCode());
				this.documentList.add(transformedDoc);
			}
		}

		if (this.getDocumentCount() > 0) {
			MongoFieldPredictor predictor = new MongoFieldPredictor(this.getDocumentList());
			this.fields = predictor.getFields();
		} else {
			this.fields = new ArrayList<>();
		}
	}
}
