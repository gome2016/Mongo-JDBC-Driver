package com.slemma.jdbc;

import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Field metadata predictor (predict result data types)
 *
 * @author Igor Shestakov.
 */
public class MongoFieldPredictor
{
	private final ArrayList<MongoField> fields = new ArrayList<>();

	public static final int SAMPLING_BATCH_SIZE = 10;

	public MongoFieldPredictor(ArrayList<Document> documents)
	{
		if (documents == null)
			throw new IllegalArgumentException("Documents list must be not null");

		if (documents.size() > 0)
		{
			Document doc = documents.get(0);
			//init fields
			Map<String, MongoField> fM = new LinkedHashMap<String, MongoField>();
			sampleMetadata(doc, fM, new ArrayList<String>());

			int cntMax = (this.SAMPLING_BATCH_SIZE <= documents.size()) ? this.SAMPLING_BATCH_SIZE : documents.size();
			//correction
			for (int i = 1; i < cntMax; i++)
			{
				doc = documents.get(i);
				Map<String, MongoField> fMC = new LinkedHashMap<String, MongoField>();
				sampleMetadata(doc, fMC, new ArrayList<String>());
				for (String fieldId : fM.keySet())
				{
					MongoField field = fM.get(fieldId);
					MongoField fieldC = fMC.get(fieldId);
					if (field != null && fieldC !=null
							  && field.getType() != fieldC.getType()
							  && ConversionHelper.isSecondTypeMoreUniversality(field.getType(), fieldC.getType()))
					{
						field.setType(fieldC.getType());
					}
				}
			}

			for (String fieldId : fM.keySet())
			{
				fields.add(fM.get(fieldId));
			}
		}
	}

	public ArrayList<MongoField> getFields()
	{
		return fields;
	}

	private void sampleMetadata(Document sampleDocument, Map<String, MongoField> fieldsMap, ArrayList<String> levelPath)
	{
		for (Map.Entry<String, Object> entry : sampleDocument.entrySet())
		{
			ArrayList<String> path = (ArrayList<String>) levelPath.clone();
			path.add(entry.getKey());
			if (entry.getValue() != null) {
				if (entry.getValue().getClass() == Document.class)
				{
					sampleMetadata((Document) entry.getValue(), fieldsMap, path);
				}
				else
				{
					if (ConversionHelper.sqlTypeExists(entry.getValue().getClass())){
						int dataType = ConversionHelper.lookup(entry.getValue().getClass());
						MongoField field = new MongoField(dataType, entry.getValue().getClass(), path);
						if (ConversionHelper.sqlTypeExists(entry.getValue().getClass()))
							fieldsMap.put(field.getName(), field);
					}
				}
			} //TODO: implement else
		}
	}
}
