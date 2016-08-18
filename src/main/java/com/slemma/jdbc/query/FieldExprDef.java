package com.slemma.jdbc.query;

/**
 * @author igorshestakov.
 */
public class FieldExprDef
{
	private final String Expression;
	private final String Alias;

	public FieldExprDef(String expression, String alias)
	{
		Expression = expression;
		Alias = alias;
	}

	public String getExpression()
	{
		return Expression;
	}

	public String getAlias()
	{
		return Alias;
	}
}
