package de.hpi.hpcc.parsing;

import net.sf.jsqlparser.expression.Expression;

abstract public class ECLBuilder {
	protected ECLLayouts eclLayouts;
	protected StringBuilder eclCode;
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate subclass, 
	 * depending on the type of the given SQL (e.g. Select, Insert, Update, Drop or Create) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	
	public ECLBuilder(ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	/**
	 * This methods surrounds a given ECL string with a Table definition. 
	 * The input StringBuilder is modified.
	 * @param eclCode
	 */
	abstract protected SQLParser getSqlParser();
	
	/**
	 * Generates for a given Expression the ECL code by a recursive approach
	 * Each Expression can be e.g. a BinaryExpression (And, or etc. ), but also a Column or a SubSelect.
	 * The break condition is accomplished if the Expression is a Column, Long or String
	 * @param expressionItem is the Expression under consideration
	 * @return returns the ECL code for the given expression
	 */
	protected String parseExpressionECL(Expression expressionItem) {
		StringBuilder expression = new StringBuilder();
		ECLExpressionParser expressionParser = new ECLExpressionParser(eclLayouts);
		expressionParser.setAllTables(getSqlParser().getAllTables());
		expression.append(expressionParser.parse(expressionItem));
		 
		return expression.toString();
	}
	
}
