package de.hpi.hpcc.parsing;

import de.hpi.hpcc.parsing.visitor.ECLExpressionParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;

abstract public class ECLBuilder {
	protected ECLLayouts eclLayouts;
	protected StringBuilder eclCode;
	
	public ECLBuilder(Statement statement, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	public abstract String generateECL();
	abstract protected Statement getStatement();
	
	/**
	 * Generates for a given Expression the ECL code by using the appropriate visitor
	 * Each Expression can be e.g. a BinaryExpression (and, or etc. ), but also a Column or a SubSelect.
	 * @param expressionItem is the Expression under consideration
	 * @return returns the ECL code for the given expression
	 */
	protected String parseExpressionECL(Expression expressionItem) {
		StringBuilder expression = new StringBuilder();
		ECLExpressionParser expressionParser = new ECLExpressionParser(eclLayouts);
		expressionParser.setAllTables(SQLParser.getAllTables(getStatement()));
		expression.append(expressionParser.parse(expressionItem));
		 
		return expression.toString();
	}
}
