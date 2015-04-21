package de.hpi.hpcc.parsing;

import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLItemsListParser implements ItemsListVisitor {

	private String parsed;
	private String inColumn;
	private ECLLayouts eclLayouts;
	
	public ECLItemsListParser(ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	public String parse(ItemsList itemsList) {
		itemsList.accept(this);
		return parsed;
	}
	
	@Override
	public void visit(SubSelect subSelect) {
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("SET(");
		expressionBuilder.append(new ECLBuilderSelect(eclLayouts).generateECL(subSelect.getSelectBody().toString()));
		expressionBuilder.append(","+inColumn+")");
		
		parsed = expressionBuilder.toString();
	}

	@Override
	public void visit(ExpressionList expressionList) {
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("[");
		String expressions = "";
		for (Expression expression : expressionList.getExpressions()) {
			expressions += (expressions.equals("")?"":", ");
			ECLExpressionParser expressionParser = new ECLExpressionParser(eclLayouts);
			expressions += expressionParser.parse(expression);
		}
		expressionBuilder.append(expressionList).append("]");
		
	}

	@Override
	public void visit(MultiExpressionList multiExprList) {
		// TODO Auto-generated method stub
		
	}

	public void setInColumn(String inColumn) {
		this.inColumn = inColumn;
	}

}