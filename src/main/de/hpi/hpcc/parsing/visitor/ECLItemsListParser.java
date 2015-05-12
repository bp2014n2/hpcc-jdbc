package de.hpi.hpcc.parsing.visitor;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLItemsListParser implements ItemsListVisitor {

	private String parsed;
	private String inColumn;
	private ECLLayouts eclLayouts;
	private Expression whereExpression;
	
	public ECLItemsListParser(ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	public String parse(ItemsList itemsList) {
		itemsList.accept(this);
		return parsed;
	}
	
	//just for optimization purposes
	public Expression parseWhereExpression(ItemsList itemsList) {
		itemsList.accept(this);
		return whereExpression;
	}
	
	@Override
	public void visit(SubSelect subSelect) {
		StringBuilder expressionBuilder = new StringBuilder();
		//TODO: handle Unions as other possible type for selectBodies
		PlainSelect select = (PlainSelect) subSelect.getSelectBody();
		whereExpression = select.getWhere();
		expressionBuilder.append(new ECLBuilderSelect(select, eclLayouts).generateECL());
		expressionBuilder.append(","+inColumn);
		expressionBuilder = ECLUtils.convertToSet(expressionBuilder);
		parsed = expressionBuilder.toString();
	}

	@Override
	public void visit(ExpressionList expressionList) {
		StringBuilder expressionBuilder = new StringBuilder();
		String expressions = "";
		for (Expression expression : expressionList.getExpressions()) {
			expressions += (expressions.equals("")?"":", ");
			ECLExpressionParser expressionParser = new ECLExpressionParser(eclLayouts);
			expressions += expressionParser.parse(expression);
		}
		expressionBuilder.append(expressions);
		parsed = ECLUtils.encapsulateWithSquareBrackets(expressionBuilder).toString();
		
	}

	@Override
	public void visit(MultiExpressionList multiExprList) {
		// TODO Auto-generated method stub
		
	}

	public void setInColumn(String inColumn) {
		this.inColumn = inColumn;
	}

}
