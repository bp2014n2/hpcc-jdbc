package de.hpi.hpcc.parsing.visitor;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class ECLWhereExpressionFinder extends FullVisitorAdapter {
	
	private Expression where = null;
	private FromItem parentTable;
	
	public Expression find(ItemsList itemsList, FromItem table) {
		this.parentTable = table;
		itemsList.accept(this);
		return where;
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		FromItem fromItem = plainSelect.getFromItem();
		//Philipp approved this solution
		if (parentTable instanceof Table && fromItem instanceof Table) {
			Table table = (Table) parentTable;
			Table fromItemTable = (Table) fromItem;
			if (table.getName().equalsIgnoreCase(fromItemTable.getName())) {
				where = plainSelect.getWhere();
			}
		}
		
		
	}
	
	@Override
	public void visit(ExpressionList expressionList) {
		
	}

	@Override
	public void visit(MultiExpressionList multiExprList) {
		
	}
}
