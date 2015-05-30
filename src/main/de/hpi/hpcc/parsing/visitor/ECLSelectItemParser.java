package de.hpi.hpcc.parsing.visitor;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class ECLSelectItemParser implements SelectItemVisitor {

	private String parsed;

	public String parse(SelectItem item) {
		item.accept(this);
		return parsed;
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		
	}

}
