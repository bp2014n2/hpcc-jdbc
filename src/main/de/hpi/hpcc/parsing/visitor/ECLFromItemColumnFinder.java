package de.hpi.hpcc.parsing.visitor;

import java.util.List;

import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class ECLFromItemColumnFinder extends FullVisitorAdapter {
	
	public FromItem fromItem;
	private ECLLayouts layouts;
	public Expression expression;
	private boolean contains = false;

	public ECLFromItemColumnFinder(FromItem fromItem, ECLLayouts layouts) {
		this.fromItem = fromItem;
		this.layouts = layouts;
	}
	
	public boolean contains(Expression expression) {
		this.expression = expression;
		this.fromItem.accept(this);
		//this.expression = null;
		return this.contains;
	}

	@Override
	public void visit(Table table) {
		if (HPCCJDBCUtils.containsStringCaseInsensitive(layouts.getAllColumns(table.getName()), expression.toString())) {
			fromItem = table;
			contains = true;
		}
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		if(plainSelect.getSelectItems() != null) {
			ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
			List<SelectExpressionItem> list = finder.find(plainSelect);
			for(SelectExpressionItem item : list) {
				if (item.getAlias() != null && item.getAlias().getName().equalsIgnoreCase(expression.toString())) {
					expression = item.getExpression();
					fromItem = plainSelect.getFromItem();
					contains = true;
					break;
				}
				if (item.getExpression().toString().equalsIgnoreCase(expression.toString())) {
					fromItem = plainSelect.getFromItem();
					contains = true;
					break;
				}
			}
		}
	}
}
