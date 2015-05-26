package de.hpi.hpcc.parsing.select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SQLParserPlainSelect extends SQLParserSelect {

	private PlainSelect plain;

	public SQLParserPlainSelect(PlainSelect plainSelect, ECLLayouts layouts) {
		super(null, layouts);
		this.select = new Select();
		this.select.setSelectBody(plainSelect);
		plain = plainSelect;
	}
	
	public FromItem getFromItem() {
		if (plain == null) return null;
		return plain.getFromItem();
	}
	
	public List<SelectItem> getSelectItems() {
		if (plain == null) return null;
		return plain.getSelectItems();
	}
	
	public LinkedHashSet<String> getAllColumns() {
		FromItem fromItem = getFromItem();
		String table;
		if (fromItem instanceof Table) {
			table = ((Table) fromItem).getName();
			return eclLayouts.getAllColumns(table);
		} else {
			SQLParserSelectVisitor visitor = new SQLParserSelectVisitor(eclLayouts);
			return visitor.find(((SubSelect) fromItem).getSelectBody()).getAllColumns();
		}
	}
	
	public Expression getWhere() {
		if (plain == null) return null;
		return plain.getWhere();
	}
	
	public void setWhere(Expression expression) {
		plain.setWhere(expression);
	}
	
	public List<Expression> getGroupBys() {
		if (plain == null) return null;
		return plain.getGroupByColumnReferences();
	}
	
	public List<OrderByElement> getOrderBys() {
		if (plain == null) return null;
		return plain.getOrderByElements();
	}
	
	public List<Join> getJoins() {
		if (plain == null) return null;
		return plain.getJoins();
	}
	
	public Boolean isDistinct() {
		if (plain == null || plain.getDistinct() == null) return false;
		return true;
	}
	
	public Boolean isSelectAll() {
    	for (SelectItem selectItem : getSelectItems()) {
    		if(selectItem instanceof AllColumns) return true;
    	}
    	return false;
	}
	
	public Limit getLimit() {
		if (plain == null) return null;
		return plain.getLimit();
	}
	
	public Expression getHaving() {
		if (plain == null) return null;
		return plain.getHaving();
	}
	
	public HashSet<String> concatenateSelectsOrderBysHaving() {
		return null;
	}

	public List<String> getFromItemColumns() {
		if (plain == null) return null;
		if (plain.getFromItem() instanceof SubSelect) {
			SQLParserSelectVisitor visitor = new SQLParserSelectVisitor(eclLayouts);
			List<SelectItem> selectItems = visitor.find(((SubSelect) plain.getFromItem()).getSelectBody()).getSelectItems();
			List<String> selectItemStrings = new ArrayList<String>();
			for (SelectItem selectItem : selectItems) {
				selectItemStrings.add(selectItem.toString());
			}
			return selectItemStrings;
		}
		return null;
	}

	@Override
	public Statement getStatement() {
		return select;
	}
}
