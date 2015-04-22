package de.hpi.hpcc.parsing.select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLNameParser;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;

public class SQLParserSelect extends SQLParser {

	private PlainSelect plain;

	public SQLParserSelect(SelectBody expression, ECLLayouts layouts) {
		super(null, layouts);
		try {
			statement = SQLParser.parse(expression.toString());
		} catch (HPCCException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		plain = (PlainSelect) expression;
	}
	
	public SQLParserSelect(Select statement, ECLLayouts layouts) {
		super(statement, layouts);
		plain = (PlainSelect) statement.getSelectBody();
	}
	/*
	public SQLParserSelect (Statement statement) {
		super(statement);
		this.statement = statement;
		if (statement instanceof Select) {
			plain = (PlainSelect) ((Select) statement).getSelectBody();
		}
	}
		*/
	public FromItem getFromItem() {
		if (plain == null) return null;
		return plain.getFromItem();
	}
	
	public List<SelectItem> getSelectItems() {
		if (plain == null) return null;
		return plain.getSelectItems();
	}
	
	public List<String> getAllSelectItemsInQuery() {
		ArrayList<String> allSelects = new ArrayList<String>();
		/*
		if (isCount()) {
			//TODO: better
			SelectItem count = getSelectItems().get(0);
			String columnName = "count";
			Alias alias = ((SelectExpressionItem) count).getAlias();
			if (alias != null) {
				columnName = alias.getName();
			}
			allSelects.add(columnName);
			return allSelects;
		}*/
		for (SelectItem selectItem : getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				if (((SelectExpressionItem) selectItem).getAlias() != null) {
					allSelects.add(((SelectExpressionItem) selectItem).getAlias().getName());
				} else {
					Expression expression = ((SelectExpressionItem) selectItem).getExpression();
					ECLNameParser nameParser = new ECLNameParser();
					String name = nameParser.name(expression);
					allSelects.add(name);
				}
			}
			else if (selectItem instanceof AllColumns) {
				if (getFromItem() instanceof Table) {
					String tableName = ((Table) getFromItem()).getName();
					allSelects.addAll(eclLayouts.getAllColumns(tableName));
				} else if (getFromItem() instanceof SubSelect) {
					allSelects.addAll(new SQLParserSelect(((SubSelect) getFromItem()).getSelectBody(), eclLayouts).getAllSelectItemsInQuery());
				}	
			}		
		}
		return allSelects;
	}
	
	public LinkedHashSet<String> getAllColumns() {
		FromItem fromItem = getFromItem();
		String table;
		if (fromItem instanceof Table) {
			table = ((Table) fromItem).getName();
			try {
				return eclLayouts.getAllColumns(table);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		} else {
			return new SQLParserSelect(((SubSelect) fromItem).getSelectBody(), eclLayouts).getAllColumns();
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
			List<SelectItem> selectItems = new SQLParserSelect(((SubSelect) plain.getFromItem()).getSelectBody(), eclLayouts).getSelectItems();
			List<String> selectItemStrings = new ArrayList<String>();
			for (SelectItem selectItem : selectItems) {
				selectItemStrings.add(selectItem.toString());
			}
			return selectItemStrings;
		}
		return null;
	}

	@Override
	public List<String> getQueriedColumns(String table) {
		List<String> columns = new ArrayList<String>();
		for (SelectItem selectItem : getSelectItems()) {
			columns.addAll(findColumns(getTableNameAndAlias(table),((SelectExpressionItem) selectItem).getExpression()));
		}
		if (plain.getWhere() != null) {
			columns.addAll(findColumns(getTableNameAndAlias(table), plain.getWhere()));
		}
		List<WithItem> withItems = null;
		if((withItems = ((Select) statement).getWithItemsList()) != null) {
			for(WithItem with : withItems) {
				SQLParserSelect subParser = new SQLParserSelect(with.getSelectBody(), eclLayouts);
				columns.addAll(subParser.getQueriedColumns(table));
			}
		}
		
		return columns;
	}
}
