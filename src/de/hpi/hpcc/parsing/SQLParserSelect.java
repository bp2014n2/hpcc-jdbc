package de.hpi.hpcc.parsing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SQLParserSelect extends SQLParser {

	private PlainSelect plain;

	protected SQLParserSelect(Expression expression, ECLLayouts layouts) {
		super(expression, layouts);
		if (expression instanceof SubSelect) {
			statement = (Statement) expression;
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			this.expression = expression;			
		} 
	}
	
	protected SQLParserSelect(String sql, ECLLayouts layouts) {
		super(sql, layouts);
		try {
			statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Select) {
				plain = (PlainSelect) ((Select) statement).getSelectBody();
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	/*
	protected SQLParserSelect (Statement statement) {
		super(statement);
		this.statement = statement;
		if (statement instanceof Select) {
			plain = (PlainSelect) ((Select) statement).getSelectBody();
		}
	}
		*/
	protected FromItem getFromItem() {
		if (plain == null) return null;
		return plain.getFromItem();
	}
	
	protected List<SelectItem> getSelectItems() {
		if (plain == null) return null;
		return plain.getSelectItems();
	}
	
	protected HashMap<String,List<String>> getAllSelectItemsInQuery() {
		HashMap<String, List<String>> allSelects = new HashMap<String, List<String>>();
		if (isCount()) {	
			List<String> columns = new ArrayList<String>();
			columns.add(((SelectExpressionItem) getSelectItems().get(0)).getAlias().getName());
			allSelects.put("default", columns);
			return allSelects;
		}
		List<String> columns = new ArrayList<String>();
		for (SelectItem selectItem : getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				if (((SelectExpressionItem) selectItem).getAlias() != null) {
					
					columns.add(((SelectExpressionItem) selectItem).getAlias().getName());
					
				} else {
					Expression expression = ((SelectExpressionItem) selectItem).getExpression();
					if (expression instanceof Column) {
						columns.add(expression.toString());
					} else if (expression instanceof Function) {
						String function = expression.toString().replace("(", "_").replace(")", "").toLowerCase();
						columns.add(function);
					}
				}
			}
			else if (selectItem instanceof AllColumns) {
				if (getFromItem() instanceof Table) {
					String tableName = ((Table) getFromItem()).getName();
					allSelects.put(tableName, null);
				} else if (getFromItem() instanceof SubSelect) {
					HashMap<String,List<String>> recursive = new SQLParserSelect(((SubSelect) getFromItem()).toString(), eclLayouts).getAllSelectItemsInQuery();
					for (Entry<String, List<String>> entry : recursive.entrySet()) {
						List<String> oldColumns = allSelects.get(entry.getKey());
						if (oldColumns == null) {
							oldColumns = new ArrayList<String>();
						} 
						oldColumns.addAll(entry.getValue());
						allSelects.put(entry.getKey(), oldColumns);
					}
				}	
				return allSelects;
			}		
		}
		allSelects.put("default", columns);
		return allSelects;
	}
	
	protected LinkedHashSet<String> getAllColumns() {
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
			return new SQLParserSelect((SubSelect) fromItem, eclLayouts).getAllColumns();
		}
		
	}
	
	protected Expression getWhere() {
		if (plain == null) return null;
		return plain.getWhere();
	}
	
	protected void setWhere(Expression expression) {
		plain.setWhere(expression);
	}
	
	protected List<Expression> getGroupBys() {
		if (plain == null) return null;
		return plain.getGroupByColumnReferences();
	}
	
	protected List<OrderByElement> getOrderBys() {
		if (plain == null) return null;
		return plain.getOrderByElements();
	}
	
	protected List<Join> getJoins() {
		if (plain == null) return null;
		return plain.getJoins();
	}
	
	protected Boolean isDistinct() {
		if (plain == null || plain.getDistinct() == null) return false;
		return true;
	}
	
	protected Boolean isSelectAll() {
    	for (SelectItem selectItem : getSelectItems()) {
    		if(selectItem instanceof AllColumns) return true;
    	}
    	return false;
	}
	
	protected Limit getLimit() {
		if (plain == null) return null;
		return plain.getLimit();
	}
	
	protected Expression getHaving() {
		if (plain == null) return null;
		return plain.getHaving();
	}
	
	protected HashSet<String> concatenateSelectsOrderBysHaving() {
		return null;
	}

	protected List<String> getFromItemColumns() {
		if (plain == null) return null;
		List<SelectItem> selectItems = new SQLParserSelect(trimInnerStatement(plain.getFromItem().toString()), eclLayouts).getSelectItems();
		List<String> selectItemStrings = new ArrayList<String>();
		for (SelectItem selectItem : selectItems) {
			selectItemStrings.add(selectItem.toString());
		}
		return selectItemStrings;
	}	
	
	protected String trimInnerStatement(String innerStatement) {
		if (innerStatement.charAt(0) == '(') {
			int end = innerStatement.lastIndexOf(")");
			innerStatement = innerStatement.substring(1, end);
		}
		return innerStatement;
	}

	public boolean isCount() {
		return (getSelectItems().size() == 1 && getSelectItems().get(0).toString().toLowerCase().contains("count") && getGroupBys() == null);
	}
}
