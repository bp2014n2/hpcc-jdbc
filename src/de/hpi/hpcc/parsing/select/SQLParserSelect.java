package de.hpi.hpcc.parsing.select;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
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

	public SQLParserSelect(Expression expression, ECLLayouts layouts) {
		super(expression, layouts);
		if (expression instanceof SubSelect) {
			statement = (Statement) expression;
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			this.expression = expression;			
		} 
	}
	
	public SQLParserSelect(String sql, ECLLayouts layouts) {
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
		if (isCount()) {	
			allSelects.add(((SelectExpressionItem) getSelectItems().get(0)).getAlias().getName());
			return allSelects;
		}
		for (SelectItem selectItem : getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				if (((SelectExpressionItem) selectItem).getAlias() != null) {
					allSelects.add(((SelectExpressionItem) selectItem).getAlias().getName());
				} else {
					Expression expression = ((SelectExpressionItem) selectItem).getExpression();
					if (expression instanceof Column) {
						allSelects.add(expression.toString());
					} else if (expression instanceof Function) {
						String function = expression.toString().replace("(", "_").replace(")", "").toLowerCase();
						allSelects.add(function);
					}
				}
			}
			else if (selectItem instanceof AllColumns) {
				if (getFromItem() instanceof Table) {
					String tableName = ((Table) getFromItem()).getName();
					allSelects.addAll(eclLayouts.getAllColumns(tableName));
				} else if (getFromItem() instanceof SubSelect) {
					allSelects.addAll(new SQLParserSelect(((SubSelect) getFromItem()).toString(), eclLayouts).getAllSelectItemsInQuery());
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
			return new SQLParserSelect((SubSelect) fromItem, eclLayouts).getAllColumns();
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
		List<SelectItem> selectItems = new SQLParserSelect(trimInnerStatement(plain.getFromItem().toString()), eclLayouts).getSelectItems();
		List<String> selectItemStrings = new ArrayList<String>();
		for (SelectItem selectItem : selectItems) {
			selectItemStrings.add(selectItem.toString());
		}
		return selectItemStrings;
	}	
	
	public String trimInnerStatement(String innerStatement) {
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
