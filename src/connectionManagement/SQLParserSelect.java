package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
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

	protected SQLParserSelect(Expression expression) {
		super(expression);
		if (expression instanceof SubSelect) {
			statement = (Statement) expression;
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			this.expression = expression;			
		} 
	}
	
	protected SQLParserSelect(String sql) {
		super(sql);
		try {
			statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Select) {
				plain = (PlainSelect) ((Select) statement).getSelectBody();
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected SQLParserSelect (Statement statement) {
		super(statement);
		this.statement = statement;
		if (statement instanceof Select) {
			plain = (PlainSelect) ((Select) statement).getSelectBody();
		}
	}
		
	protected FromItem getFromItem() {
		if (plain == null) return null;
		return plain.getFromItem();
	}
	
	protected List<SelectItem> getSelectItems() {
		if (plain == null) return null;
		return plain.getSelectItems();
	}
	
	protected List<String> getAllSelectItemsInQuery() {
		ArrayList<String> allSelects = new ArrayList<String>();
		for (SelectItem selectItem : getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) selectItem).getExpression();
				if (expression instanceof Column) {
					allSelects.add(expression.toString());
				} else if (expression instanceof Function) {
					String function = expression.toString().replace("(", "_").replace(")", "").toLowerCase();
					allSelects.add(function);
				}
			}
			else if (selectItem instanceof AllColumns) {
				if (getFromItem() instanceof Table) {
					String tableName = ((Table) getFromItem()).getName();
					allSelects.addAll(ECLLayouts.getAllColumns(tableName));
				} else if (getFromItem() instanceof SubSelect) {
					allSelects.addAll(new SQLParserSelect(((SubSelect) getFromItem()).toString()).getAllSelectItemsInQuery());
				}	
			}		
		}
		return allSelects;
	}
	
	protected LinkedHashSet<String> getAllColumns() {
		FromItem fromItem = getFromItem();
		String table;
		if (fromItem instanceof Table) {
			table = ((Table) fromItem).getName();
			return ECLLayouts.getAllColumns(table);
		} else {
			return new SQLParserSelect((SubSelect) fromItem).getAllColumns();
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
		List<SelectItem> selectItems = new SQLParserSelect(trimInnerStatement(plain.getFromItem().toString())).getSelectItems();
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
		return (getSelectItems().size() == 1 && getSelectItems().get(0).toString().toLowerCase().contains("count"));
	}
}
