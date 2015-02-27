package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParser{
	
	CCJSqlParserManager parserManager = new CCJSqlParserManager();
	Statement statement;
	Expression expression;
	private PlainSelect plain;

	protected SQLParser() {
	}
	
	protected SQLParser(Expression expression) {
		if (expression instanceof SubSelect) {
			statement = (Statement) expression;
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			this.expression = expression;			
		} 
	}
	
	protected SQLParser(String sql) {
		try {
			statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Select) {
				plain = (PlainSelect) ((Select) statement).getSelectBody();
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected SQLParser (Statement statement) {
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
					allSelects.addAll(new SQLParser(((SubSelect) getFromItem()).toString()).getAllSelectItemsInQuery());
				}	
			}		
		}
		return allSelects;
	}
	
	protected TreeSet<String> getAllColumns() {
		FromItem fromItem = getFromItem();
		String table;
		if (fromItem instanceof Table) {
			table = ((Table) fromItem).getName();
			return ECLLayouts.getAllColumns(table);
		} else {
			return new SQLParser((SubSelect) fromItem).getAllColumns();
		}
		
	}
	
	protected List<String> getAllTables() {
		List<String> tableList;
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		if (statement instanceof Select) {
			tableList = tablesNamesFinder.getTableList((Select) statement);
		} else if (statement instanceof Insert) {
			tableList = tablesNamesFinder.getTableList((Insert) statement);
		} else if (statement instanceof Update) {
			tableList = tablesNamesFinder.getTableList((Update) statement);
		} else {
			tableList = null;
		}
		List<String> lowerTableList = new ArrayList<String>();
		for (String table : tableList) {
			lowerTableList.add(table.toLowerCase());
		}
		return lowerTableList;
	}
	
	protected Limit getLimit() {
		if (plain == null) return null;
		return plain.getLimit();
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
	
	protected Expression getHaving() {
		if (plain == null) return null;
		return plain.getHaving();
	}
	
	protected HashSet<String> concatenateSelectsOrderBysHaving() {
		
		return null;
	}
}
