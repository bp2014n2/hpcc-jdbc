package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

	public SQLParser() {
	}
	
	public SQLParser(Expression expression) {
		if (expression instanceof SubSelect) {
			statement = (Statement) expression;
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			this.expression = expression;			
		} 
	}
	
	public SQLParser(String sql) {
		try {
			statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Select) {
				plain = (PlainSelect) ((Select) statement).getSelectBody();
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	public SQLParser (Statement statement) {
		this.statement = statement;
		if (statement instanceof Select) {
			plain = (PlainSelect) ((Select) statement).getSelectBody();
		}
	}
	
	public FromItem getTable() {
		if (plain == null) return null;
		return plain.getFromItem();
	}
	
	public List<SelectItem> getSelectItems() {
		if (plain == null) return null;
		return plain.getSelectItems();
	}
	
	public List<String> getAllSelectItems() {
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
				if (getTable() instanceof Table) {
					String tableName = ((Table) getTable()).getName();
					String layout = ECLBuilder.getLayouts().get(tableName);
					String[] layout_splitted = layout.split(";");
					for (String l : layout_splitted) {
						String[] foo = l.split(" ");
						allSelects.add(foo[foo.length-1]);
					}
				} else if (getTable() instanceof SubSelect) {
					allSelects.addAll(new SQLParser(((SubSelect) getTable()).toString()).getAllSelectItems());
				}
				
			}
			
		}
		return allSelects;
	}
	
	public List<String> getAllTables() {
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
	
	public Expression getWhere() {
		if (plain == null) return null;
		return plain.getWhere();
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
	
	protected Boolean isSelectAll() {
    	for (SelectItem selectItem : getSelectItems()) {
    		if(selectItem instanceof AllColumns) return true;
    	}
    	return false;
	}
}
