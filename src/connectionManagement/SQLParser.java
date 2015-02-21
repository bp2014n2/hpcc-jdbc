package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
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
	
	public List<String> extractAllTables() {
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
	
	
	
	
	
	
	
	
	
	
	/*
	private <T> List<String> getColumns (List<T> elements) {
		if (elements == null) return (List<String>) new ArrayList<String> ();
		
		List<String> result = (List<String>) new ArrayList<String>();
		for (T element : elements) {
			try {
//				mega-hyper-krasse Meta-Programmierung
//				mega-hyper-crass meta-programming
				java.lang.reflect.Method method = element.getClass().getMethod("getExpression", null);
				Column c = (Column) method.invoke(element, null);
				result.add(transformColumnToString(c));
				
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return result;
	}
	*/
	
	
	
	/*
	public List<String> getSelects() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
		if (plain.getSelectItems().get(0) instanceof AllColumns) {
			return (List<String>) new ArrayList<String>();
		}
		
		/* only working with Java 8
		List<String> selectItems = plain.getSelectItems()
				.stream()
				.map(s -> transformSelectItemToString(s))
				.collect(Collectors.toList()); */
//		code for Java 7
	/*	
		return getColumns(plain.getSelectItems());
	}
	
	public String transformColumnToString(Column c) {
		String string = "";
		if (c.getTable().getName() != null) {
			string += c.getTable();
			string += ".";
		}
		string += c.getColumnName();
		return string;
	}
	
	*/

}
