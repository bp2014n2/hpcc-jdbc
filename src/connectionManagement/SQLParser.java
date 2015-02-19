package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.Statement;

public class SQLParser{
	
	CCJSqlParserManager parserManager = new CCJSqlParserManager();
	Statement statement;
	private PlainSelect plain;

	public SQLParser() {
		
	}
	
	public SQLParser(Expression expression) {
		statement = (Statement) expression;
		if (expression instanceof SubSelect) {
			plain = (PlainSelect) ((SubSelect) expression).getSelectBody();
		}
	}
	
	public SQLParser(String sql) {
		try {
			statement = parserManager.parse(new StringReader(sql));
			System.out.println("test");
			
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
		return plain.getFromItem();
	}
	
	public List<Table> extractAllTables() {
		ArrayList<Table> allTables = new ArrayList<Table>();
		
		if (getTable() instanceof Table) {
			allTables.add((Table) getTable());
		} else if (getTable() instanceof SubSelect){
			allTables.addAll(new SQLParser((Expression) getTable()).extractAllTables());
		}
		
		if (getJoins() != null) {
			for (Join join : getJoins()) {
				if (join.getRightItem() instanceof Table) {
					allTables.add((Table) join.getRightItem());
				} else if (getTable() instanceof SubSelect){
					allTables.addAll(new SQLParser((Expression) join.getRightItem()).extractAllTables());
				}
			}
		}
		
		if (getWhere() != null) {
			Expression e = getWhere();
			if (e instanceof BinaryExpression) {
				allTables.addAll(new SQLParser((Expression) ((BinaryExpression) e).getRightExpression()).extractAllTables());
				allTables.addAll(new SQLParser((Expression) ((BinaryExpression) e).getLeftExpression()).extractAllTables());
			}
		}
		
		System.out.println(allTables.toString());
		return allTables;
	}
	
	public Expression getWhere() {
		return plain.getWhere();
	}
	
	public List<Expression> getGroupBys() {
		return plain.getGroupByColumnReferences();
	}
	
	public List<OrderByElement> getOrderBys() {
		return plain.getOrderByElements();
	}
	
	public List<Join> getJoins() {
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
