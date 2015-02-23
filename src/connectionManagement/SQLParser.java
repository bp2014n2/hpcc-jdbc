package connectionManagement;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParser{
	
	CCJSqlParserManager parserManager = new CCJSqlParserManager();
	Statement statement;

	public SQLParser() {
		
	}
	
	public void generateObjectTree(String sql) {
		try {
			statement = parserManager.parse(new StringReader(sql));
	
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private <T> List<String> getColumns (List<T> elements) {
		if (elements == null) return (List<String>) new ArrayList<String> ();
		List<String> result = (List<String>) new ArrayList<String>();
		for (T element : elements) {
			String funcName = "getExpression";
			try {
				//mega-hyper-krasse Meta-Programmierung
//				mega-hyper-crass meta-programming
				java.lang.reflect.Method method = element.getClass().getMethod(funcName, null);
				Column c = (Column) method.invoke(element, null);
				result.add(transformColumnToString(c));
				
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return result;
	}
	
	public List<String> getTables() {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		/* only working with Java 8
		List<String> test = tablesNamesFinder.getTableList((Select) statement)
				.stream()
				.map(s -> s.replace(".", "::"))
				.collect(Collectors.toList()); */
//		code for Java 7
		List<String> test = tablesNamesFinder.getTableList((Select) statement);
		for(int i = 0; i<test.size(); i++) {
			test.set(i, test.get(i).replace(".", "::"));
		}
		
		return test;
	}
	
	public List<String> getGroupBys() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
		
		plain.getGroupByColumnReferences();
		
		return null;
	}
	
	public List<String> getOrderBys() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
	
		return getColumns(plain.getOrderByElements());
	}
	
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
	
	

}
