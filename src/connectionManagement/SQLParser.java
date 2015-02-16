package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
//import java.util.stream.Collectors;

import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
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
	
	public Boolean matchRegex(String sql) {
		return true;
	}
	
	public void generateObjectTree(String sql) {
		try {
			/**
			if (sql.matches("select .*")){
				Select statement=(Select) parserManager.parse(new StringReader(sql));
				
				
				
				PlainSelect plain = (PlainSelect) statement.getSelectBody();
				
				//test prints
				List<SelectItem> selectItems = plain.getSelectItems();
				System.out.println(selectItems.size());
				for(int i=0;i<selectItems.size();i++)
		        {
		            Expression expression=((SelectExpressionItem) selectItems.get(i)).getExpression();  
		            System.out.println("Expression:-"+expression);
		            Column col=(Column)expression;
		            System.out.println(col.getTable()+","+col.getColumnName());      
		        }
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				List<String> tableList = tablesNamesFinder.getTableList(statement);
				for (String table : tableList) {
					System.out.println(table);
				}
			} else {
				Statement statement = null;
			}
			**/
			statement = parserManager.parse(new StringReader(sql));
	
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public List<String> getTables() {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> test =  tablesNamesFinder.getTableList((Select) statement)
				.stream()
				.map(s -> s.replace(".", "::"))
				.collect(Collectors.toList());
		return test;
	}
	
	public List<String> getSelects() {
		Select select = (Select) statement;
		PlainSelect plain = (PlainSelect) select.getSelectBody();
		if (plain.getSelectItems().get(0) instanceof AllColumns) {
			return (List<String>) new ArrayList<String>();
		}
		List<String> selectItems = plain.getSelectItems()
				.stream()
				.map(s -> transformSelectItemToString(s))
				.collect(Collectors.toList());
		return selectItems;
	}
	
	public String transformSelectItemToString(SelectItem s) {
		Column c = ((Column) ((SelectExpressionItem) s).getExpression());
		String string = "";
		if (c.getTable().getName() != null) {
			string += c.getTable();
			string += ".";
		}
		string += c.getColumnName();
		return string;
	}

}
