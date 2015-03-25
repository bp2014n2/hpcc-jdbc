package de.hpi.hpcc.parsing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParser{
	
	public static final String parameterizedPrefix = "var";
	static CCJSqlParserManager parserManager = new CCJSqlParserManager();
	Statement statement;
	Expression expression;
	
	protected SQLParser(Expression expression) {
	}
	
	protected SQLParser(String sql) {
	}
	
	protected SQLParser(Statement statement) {
	}
	
	
	protected static String expressionIsInstanceOf(Expression expression) {
		if (expression instanceof SubSelect) {
			return "SubSelect";
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			return "minor";		
		} 
		return "";
	}
	
	protected static String sqlIsInstanceOf(String sql) {
		try {
			Statement statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Select) {
				return "Select";
			} else if (statement instanceof Insert) {
				return "Insert";
			} else if (statement instanceof Drop) {
				return "Drop";
			} else if (statement instanceof Update) {
				return "Update";
			} else if (statement instanceof CreateTable) {
				return "Create";
			}
		} catch (JSQLParserException e) {
			System.out.println("No valid SQL:");
			e.printStackTrace();
		}
		return "";
	}
	
	protected List<String> getAllTables() {
		List<String> tableList = new ArrayList<String>();
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

	public boolean hasWhereOf(String table, String column) {
		return statement.toString().contains(table) && statement.toString().contains(column);
	}

	public int getParameterizedCount() {
		return statement.toString().length() - statement.toString().replace("?", "").length();
	}
	
	
}
