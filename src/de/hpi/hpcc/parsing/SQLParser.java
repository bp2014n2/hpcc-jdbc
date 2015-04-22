package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

public abstract class SQLParser{
	
	//public static final String parameterizedPrefix = "var";
	protected static CCJSqlParserManager parserManager = new CCJSqlParserManager();
	protected Statement statement;
	protected ECLLayouts eclLayouts;
	
	public SQLParser(Statement sql, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
		this.statement = sql;
	}
	
	
	protected static String expressionIsInstanceOf(Expression expression) {
		if (expression instanceof SubSelect) {
			return "SubSelect";
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			return "minor";		
		} 
		return "";
	}
	
	public static Statement parse(String sql) throws HPCCException {
		try {
			return CCJSqlParserUtil.parse(sql);
		} catch (JSQLParserException e) {
			throw new HPCCException("No valid SQL");
		}
	}

	public abstract List<String> getQueriedColumns(String table);
	
	public List<String> getAllTables() {
		List<String> tableList = new ArrayList<String>();
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		if (statement instanceof Select) {
			boolean nextval = false;
			if (((Select) statement).getSelectBody() instanceof PlainSelect) {
				PlainSelect sb = (PlainSelect) ((Select) statement).getSelectBody();
				
				for (SelectItem si : sb.getSelectItems()) {
					if (si instanceof SelectExpressionItem) {
						Expression ex = ((SelectExpressionItem) si).getExpression();
						if (ex instanceof Function) {
							if (((Function) ex).getName().equalsIgnoreCase("nextval")) {
								tableList.add("sequences");
								nextval = true;
							}
						}
					}
				}
			}
			if (!nextval) {
				tableList = tablesNamesFinder.getTableList((Select) statement);
			}
			
			
		} else if (statement instanceof Insert) {
			tableList = tablesNamesFinder.getTableList((Insert) statement);
		} else if (statement instanceof Update) {
			tableList = tablesNamesFinder.getTableList((Update) statement);
		} else if (statement instanceof CreateTable) {
			tableList.add(((CreateTable) statement).getTable().getName());
		} else if (statement instanceof Drop) {
			tableList.add(((Drop) statement).getName());
		} else {
			tableList = null;
		}
		List<String> lowerTableList = new ArrayList<String>();
		for (String table : tableList) {
			if (table.contains(".")) {
				table = table.split("\\.")[1];
			}
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
	
	protected List<String> findColumns(List<String> tableNameAndAlias, Expression expr) {
		ECLColumnFinder finder = new ECLColumnFinder(eclLayouts, statement, tableNameAndAlias);
		return finder.find(expr);
			
	}
	
	protected List<String> getTableNameAndAlias(String table) {
		List<String> tableNameAndAlias = new ArrayList<String>();
		tableNameAndAlias.add(table);
		Pattern findAlias = Pattern.compile("from\\s*(\\w+(\\s*(i2b2demodata\\.)?\\w+)?\\s*,\\s*)*(i2b2demodata\\.)?" + table + "\\s*(\\w+)\\s*", Pattern.CASE_INSENSITIVE);
		Matcher alias = findAlias.matcher(((Select) statement).toString());
		while (alias.find()) {
			String aliasName = alias.group(5);
			if (isValidAlias(aliasName)) {
				tableNameAndAlias.add(aliasName);
			}
		}
		return tableNameAndAlias;
	}

	private boolean isValidAlias(String aliasName) {
		List<String> invalidAlias = new ArrayList<String>();
		invalidAlias.add("where");
		if (!HPCCJDBCUtils.containsStringCaseInsensitive(invalidAlias, aliasName)) {
			return true;
		}
		return false;
	}

	public static SQLParser getInstance(String sql, ECLLayouts eclLayouts) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		return typeParser.getParser(sql);
	}
}
