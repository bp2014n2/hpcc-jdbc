package de.hpi.hpcc.parsing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.create.SQLParserCreate;
import de.hpi.hpcc.parsing.drop.SQLParserDrop;
import de.hpi.hpcc.parsing.insert.SQLParserInsert;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.update.SQLParserUpdate;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
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

abstract public class SQLParser{
	
	//public static final String parameterizedPrefix = "var";
	protected static CCJSqlParserManager parserManager = new CCJSqlParserManager();
	protected Statement statement;
	protected Expression expression;
	protected ECLLayouts eclLayouts;
	
	public enum Types {CREATE, DROP, INSERT, SELECT, UPDATE, OTHER};

	
	public SQLParser(Expression expression, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	public SQLParser(String sql, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
		try {
			statement = parserManager.parse(new StringReader(sql));
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	
	public SQLParser(Statement statement, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	
	protected static String expressionIsInstanceOf(Expression expression) {
		if (expression instanceof SubSelect) {
			return "SubSelect";
		} else if (expression instanceof MinorThan || expression instanceof Column || expression instanceof LongValue) {
			return "minor";		
		} 
		return "";
	}
	
	public static Types sqlIsInstanceOf(String sql) throws HPCCException {
		try {
			Statement statement = parserManager.parse(new StringReader(sql));
			ECLStatementTypeParser typeParser = new ECLStatementTypeParser();
			return typeParser.parse(statement);
		} catch (JSQLParserException e) {
			throw new HPCCException("No valid SQL:");
		}
	}
	
	public static SQLParser getInstance(String sql, ECLLayouts eclLayouts) throws HPCCException {
		switch(sqlIsInstanceOf(sql)) {
    	case SELECT:
    		return new SQLParserSelect(sql, eclLayouts);
    	case INSERT:
    		return new SQLParserInsert(sql, eclLayouts);
    	case UPDATE:
    		return new SQLParserUpdate(sql, eclLayouts);
    	case DROP:
    		return new SQLParserDrop(sql, eclLayouts);
    	case CREATE:
    		return new SQLParserCreate(sql, eclLayouts);
    	default:
    		System.out.println("type of sql not recognized"+SQLParser.sqlIsInstanceOf(sql));
//    		throw new SQLException();
    		return null;
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
	
	protected void findColumns(List<String> columns, List<String> tableNameAndAlias,
			Expression expr) {
		if (expr instanceof Column) {
			String columnName = ((Column) expr).getColumnName().toLowerCase();
			String tableName = ((Column) expr).getTable().getName();
			if (tableName != null) {
				if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase()) && !columns.contains(columnName)) {
					columns.add(columnName);
				}
			} else {
				Pattern selectPattern = Pattern.compile("select\\s*(distinct\\s*)?(((count|sum|avg)\\(w*\\))?\\w*\\s*,\\s*)*(" + columnName + "\\s*|(count|sum|avg)\\(\\s*" + columnName + "\\s*\\))(,\\s*((count|sum|avg)\\(w*\\))?\\w*\\s*)*from\\s*(\\w*\\.)?(\\w*)",Pattern.CASE_INSENSITIVE);
				Pattern wherePattern = Pattern.compile("from\\s*(\\w*\\.)?(\\w*)(\\s*\\w*)?\\s*where\\s*(\\(?(\\w*\\.)?\\w*\\s*((=|<=|>=)\\s*'?\\w*'?|in\\s*\\([\\w\\s\\\\'%\\.\\-]*\\))\\s*\\)?\\s*(and|or)\\s*)*\\(?" + columnName,Pattern.CASE_INSENSITIVE);
				Matcher selectMatcher = selectPattern.matcher(this.statement.toString());
				Matcher whereMatcher = wherePattern.matcher(this.statement.toString());
				if (selectMatcher.find()) {
					tableName = selectMatcher.group(11);
				} else if (whereMatcher.find()) {
					tableName = whereMatcher.group(2);
				}
				if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase())) {
					columns.add(columnName);
				}
			}
		} else if (expr instanceof BinaryExpression) {
			findColumns(columns, tableNameAndAlias, ((BinaryExpression) expr).getLeftExpression());
			findColumns(columns, tableNameAndAlias, ((BinaryExpression) expr).getRightExpression());
		} else if (expr instanceof ExistsExpression) {
			findColumns(columns, tableNameAndAlias, ((ExistsExpression) expr).getRightExpression());
		} else if (expr instanceof SubSelect) {
			SQLParserSelect selectParser = new SQLParserSelect(((SubSelect) expr).getSelectBody().toString(),eclLayouts);
			for (SelectItem selectItem : selectParser.getSelectItems()) {
				findColumns(columns,tableNameAndAlias,((SelectExpressionItem) selectItem).getExpression());
			}
			if (selectParser.getWhere() != null) {
				findColumns(columns, tableNameAndAlias, (Expression) selectParser.getWhere());
			}
			if (selectParser.getFromItem() instanceof SubSelect) {
				findColumns(columns, tableNameAndAlias, (Expression) selectParser.getFromItem());
			}
		} else if (expr instanceof InExpression) {
			findColumns(columns, tableNameAndAlias, ((InExpression) expr).getLeftExpression());
			if (((InExpression) expr).getRightItemsList() instanceof SubSelect) {
				findColumns(columns, tableNameAndAlias, (Expression) ((InExpression) expr).getRightItemsList());
			}
		} else if (expr instanceof Parenthesis) {
			findColumns(columns, tableNameAndAlias, ((Parenthesis) expr).getExpression());
		}
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
}
