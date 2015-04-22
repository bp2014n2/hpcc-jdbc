package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;

public abstract class SQLParser{
	
	protected ECLLayouts eclLayouts;
	
	public SQLParser(Statement sql, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	protected abstract Statement getStatement();
	
	protected abstract List<String> primitiveGetAllTables();
	
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

	public List<String> getQueriedColumns(String table) {
		return findColumns(getTableNameAndAlias(table), getStatement());
	};
	
	public List<String> getAllTables() {
		List<String> tableList = primitiveGetAllTables();
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
		return getStatement().toString().contains(table) && getStatement().toString().contains(column);
	}

	public int getParameterizedCount() {
		return getStatement().toString().length() - getStatement().toString().replace("?", "").length();
	}
	
	protected List<String> findColumns(List<String> tableNameAndAlias, Statement statement) {
		ECLColumnFinder finder = new ECLColumnFinder(eclLayouts, tableNameAndAlias);
		return finder.find(statement);
			
	}
	
	protected List<String> getTableNameAndAlias(String table) {
		List<String> tableNameAndAlias = new ArrayList<String>();
		tableNameAndAlias.add(table);
		Pattern findAlias = Pattern.compile("from\\s*(\\w+(\\s*(i2b2demodata\\.)?\\w+)?\\s*,\\s*)*(i2b2demodata\\.)?" + table + "\\s*(\\w+)\\s*", Pattern.CASE_INSENSITIVE);
		Matcher alias = findAlias.matcher(((Select) getStatement()).toString());
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
