package de.hpi.hpcc.parsing;

import java.util.HashSet;
import java.util.Set;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.visitor.ECLColumnFinder;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;
import de.hpi.hpcc.parsing.visitor.ECLTableFinder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SubSelect;

public abstract class SQLParser{
	
	protected ECLLayouts eclLayouts;
	
	public SQLParser(Statement sql, ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}
	
	protected abstract Statement getStatement();
	
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

	public Set<String> getQueriedColumns(String table) {
		ECLColumnFinder finder = new ECLColumnFinder(eclLayouts, table);
		return finder.find(getStatement());
	}
	
	public Set<String> getAllTables() {
		ECLTableFinder finder = new ECLTableFinder();
		Set<String> tableList = finder.find(getStatement());
		Set<String> lowerTableList = new HashSet<String>();
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

	public static SQLParser getInstance(String sql, ECLLayouts eclLayouts) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		return typeParser.getParser(sql);
	}
}
