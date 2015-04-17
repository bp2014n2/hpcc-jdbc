package de.hpi.hpcc.parsing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.create.SQLParserCreate;
import de.hpi.hpcc.parsing.drop.SQLParserDrop;
import de.hpi.hpcc.parsing.insert.SQLParserInsert;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.update.SQLParserUpdate;
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
	
	public static Types sqlIsInstanceOf(String sql) {
		try {
			long timeBefore = System.currentTimeMillis();
			Statement statement = parserManager.parse(new StringReader(sql));
			long timeAfter = System.currentTimeMillis();
			long timeDifference = timeAfter-timeBefore;
			HPCCJDBCUtils.traceoutln(Level.INFO, "Time for parsing SQL to object tree: "+timeDifference);
			if (statement instanceof Select) {
				return Types.SELECT;
			} else if (statement instanceof Insert) {
				return Types.INSERT;
			} else if (statement instanceof Drop) {
				return Types.DROP;
			} else if (statement instanceof Update) {
				return Types.UPDATE;
			} else if (statement instanceof CreateTable) {
				return Types.CREATE;
			}
		} catch (JSQLParserException e) {
			System.out.println("No valid SQL:");
			e.printStackTrace();
		}
		return Types.OTHER;
	}
	
	public static SQLParser getInstance(String sql, ECLLayouts eclLayouts) {
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
	
	public List<String> getAllTables() {
		List<String> tableList = new ArrayList<String>();
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		if (statement instanceof Select) {
			tableList = tablesNamesFinder.getTableList((Select) statement);
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
}
