package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParser{
	
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
			}
		} catch (JSQLParserException e) {
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
	
	protected Expression containsJoinCondition(Expression expression) {
		if (expression instanceof EqualsTo && ((EqualsTo) expression).getLeftExpression() instanceof Column && ((EqualsTo) expression).getRightExpression() instanceof Column) {
			return expression;
		}
		
		if (expression instanceof BinaryExpression) {
			Expression e =  containsJoinCondition(((BinaryExpression) expression).getLeftExpression());
			if (e == null) {
				e = containsJoinCondition(((BinaryExpression) expression).getRightExpression());
			}
			return e;
		}
		
		if (expression instanceof SubSelect) {
			return containsJoinCondition(((PlainSelect) ((SubSelect) expression).getSelectBody()).getWhere());
		}
		
		if (expression instanceof ExistsExpression) {
			return containsJoinCondition(((ExistsExpression) expression).getRightExpression());
		}
		
		if (expression instanceof InExpression) {
			return containsJoinCondition(((InExpression) expression).getLeftExpression());
		}
		
		return null;
	}
	
	
}
