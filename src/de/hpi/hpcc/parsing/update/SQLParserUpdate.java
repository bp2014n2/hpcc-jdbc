package de.hpi.hpcc.parsing.update;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParserUpdate extends SQLParser {

	public SQLParserUpdate(String sql, ECLLayouts eclLayouts) {
		super(sql, eclLayouts);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Update) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	public String getName() {
		return ((Update) statement).getTables().get(0).getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}
	
	public Expression getWhere() {
		return ((Update) statement).getWhere();
	}
	
	public Expression getWhereWithoutExists() {
		return deleteExist(((Update) statement).getWhere());
	}
	
	private Expression deleteExist(Expression expr) {
		if (expr instanceof BinaryExpression) {
			Expression l = deleteExist(((BinaryExpression) expr).getLeftExpression());
			Expression r = deleteExist(((BinaryExpression) expr).getRightExpression());
			if (l == null) {
				expr = ((BinaryExpression) expr).getRightExpression();
			} else if (r == null) {
				expr = ((BinaryExpression) expr).getLeftExpression();
			} else {
				((BinaryExpression)expr).setLeftExpression(l);
				((BinaryExpression)expr).setRightExpression(r);
			}
		} else if (expr instanceof ExistsExpression) {
			return null;
		}
		return expr;
	}
	
	public boolean hasExist() {
		return getExist(((Update) statement).getWhere()) == null;
	}
	
	public Expression getExist(Expression expr) {
		if (expr instanceof ExistsExpression) 
			return expr;
		else if (expr instanceof BinaryExpression) 
			if (getExist(((BinaryExpression) expr).getLeftExpression()) != null)
				return getExist(((BinaryExpression) expr).getLeftExpression());
			else if (getExist(((BinaryExpression) expr).getRightExpression()) != null)
				return getExist(((BinaryExpression) expr).getRightExpression());
		return null;
	}
	
	public LinkedHashSet<String> getAllCoumns() {
		String table = ((Update) statement).getTables().get(0).getName();
		return eclLayouts.getAllColumns(table);
	}
	
	public List<String> getColumns() {
		List<Column> columns = ((Update) statement).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName());
		}
		return columnNames;
	}
	
	public ArrayList<Expression> getExpressions() {
		return (ArrayList<Expression>) ((Update) statement).getExpressions();
	}

	public boolean isIncrement() {
		return getExpressions().get(0).toString().contains(getColumns().get(0));
	}

	public List<String> getColumnsToLowerCase() {
		List<Column> columns = ((Update) statement).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName().toLowerCase());
		}
		return columnNames;
	}

	@Override
	public List<String> getQueriedColumns(String table) {
		List<String> columns = findColumns(getTableNameAndAlias(table), ((Update) statement).getWhere());
		return columns;
	}
}
