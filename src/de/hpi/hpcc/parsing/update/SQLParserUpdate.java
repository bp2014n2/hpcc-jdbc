package de.hpi.hpcc.parsing.update;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParserUpdate extends SQLParser {

	private Update update;

	public SQLParserUpdate(Update update, ECLLayouts eclLayouts) {
		super(update, eclLayouts);
		this.update = update;
	}
	
	public String getName() {
		return update.getTables().get(0).getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}
	
	public Expression getWhere() {
		return update.getWhere();
	}
	
	public Expression getWhereWithoutExists() {
		return deleteExist(update.getWhere());
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
		return getExist(update.getWhere()) == null;
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
		String table = update.getTables().get(0).getName();
		return eclLayouts.getAllColumns(table);
	}
	
	public List<String> getColumns() {
		List<Column> columns = update.getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName());
		}
		return columnNames;
	}
	
	public ArrayList<Expression> getExpressions() {
		return (ArrayList<Expression>) update.getExpressions();
	}

	public boolean isIncrement() {
		return getExpressions().get(0).toString().contains(getColumns().get(0));
	}

	public List<String> getColumnsToLowerCase() {
		List<Column> columns = update.getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName().toLowerCase());
		}
		return columnNames;
	}

	@Override
	protected Statement getStatement() {
		return update;
	}

	@Override
	protected Set<String> primitiveGetAllTables() {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		return new HashSet<String>(tablesNamesFinder.getTableList(update));
	}
}
