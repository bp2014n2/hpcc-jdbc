package de.hpi.hpcc.parsing.update;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

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
			BinaryExpression be = (BinaryExpression) expr;
			Expression l = deleteExist(be.getLeftExpression());
			Expression r = deleteExist(be.getRightExpression());
			if (l == null) {
				expr = be.getRightExpression();
			} else if (r == null) {
				expr = be.getLeftExpression();
			} else {
				be.setLeftExpression(l);
				be.setRightExpression(r);
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
		if (expr instanceof ExistsExpression) {
			return expr;
		} else if (expr instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression) expr;
			if (getExist(be.getLeftExpression()) != null) {
				return getExist(be.getLeftExpression());
			}
			else if (getExist(be.getRightExpression()) != null) {
				return getExist(be.getRightExpression());
			}
		}
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
}
