package de.hpi.hpcc.parsing.update;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
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
}
