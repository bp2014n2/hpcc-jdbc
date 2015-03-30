package de.hpi.hpcc.parsing;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParserInsert extends SQLParser {

//	Insert insert;
	protected SQLParserInsert(Expression expression) {
		super(expression);
		// TODO Auto-generated constructor stub
	}
	
	protected SQLParserInsert(String sql) {
		super(sql);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Insert) {
//				insert = (Insert) parserManager.parse(new StringReader(sql));
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected SQLParserInsert(Statement statement) {
		super(statement);
		// TODO Auto-generated constructor stub
	}
	
	protected Boolean isAllColumns() {
		if (((Insert) statement).getColumns() == null) return true;
		return ((Insert) statement).getColumns().size() == ECLLayouts.getAllColumns(getTable().getName()).size();
	}
	
	protected Table getTable() {
		return ((Insert) statement).getTable();
	}
	
	protected List<Expression> getExpressions() {
		return ((ExpressionList) ((Insert) statement).getItemsList()).getExpressions();
	}
	
	protected List<String> getColumnNames() {
		List<Column> columns = ((Insert) statement).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName().toLowerCase());
		}
		return columnNames;
	}

	protected LinkedHashSet<String> getAllCoumns() {
		String table = ((Insert) statement).getTable().getName();
		return ECLLayouts.getAllColumns(table);
	}

	protected ItemsList getItemsList() {
		return ((Insert) statement).getItemsList();
	}

	protected boolean hasWith() {
		try {return ((Insert) statement).getSelect().getWithItemsList() != null;} catch (NullPointerException e) {}
		return false;
	}

	protected List<WithItem> getWithItemsList() {
		return ((Insert) statement).getSelect().getWithItemsList();
	}

	protected Select getSelect() {
		return ((Insert) statement).getSelect();
	}
	
	protected List<String> getAllTables() {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableList = new ArrayList<String>();
		tableList = tablesNamesFinder.getTableList((Insert) statement);
		if (getSelect() != null)
			tableList.addAll(tablesNamesFinder.getTableList(getSelect()));
		List<String> lowerTableList = new ArrayList<String>();
		for (String table : tableList) {
			if (table.contains(".")) table = table.substring(table.indexOf(".")+1);
			lowerTableList.add(table.toLowerCase());
		}
		return lowerTableList;
	}
}
