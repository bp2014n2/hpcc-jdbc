package de.hpi.hpcc.parsing.insert;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class SQLParserInsert extends SQLParser {
	
	public SQLParserInsert(String sql, ECLLayouts layouts) {
		super(sql, layouts);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Insert) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	public List<Column> getColumns() {
		return ((Insert) statement).getColumns();
	}
	
	public Table getTable() {
		return ((Insert) statement).getTable();
	}
	
	public List<Expression> getExpressions() {
		return ((ExpressionList) ((Insert) statement).getItemsList()).getExpressions();
	}
	
	public List<String> getColumnNames() {
		List<Column> columns = ((Insert) statement).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName());
		}
		return columnNames;
	}

	public ItemsList getItemsList() {
		return ((Insert) statement).getItemsList();
	}

	public boolean hasWith() {
		try {return ((Insert) statement).getSelect().getWithItemsList() != null;} catch (NullPointerException e) {}
		return false;
	}

	public List<WithItem> getWithItemsList() {
		return ((Insert) statement).getSelect().getWithItemsList();
	}

	public Select getSelect() {
		return ((Insert) statement).getSelect();
	}
	
	public List<String> getAllTables() {
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
