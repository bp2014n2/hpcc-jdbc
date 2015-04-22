package de.hpi.hpcc.parsing.insert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
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
	
	private Insert insert;

	public SQLParserInsert(Insert insert, ECLLayouts layouts) {
		super(insert, layouts);
		this.insert = insert;
	}
	
	public List<Column> getColumns() {
		return insert.getColumns();
	}
	
	public Table getTable() {
		return insert.getTable();
	}
	
	public List<Expression> getExpressions() {
		return ((ExpressionList) insert.getItemsList()).getExpressions();
	}
	
	public List<String> getColumnNames() {
		List<Column> columns = insert.getColumns();
		List<String> columnNames = new ArrayList<String>();
		if (columns == null) {
			for(String column : eclLayouts.getAllColumns(getTable().getName())) {
				columnNames.add(column);
			}
			return columnNames;
		}
		for (Column column : columns) {
			columnNames.add(column.getColumnName());
		}
		return columnNames;
	}

	public ItemsList getItemsList() {
		return insert.getItemsList();
	}

	public boolean hasWith() {
		try {return insert.getSelect().getWithItemsList() != null;} catch (NullPointerException e) {}
		return false;
	}

	public List<WithItem> getWithItemsList() {
		return insert.getSelect().getWithItemsList();
	}

	public Select getSelect() {
		return insert.getSelect();
	}

	@Override
	protected Statement getStatement() {
		return insert;
	}

	@Override
	protected Set<String> primitiveGetAllTables() {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		return new HashSet<String>(tablesNamesFinder.getTableList(insert));
	}
}
