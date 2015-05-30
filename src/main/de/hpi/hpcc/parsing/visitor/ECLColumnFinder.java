package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

public class ECLColumnFinder extends FullVisitorAdapter {

	private Set<String> columns = new HashSet<String>();
	private ECLLayouts eclLayouts;
	private String tableName;
	private Stack<List<String>> tables;

	public Set<String> find(Statement statement) {
		tables = new Stack<List<String>>();
		tables.push(new ArrayList<String>());
		statement.accept(this);
		return columns;
	}
	
	public ECLColumnFinder(ECLLayouts eclLayouts, String tableName) {
		this.tableName = tableName;
		this.eclLayouts = eclLayouts;
	}

	@Override
	public void visit(Column tableColumn) {
		String columnName = tableColumn.getColumnName();
		if (!tables.empty() 
				&& HPCCJDBCUtils.containsStringCaseInsensitive(eclLayouts.getAllColumns(tableName), columnName)
				&& HPCCJDBCUtils.containsStringCaseInsensitive(tables.peek(), tableName)) {
			columns.add(columnName);
		}
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		if (!tables.empty() && tables.peek().contains(tableName)) {
			Set<String> allColumnsSet = eclLayouts.getAllColumns(tableName);
			for(String column : allColumnsSet) {
				columns.add(column);
			}
		}
	}
	
	@Override
	public void visit(Table tableName) {
		tables.peek().add(tableName.getName());
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		boolean isFrom = plainSelect.getFromItem() != null && plainSelect.getFromItem() instanceof Table;
		if (isFrom) {
			tables.push(new ArrayList<String>());
		}
		tryAccept(plainSelect.getFromItem());  	//important to get Tables before Columns
		if(plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				join.getRightItem().accept(this);
			}
		}		
		if(plainSelect.getSelectItems() != null) {
			for(SelectItem selectItem : plainSelect.getSelectItems()) {
				selectItem.accept(this);
			}
		}
		if(plainSelect.getGroupByColumnReferences() != null) {
			for(Expression groupBy : plainSelect.getGroupByColumnReferences()) {
				groupBy.accept(this);
			}
		}		
		tryAccept(plainSelect.getHaving());
		if(plainSelect.getOrderByElements() != null) {
			for(OrderByElement orderBy : plainSelect.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		tryAccept(plainSelect.getWhere());
		if(plainSelect.getDistinct() != null && plainSelect.getDistinct().getOnSelectItems() != null) {
			for(SelectItem selectItem : plainSelect.getDistinct().getOnSelectItems()) {
				selectItem.accept(this);
			}
		}
		tryAccept(plainSelect.getForUpdateTable());
		if(plainSelect.getIntoTables() != null) {
			for(Table table : plainSelect.getIntoTables()) {
				table.accept(this);
			}
		}
		if (isFrom) {
			tables.pop();
		}
	}
	
	@Override
	public void visit(Update update) {	
		if(update.getTables() != null) {		//order is important to get tables first
			for(Table table : update.getTables()) {
				table.accept(this);
			}
		}
		if(update.getColumns() != null) {
			for(Column column : update.getColumns()) {
				column.accept(this);
			}
		}		
		tryAccept(update.getFromItem());
		tryAccept(update.getSelect());	
		tryAccept(update.getWhere());
		
	}
	
	@Override
	public void visit(Insert insert) {
		if(insert.getColumns() != null) {
			for(Column column : insert.getColumns()) {
				column.accept(this);
			}
		}
		tryAccept(insert.getTable());  //need to visit Tables first
		tryAccept(insert.getSelect());
	}
}
