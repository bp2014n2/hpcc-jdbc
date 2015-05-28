package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.update.Update;

public class ECLSelectTableFinder extends FullVisitorAdapter {

	private List<Table> tables = new ArrayList<Table>();
	private List<String> tableNames = new ArrayList<String>();
	private boolean selectTable = false;

	public List<Table> findTables(Statement statement) {
		tables = new ArrayList<Table>();
		statement.accept(this);
		return tables ;
	}
	
	public List<String> findTableNames(Statement statement) {
		tableNames = new ArrayList<String>();
		statement.accept(this);
		return tableNames ;
	}
	
	@Override
	public void visit(Table table) {
		if (selectTable) {
			tables.add(table);
			tableNames.add(table.getName());
			selectTable = false;
		}
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		selectTable = true;
		super.visit(plainSelect);
	}
	/*
	@Override
	public void visit(Update update) {
		selectTable = true;
		if(update.getTables() != null) {
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
	*/
	
	@Override
	public void visit(Delete delete) {
		selectTable = true;
		super.visit(delete);
	}
}
