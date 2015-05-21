package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ECLSelectTableFinder extends FullVisitorAdapter {

	private List<Table> tables;
	private boolean selectTable = false;

	public List<Table> find(Statement statement) {
		tables = new ArrayList<Table>();
		statement.accept(this);
		return tables ;
	}
	
	@Override
	public void visit(Table table) {
		if (selectTable) {
			tables.add(table);
			selectTable = false;
		}
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		selectTable = true;
		super.visit(plainSelect);
	}



	@Override
	public void visit(SubSelect subSelect) {
		selectTable = false;
		super.visit(subSelect);
	}



	@Override
	public void visit(SubJoin subjoin) {
		selectTable = false;
		super.visit(subjoin);
	}



	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		selectTable = false;
		super.visit(lateralSubSelect);
	}



	@Override
	public void visit(ValuesList valuesList) {
		selectTable = false;
		super.visit(valuesList);
	}
}
