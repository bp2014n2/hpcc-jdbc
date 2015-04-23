package de.hpi.hpcc.parsing.visitor;

import de.hpi.hpcc.parsing.ECLLayouts;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.FromItem;

public class ECLTempTableParser extends FullVisitorAdapter {

	private ECLLayouts layouts;

	public ECLTempTableParser(ECLLayouts layouts) {
		this.layouts = layouts;
	}
	
	public void replace(FromItem fromItem) {
		fromItem.accept(this);
	}
	
	public void replace(Statement statement) {
		statement.accept(this);
	}
	
	@Override
	public void visit(Table tableName) {
		String name = tableName.getName();
		if (layouts.isTempTable(name)) {
    		name = layouts.getShortTempTableName(name);
    	}
		tableName.setName(name);
	}

	@Override
	public void visit(Drop drop) {
		String name = drop.getName();
		if (layouts.isTempTable(name)) {
    		name = layouts.getShortTempTableName(name);
    	}
		drop.setName(name);
	}
	
	@Override
	public void visit(Column tableColumn) {
		Table table = tableColumn.getTable();
		if (table != null && table.getName() != null) {
			table.accept(this);
		}
	}
}
