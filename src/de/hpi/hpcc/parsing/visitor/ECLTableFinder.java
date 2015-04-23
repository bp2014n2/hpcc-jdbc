package de.hpi.hpcc.parsing.visitor;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

public class ECLTableFinder extends FullVisitorAdapter {

	Set<String> tables = new HashSet<String>();
	
	public Set<String> find(Statement statement) {
		statement.accept(this);
		return tables;
	}
	
	@Override
	public void visit(Function function) {
		if (function.getName().equalsIgnoreCase("nextval")) {
			tables.add("sequences");
		}
	}
	
	@Override
	public void visit(Table tableName) {
		tables.add(tableName.getName().toLowerCase());
	}
	
	@Override
	public void visit(Drop drop) {
		tables.add(drop.getName().toLowerCase());
	}
	
}
