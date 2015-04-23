package de.hpi.hpcc.parsing.visitor;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.WithItem;

public class ECLTableFinder extends FullVisitorAdapter {

	private Set<String> tables = new HashSet<String>();
	
	private Set<String> otherItemNames = new HashSet<String>();
	
	public Set<String> find(Statement statement) {
		statement.accept(this);
		return tables;
	}
	
	@Override
    public void visit(WithItem withItem) {
        otherItemNames.add(withItem.getName().toLowerCase());
        super.visit(withItem);
    }
	
	@Override
	public void visit(Function function) {
		if (function.getName().equalsIgnoreCase("nextval")) {
			tables.add("sequences");
		}
	}
	
	@Override
	public void visit(Table tableName) {
		
		String table = tableName.getFullyQualifiedName();
		if(!otherItemNames.contains(table.toLowerCase())) {
			Alias alias = tableName.getAlias();
			if (alias != null) {
				otherItemNames.add(alias.getName().toLowerCase());
			}
			tables.add(table.toLowerCase());
		}
	}
	
	@Override
	public void visit(Drop drop) {
		tables.add(drop.getName().toLowerCase());
	}
	
	@Override
	public void visit(Column tableColumn) {

	}
	
}
