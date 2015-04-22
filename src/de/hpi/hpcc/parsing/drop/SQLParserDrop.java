package de.hpi.hpcc.parsing.drop;

import java.util.HashSet;
import java.util.Set;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	private Drop drop;

	public SQLParserDrop(Drop statement, ECLLayouts layouts) {
		super(statement, layouts);
		this.drop = statement;
	}
	
	public String getName() {
		return drop.getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}

	@Override
	protected Statement getStatement() {
		return drop;
	}

	@Override
	protected Set<String> primitiveGetAllTables() {
		Set<String> tables = new HashSet<String>();
		tables.add(drop.getName());
		return tables;
	}
}
