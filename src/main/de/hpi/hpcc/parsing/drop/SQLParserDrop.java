package de.hpi.hpcc.parsing.drop;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	private Drop drop;

	public SQLParserDrop(Drop statement, ECLLayouts layouts) {
		super(layouts);
		this.drop = statement;
	}
	
	public String getName() {
		return drop.getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}

	@Override
	public Statement getStatement() {
		return drop;
	}
}
