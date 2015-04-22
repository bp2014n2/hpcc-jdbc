package de.hpi.hpcc.parsing.drop;

import java.util.ArrayList;
import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	public SQLParserDrop(Drop statement, ECLLayouts layouts) {
		super(statement, layouts);
	}
	
	public String getName() {
		return ((Drop) statement).getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}

	@Override
	public List<String> getQueriedColumns(String table) {
		return new ArrayList<String>();
	}
}
