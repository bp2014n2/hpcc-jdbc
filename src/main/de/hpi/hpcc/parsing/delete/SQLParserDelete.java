package de.hpi.hpcc.parsing.delete;

import net.sf.jsqlparser.statement.delete.Delete;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;

public class SQLParserDelete extends SQLParser {

	private Delete delete;
	public SQLParserDelete(Delete delete, ECLLayouts eclLayouts) {
		super(eclLayouts);
		this.delete = delete;
	}

	@Override
	public Delete getStatement() {
		return delete;
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}

	public String getName() {
		return delete.getTable().getName();
	}

}
