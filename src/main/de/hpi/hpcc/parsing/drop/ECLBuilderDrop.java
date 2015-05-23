package de.hpi.hpcc.parsing.drop;

import net.sf.jsqlparser.statement.drop.Drop;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLBuilderDrop extends ECLBuilder {
	
	/*
	 * TODO: remove
	 * this class is used only within tests
	 */
	
	private Drop drop;
	private SQLParserDrop sqlParser;

	public ECLBuilderDrop(Drop drop, ECLLayouts eclLayouts) {
		super(drop, eclLayouts);
		this.drop = drop;
	}

	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL() {
		sqlParser = new SQLParserDrop(drop, eclLayouts);
		eclCode = new StringBuilder();
		eclCode.append("Std.File.DeleteLogicalFile('~"+sqlParser.getFullName().replace(".", "::")+"', true)");
		
		return eclCode.toString();
	}

	@Override
	protected Drop getStatement() {
		return drop;
	}
}
