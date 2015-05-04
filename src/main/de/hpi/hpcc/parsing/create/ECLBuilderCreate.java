package de.hpi.hpcc.parsing.create;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;


public class ECLBuilderCreate extends ECLBuilder {
	
	private CreateTable create;

	public ECLBuilderCreate(CreateTable create, ECLLayouts eclLayouts) {
		super(create, eclLayouts);
		this.create = create;
	}
	private SQLParserCreate sqlParser;
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL() {
		sqlParser = new SQLParserCreate(create, eclLayouts);
		eclCode = new StringBuilder();
		eclCode.append("OUTPUT(DATASET([],{");
		eclCode.append(sqlParser.getRecord());
		eclCode.append("}),,'~%NEWTABLE%',OVERWRITE);");
		return eclCode.toString();
	}

	@Override
	protected CreateTable getStatement() {
		return create;
	}
}
