package de.hpi.hpcc.parsing.create;

import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;


public class ECLBuilderCreate extends ECLBuilder {
	
	public ECLBuilderCreate(ECLLayouts eclLayouts) {
		super(eclLayouts);
	}
	private SQLParserCreate sqlParser;
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserCreate(sql, eclLayouts);
		eclCode = new StringBuilder();
		eclCode.append("OUTPUT(DATASET([],{");
		String recordString = sqlParser.getRecord();
		eclCode.append(recordString);
		eclCode.append("}),,'~%NEWTABLE%',OVERWRITE);");
		return eclCode.toString();
	}

	@Override
	protected SQLParserCreate getSqlParser() {
		return sqlParser;
	}
}
