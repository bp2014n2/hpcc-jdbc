package de.hpi.hpcc.parsing;

import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLBuilderDrop extends ECLBuilder {
	public ECLBuilderDrop(HPCCDatabaseMetaData dbMetadata) {
		super(dbMetadata);
		// TODO Auto-generated constructor stub
	}
	
	SQLParserDrop sqlParser;

	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserDrop(sql);
		StringBuilder eclCode = new StringBuilder();
		eclCode.append("Std.File.DeleteLogicalFile('~"+sqlParser.getFullName().replace(".", "::")+"', true)");
		
		return eclCode.toString();
	}

	@Override
	protected SQLParserDrop getSqlParser() {
		return sqlParser;
	}
}
