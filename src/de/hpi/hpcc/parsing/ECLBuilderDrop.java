package de.hpi.hpcc.parsing;

public class ECLBuilderDrop extends ECLBuilder {
	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		SQLParserDrop sqlParser = new SQLParserDrop(sql);
		StringBuilder eclCode = new StringBuilder();
		eclCode.append("Std.File.DeleteLogicalFile('~"+sqlParser.getFullName().replace(".", "::")+"', true)");
		
		return eclCode.toString();
	}
}
