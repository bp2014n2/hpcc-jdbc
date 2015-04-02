package de.hpi.hpcc.parsing;

public class ECLBuilderDrop extends ECLBuilder {
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
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
