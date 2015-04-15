package de.hpi.hpcc.parsing;

public class ECLBuilderDrop extends ECLBuilder {
	public ECLBuilderDrop(ECLLayouts eclLayouts) {
		super(eclLayouts);
	}
	
	SQLParserDrop sqlParser;

	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserDrop(sql, eclLayouts);
		StringBuilder eclCode = new StringBuilder();
		eclCode.append("Std.File.DeleteLogicalFile('~"+sqlParser.getFullName().replace(".", "::")+"', true)");
		
		return eclCode.toString();
	}

	@Override
	protected SQLParserDrop getSqlParser() {
		return sqlParser;
	}
}
