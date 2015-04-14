package de.hpi.hpcc.parsing;

import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLBuilderCreate extends ECLBuilder {
	
	public ECLBuilderCreate(HPCCDatabaseMetaData dbMetadata) {
		super(dbMetadata);
		// TODO Auto-generated constructor stub
	}
	private SQLParserCreate sqlParser;
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserCreate(sql);
		StringBuilder eclCode = new StringBuilder();
		String tableName = ((SQLParserCreate) sqlParser).getTableName();
		eclCode.append("OUTPUT(DATASET([],{");
		//remove "RECORD " at beginning of Layout definition
		String record = ECLLayouts.getLayout(tableName, dbMetadata);
		
		String recordString = null;
		if(record == null) {
			recordString = ((SQLParserCreate) sqlParser).getRecord();
		} else {
			recordString = record.toString();
			recordString = recordString.substring(7, recordString.length() - 6).replace(";", ",");
		}
		eclCode.append(recordString);
		eclCode.append("}),,'~%NEWTABLE%',OVERWRITE);");
		return eclCode.toString();
	}

	@Override
	protected SQLParser getSqlParser() {
		// TODO Auto-generated method stub
		return sqlParser;
	}
}
