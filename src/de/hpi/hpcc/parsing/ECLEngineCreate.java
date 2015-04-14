package de.hpi.hpcc.parsing;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLEngineCreate extends ECLEngine {

	private StringBuilder           eclCode = new StringBuilder();	
	private HPCCDatabaseMetaData dbMetadata;
	private SQLParserCreate sqlParser;
	
	
	public ECLEngineCreate(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
		this.dbMetadata = dbmetadata;
	}
	
	

	public String generateECL(String sqlQuery) throws SQLException {
		
		sqlParser = getSQLParserInstance(sqlQuery);
		
		String tablePath = sqlParser.getFullName();
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(tablePath);
		if(dfuFile == null) {
			ECLBuilderCreate eclBuilder = new ECLBuilderCreate();
			eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
	    	eclCode.append(generateImports());
	    	eclCode.append("TIMESTAMP := STRING25;\n");
			String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
			eclCode.append(eclBuilder.generateECL(sqlQuery).toString().replace("%NEWTABLE%",newTablePath));
			eclCode.append("\nSEQUENTIAL(Std.File.CreateSuperFile('~"+tablePath+"'),\n");
			eclCode.append("Std.File.StartSuperFileTransaction(),\n");
			eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
			eclCode.append("Std.File.FinishSuperFileTransaction());");
			
			String tableName = ((SQLParserCreate) sqlParser).getTableName().toLowerCase();
			HashMap<String, ECLRecordDefinition> layouts = ECLLayouts.getLayouts();
			String recordString = layouts.get(tableName).toString();
			
			if(recordString == null) {
				recordString = ((SQLParserCreate) sqlParser).getRecord();
			} else {
				recordString = recordString.substring(7, recordString.length() - 6).replace(";", ",");
			}
	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
	    	int i=0;
	    	for (String column : recordString.split(",")) {
	    		i++;
	    		expectedretcolumns.add(new HPCCColumnMetaData(column.split(" ")[1], i, ECLLayouts.getSqlTypeOfColumn(column)));
	    	}  	
		} else System.out.println("Table '"+tablePath+"' already exists. Query aborted.");
		
		return eclCode.toString();
	}



	@Override
	public SQLParserCreate getSQLParserInstance(String sqlQuery) {
		return new SQLParserCreate(sqlQuery);
	}



	@Override
	protected SQLParser getSQLParser() {
		return sqlParser;
	}
}
