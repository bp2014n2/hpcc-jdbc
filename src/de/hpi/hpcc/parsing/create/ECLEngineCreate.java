package de.hpi.hpcc.parsing.create;

import java.sql.SQLException;
import java.util.LinkedList;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLEngineCreate extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();	
	private SQLParserCreate sqlParser;
	
	
	public ECLEngineCreate(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
	}

	public String generateECL(String sqlQuery) throws SQLException {
		
		sqlParser = getSQLParserInstance(sqlQuery);
		
		String tablePath = sqlParser.getFullName();
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(tablePath);
		if(dfuFile == null) {
			ECLBuilderCreate eclBuilder = new ECLBuilderCreate(eclLayouts);
			eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
	    	eclCode.append(generateImports());
	    	eclCode.append("TIMESTAMP := STRING25;\n");
			String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
			eclCode.append(eclBuilder.generateECL(sqlQuery).toString().replace("%NEWTABLE%",newTablePath));
			eclCode.append("\nSEQUENTIAL(Std.File.CreateSuperFile('~"+tablePath+"'),\n");
			eclCode.append("Std.File.StartSuperFileTransaction(),\n");
			eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
			eclCode.append("Std.File.FinishSuperFileTransaction());");
			
			String recordString = ((SQLParserCreate) sqlParser).getRecord();

	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
	    	int i=0;
	    	for (String column : recordString.split(",\\s*")) {
	    		i++;
	    		expectedretcolumns.add(new HPCCColumnMetaData(column.split("\\s+")[1], i, ECLLayouts.getSqlType(column.split("\\s+")[0])));
	    	}  	
		} else System.out.println("Table '"+tablePath+"' already exists. Query aborted.");
		
		return eclCode.toString();
	}



	@Override
	public SQLParserCreate getSQLParserInstance(String sqlQuery) {
		return new SQLParserCreate(sqlQuery, eclLayouts);
	}



	@Override
	protected SQLParserCreate getSQLParser() {
		return sqlParser;
	}
}
