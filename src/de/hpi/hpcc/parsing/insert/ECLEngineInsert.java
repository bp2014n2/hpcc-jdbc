package de.hpi.hpcc.parsing.insert;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLEngine;

public class ECLEngineInsert extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();
	private SQLParserInsert sqlParser;
	
	public ECLEngineInsert(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
	}
	
	public String generateECL(String sqlQuery) throws SQLException{
		this.sqlParser = getSQLParserInstance(sqlQuery);
		
    	ECLBuilderInsert eclBuilder = new ECLBuilderInsert(eclLayouts);
    	eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append("#OPTION('expandpersistinputdependencies', 1);\n");
//    	eclCode.append("#OPTION('targetclustertype', 'thor');\n");
//    	eclCode.append("#OPTION('targetclustertype', 'hthor');\n");
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	
    	eclCode.append(generateImports());
		eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
		String tablePath = "i2b2demodata::"+ sqlParser.getTable().getName();
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
		
		
		eclCode.append(eclBuilder.generateECL(sqlQuery).replace("%NEWTABLE%",newTablePath));
		
//		add new subfile to superfile
		eclCode.append("SEQUENTIAL(\n Std.File.StartSuperFileTransaction(),\n"
				+ " Std.File.AddSuperFile('~"+tablePath+"', '~"+newTablePath);
		eclCode.append("'),\n Std.File.FinishSuperFileTransaction());");
		
		System.out.println(eclCode.toString());
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();
		String tableName;
    	for (String table : sqlParser.getAllTables()) {
    		if(table.contains(".")) {
    			tableName = table.replace(".", "::");
    		} else {
    			tableName = "i2b2demodata::"+table;
    		}
    		
    		long timeBeforeDFUFile = System.nanoTime();
    		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tableName);
    		long timeAfterDFUFile = System.nanoTime();
    		long timeDifferenceDFUFile = (timeAfterDFUFile-timeBeforeDFUFile)/1000000;
    		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for getting DFUFile: "+timeDifferenceDFUFile);
    		
 
    		if(hpccQueryFile != null) {
        		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    		}
    	}

    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	HashSet<String> columns = eclLayouts.getAllColumns(((SQLParserInsert) sqlParser).getTable().getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, eclLayouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    		
    	} 
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserInsert getSQLParser() {
		return sqlParser;
	}

	@Override
	public SQLParserInsert getSQLParserInstance(String sqlQuery) {
		return new SQLParserInsert(sqlQuery, eclLayouts);
	}    
	
    protected String generateLayouts() {
    	StringBuilder layoutsString = new StringBuilder();
    	String table = getSQLParser().getTable().getName();
		layoutsString.append(eclLayouts.getLayout(table));
		layoutsString.append("\n");
		for (String otherTable : getSQLParser().getAllTables()) {
			layoutsString.append(eclLayouts.getLayout(otherTable));
			layoutsString.append("\n");
		}
		return layoutsString.toString();
    }
}
