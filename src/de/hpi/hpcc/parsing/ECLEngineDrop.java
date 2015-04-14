package de.hpi.hpcc.parsing;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLEngineDrop extends ECLEngine {
	
	private HPCCDatabaseMetaData dbMetadata;
	private StringBuilder           eclCode = new StringBuilder();

	public ECLEngineDrop(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
		this.dbMetadata = dbmetadata;
	}

	public String generateECL(String sqlQuery) throws SQLException {
		
		SQLParserDrop sqlParser = new SQLParserDrop(sqlQuery);
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append(generateImports());
//		eclCode.append(eclBuilder.generateECL(sqlQuery));
    	
    	String tablePath = ((SQLParserDrop) sqlParser).getFullName();
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();

   		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tablePath);
//   		addFileColsToAvailableCols(hpccQueryFile, availablecols);
   		if(hpccQueryFile != null) {
   			dbMetadata.removeDFUFile(tablePath);
   			if(hpccQueryFile.isSuperFile()) {
   				eclCode.append("Std.File.DeleteSuperFile('~"+tablePath+"', TRUE);\n");
   			} else {
   				eclCode.append("Std.File.DeleteLogicalFile('~"+tablePath+"');\n");
   			}
   			
   			// TODO: replace with much, much, much better solution
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   			
   	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
   	    	HashSet<String> columns = ECLLayouts.getAllColumns(((SQLParserDrop) sqlParser).getName());
   	    	int i=0;
   	    	for (String column : columns) {
   	    		i++;
   	    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
   	    	}  	
   		} else {
   			/*
   			 * TODO: replace with much, much, much better solution
   			 */
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   		}
   		return eclCode.toString();
	}
}
