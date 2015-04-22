package de.hpi.hpcc.parsing.drop;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import net.sf.jsqlparser.statement.drop.Drop;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLEngineDrop extends ECLEngine {
	
	private StringBuilder           eclCode = new StringBuilder();
	private SQLParserDrop sqlParser;
	private Drop drop;

	public ECLEngineDrop(Drop drop, ECLLayouts layouts) {
		super(drop, layouts);
		this.drop = drop;
	}

	public String generateECL() throws SQLException {
		
		this.sqlParser = new SQLParserDrop(drop, layouts);
    	eclCode.append(generateImports());
//		eclCode.append(eclBuilder.generateECL(sqlQuery));
    	
    	String tablePath = ((SQLParserDrop) sqlParser).getFullName();
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();

   		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tablePath);
//   		addFileColsToAvailableCols(hpccQueryFile, availablecols);
   		if(hpccQueryFile != null) {
   			
   			if(hpccQueryFile.isSuperFile()) {
   				eclCode.append("Std.File.DeleteSuperFile('~"+tablePath+"', TRUE);\n");
   			} else {
   				eclCode.append("Std.File.DeleteLogicalFile('~"+tablePath+"');\n");
   			}
   			
   			// TODO: replace with much, much, much better solution
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   			
   	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
   	    	HashSet<String> columns = layouts.getAllColumns(((SQLParserDrop) sqlParser).getName());
   	    	int i=0;
   	    	for (String column : columns) {
   	    		i++;
   	    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, layouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
   	    	}  	
   	    	layouts.removeDFUFile(tablePath);
   		} else {
   			/*
   			 * TODO: replace with much, much, much better solution
   			 */
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   		}
   		return eclCode.toString();
	}

	@Override
	protected SQLParserDrop getSQLParser() {
		return sqlParser;
	}
}
