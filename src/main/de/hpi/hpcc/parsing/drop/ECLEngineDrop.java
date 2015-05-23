package de.hpi.hpcc.parsing.drop;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;

import net.sf.jsqlparser.statement.drop.Drop;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineDrop extends ECLEngine {
	
	private StringBuilder eclCode = new StringBuilder();
	private SQLParserDrop sqlParser;
	private Drop drop;
	private String originalTableName;

	public ECLEngineDrop(Drop drop, ECLLayouts layouts) {
		super(drop, layouts);
		this.drop = drop;
		originalTableName = drop.getName();
		this.sqlParser = new SQLParserDrop(drop, layouts);
	}

	public String generateECL() throws SQLException {

		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(drop);
		
    	eclCode.append(generateImports());
    	
    	String tablePath = sqlParser.getFullName();
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();

   		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tablePath);
   		if(hpccQueryFile != null) {
   			if(hpccQueryFile.isSuperFile()) {
   				eclCode.append("Std.File.DeleteSuperFile('~"+tablePath+"', TRUE);\n");
   			} else {
   				eclCode.append("Std.File.DeleteLogicalFile('~"+tablePath+"');\n");
   			}
   			
   			// TODO: replace with much, much, much better solution
   			eclCode.append(EMPTY_QUERY);
   			outputCount++;
   	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
 	
			layouts.removeDFUFile(tablePath);
			layouts.removeTempTable(layouts.getFullTableName(originalTableName));
   		} else {
   			return null;
   		}
   		return eclCode.toString();
	}

	@Override
	protected SQLParserDrop getSQLParser() {
		return sqlParser;
	}
}
