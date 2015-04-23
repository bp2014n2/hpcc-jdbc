package de.hpi.hpcc.parsing.insert;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.logging.Level;

import net.sf.jsqlparser.statement.insert.Insert;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineInsert extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();
	private SQLParserInsert sqlParser;
	private Insert insert;
	
	public ECLEngineInsert(Insert insert, ECLLayouts layouts) {
		super(insert, layouts);
		this.insert = insert;
	}
	
	public String generateECL() throws SQLException{
		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(insert);
		this.sqlParser = new SQLParserInsert(insert, layouts);
		
    	ECLBuilderInsert eclBuilder = new ECLBuilderInsert(insert, layouts);
    	eclCode.append("#OPTION('expandpersistinputdependencies', 1);\n");
//    	eclCode.append("#OPTION('targetclustertype', 'thor');\n");
//    	eclCode.append("#OPTION('targetclustertype', 'hthor');\n");
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	
    	eclCode.append(generateImports());
		eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
		String tablePath = "i2b2demodata::"+ sqlParser.getTable().getName();
		//tablePath = checkForTempTable(tablePath);
		String newTablePath = tablePath + "_" + Long.toString(System.currentTimeMillis());
		
		
		eclCode.append(eclBuilder.generateECL().replace("%NEWTABLE%",newTablePath));
		
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
    		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tableName);
    		long timeAfterDFUFile = System.nanoTime();
    		long timeDifferenceDFUFile = (timeAfterDFUFile-timeBeforeDFUFile)/1000000;
    		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for getting DFUFile: "+timeDifferenceDFUFile);
    		
 
    		if(hpccQueryFile != null) {
        		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    		}
    	}

    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	HashSet<String> columns = layouts.getAllColumns(((SQLParserInsert) sqlParser).getTable().getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, layouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    		
    	} 
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserInsert getSQLParser() {
		return sqlParser;
	}
	
	protected String generateLayouts() {
    	StringBuilder layoutsString = new StringBuilder();
//    	String table = getSQLParser().getTable().getName();
//		layoutsString.append(layouts.getLayout(table));
//		layoutsString.append("\n");
		for (String table : getSQLParser().getAllTables()) {
			layoutsString.append(layouts.getLayout(table));
			layoutsString.append("\n");
		}
		return layoutsString.toString();
    }
}
