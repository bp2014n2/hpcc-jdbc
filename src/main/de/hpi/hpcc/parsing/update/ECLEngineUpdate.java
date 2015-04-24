package de.hpi.hpcc.parsing.update;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import net.sf.jsqlparser.statement.update.Update;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineUpdate extends ECLEngine {
	
	private StringBuilder eclCode = new StringBuilder();
	private SQLParserUpdate sqlParser;
	private Update update;
	
	public ECLEngineUpdate(Update update, ECLLayouts layouts) {
		super(update, layouts);
		this.update = update;
	}

	public String generateECL() throws SQLException {
		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(update);
		
		this.sqlParser = new SQLParserUpdate(update, layouts);
		
		ECLBuilderUpdate eclBuilder = new ECLBuilderUpdate(update, layouts);
    	eclCode.append(generateImports());
    	eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
    	String tablePath = sqlParser.getFullName();
    	//tablePath = checkForTempTable(tablePath);
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
    	
		eclCode.append(eclBuilder.generateECL().toString().replace("%NEWTABLE%",newTablePath));
		
   		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(sqlParser.getFullName());
		
		eclCode.append("SEQUENTIAL(\nStd.File.StartSuperFileTransaction(),\n Std.File.ClearSuperFile('~"+tablePath+"'),\n");
		for(String subfile : hpccQueryFile.getSubfiles()) {
			eclCode.append("Std.File.DeleteLogicalFile('~"+subfile+"'),\n");
		}
		eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
		eclCode.append("Std.File.FinishSuperFileTransaction());");
		System.out.println(eclCode.toString());
    	
    	availablecols = new HashMap<String, HPCCColumnMetaData>();


   		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	HashSet<String> columns = layouts.getAllColumns(sqlParser.getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, layouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    	}  
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserUpdate getSQLParser() {
		return sqlParser;
	}
}
