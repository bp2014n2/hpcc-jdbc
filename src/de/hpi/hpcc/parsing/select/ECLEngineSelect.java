package de.hpi.hpcc.parsing.select;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.parsing.ECLEngine;

public class ECLEngineSelect extends ECLEngine {

	private StringBuilder           eclCode = new StringBuilder();
	private SQLParserSelect sqlParser;
	
	public ECLEngineSelect(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
	}
	
	public String generateECL(String sqlQuery) throws SQLException
    {
		this.sqlParser = getSQLParserInstance(sqlQuery);
		
    	ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
    	eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	eclCode.append(generateImports());
        eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
    	eclCode.append(eclBuilder.generateECL(sqlQuery));

    	availablecols = new HashMap<String, HPCCColumnMetaData>();
    	
    	for (String table : sqlParser.getAllTables()) {
    		String tableName = table.contains(".")?table.replace(".", "::"):"i2b2demodata::"+table;
    		tableName = checkForTempTable(tableName);
    		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tableName);
    		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	}
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	List<String> selectItems = sqlParser.getAllSelectItemsInQuery();
    	for (int i=0; i<selectItems.size(); i++) {
    		String column = selectItems.get(i);
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, eclLayouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    	}
    	
    	return eclCode.toString();
    }

	@Override
	public SQLParserSelect getSQLParserInstance(String sqlQuery) {
		return new SQLParserSelect(sqlQuery, eclLayouts);
	}

	@Override
	protected SQLParserSelect getSQLParser() {
		return sqlParser;
	}
	
	
}
