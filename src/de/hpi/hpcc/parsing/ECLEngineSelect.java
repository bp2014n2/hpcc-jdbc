package de.hpi.hpcc.parsing;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;

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
		
		
		if (!sqlParser.isCount()) eclCode.append("OUTPUT(");
    	eclCode.append(eclBuilder.generateECL(sqlQuery));
    	if (!sqlParser.isCount()) eclCode.append(");");

    	availablecols = new HashMap<String, HPCCColumnMetaData>();
    	
    	for (String table : sqlParser.getAllTables()) {
    		String tableName = table.contains(".")?table.replace(".", "::"):"i2b2demodata::"+table;
    		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tableName);
    		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	}
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	HashMap<String, List<String>> selectItems = (HashMap<String, List<String>>) sqlParser.getAllSelectItemsInQuery();
    	
    	// TODO: replace select * by columns names
    	int i = 0;
    	//for (int i=0; i<selectItems.size(); i++) {
    	for (Entry<String, List<String>> entry : selectItems.entrySet()) {
    		List<String> columns;
    		if (entry.getValue() == null) {
    			try {
					columns = new ArrayList<String>(eclLayouts.getAllColumns(entry.getKey()));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					throw new SQLException(e);
				}
    		} else {
    			columns = entry.getValue();
    		}
    		for (String column : columns) {
    			expectedretcolumns.add(new HPCCColumnMetaData(column, i, eclLayouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    			i++;
    		}
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
