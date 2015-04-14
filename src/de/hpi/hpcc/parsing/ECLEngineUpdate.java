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

public class ECLEngineUpdate extends ECLEngine {
	
	private HPCCDatabaseMetaData dbMetadata;
	private StringBuilder           eclCode = new StringBuilder();
	private SQLParserUpdate sqlParser;
	
	public ECLEngineUpdate(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
		super(conn, dbmetadata);
		this.dbMetadata = dbmetadata;
	}

	public String generateECL(String sqlQuery) throws SQLException{
		this.sqlParser = getSQLParserInstance(sqlQuery);
		
		ECLBuilderUpdate eclBuilder = new ECLBuilderUpdate();
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append(generateImports());
    	eclCode.append(generateLayouts(eclBuilder));
		eclCode.append(generateTables());
		
    	String tablePath = sqlParser.getFullName();
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
    	
		eclCode.append(eclBuilder.generateECL(sqlQuery).toString().replace("%NEWTABLE%",newTablePath));
		
   		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(sqlParser.getFullName());
		
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
    	HashSet<String> columns = ECLLayouts.getAllColumns(sqlParser.getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
    	}  
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserUpdate getSQLParser() {
		return sqlParser;
	}

	@Override
	public SQLParserUpdate getSQLParserInstance(String sqlQuery) {
		return new SQLParserUpdate(sqlQuery);
	}
}
