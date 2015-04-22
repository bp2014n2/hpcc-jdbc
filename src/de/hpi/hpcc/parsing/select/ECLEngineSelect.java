package de.hpi.hpcc.parsing.select;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.statement.select.Select;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLEngineSelect extends ECLEngine {

	private StringBuilder           eclCode = new StringBuilder();
	private SQLParserSelect sqlParser;
	private Select select;
	
	public ECLEngineSelect(Select select, ECLLayouts layouts) {
		super(select, layouts);
		this.select = select;
	}
	
	public String generateECL() throws SQLException
    {
		this.sqlParser = new SQLParserSelect(select, layouts);
		
    	ECLBuilderSelect eclBuilder = new ECLBuilderSelect(select, layouts);
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	eclCode.append(generateImports());
        eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
    	eclCode.append(eclBuilder.generateECL());

    	availablecols = new HashMap<String, HPCCColumnMetaData>();
    	
    	for (String table : sqlParser.getAllTables()) {
    		String tableName = table.contains(".")?table.replace(".", "::"):"i2b2demodata::"+table;
    		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tableName);
    		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	}
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	List<String> selectItems = sqlParser.getAllSelectItemsInQuery();
    	for (int i=0; i<selectItems.size(); i++) {
    		String column = selectItems.get(i);
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, layouts.getSqlTypeOfColumn(sqlParser.getAllTables(), column)));
    	}
    	
    	return eclCode.toString();
    }

	@Override
	protected SQLParserSelect getSQLParser() {
		return sqlParser;
	}
	
	
}
