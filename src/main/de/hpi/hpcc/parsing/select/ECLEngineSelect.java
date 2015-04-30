package de.hpi.hpcc.parsing.select;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLDataTypeParser;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineSelect extends ECLEngine {

	private StringBuilder           eclCode = new StringBuilder();
	private SQLParserSelect sqlParser;
	private Select select;
	
	public ECLEngineSelect(Select select, ECLLayouts layouts) {
		super(select, layouts);
		this.select = select;
		this.sqlParser = new SQLParserSelect(select, layouts);
	}
	
	public String generateECL() throws SQLException
    {

		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(select);
		
		
    	ECLBuilderSelect eclBuilder = new ECLBuilderSelect(select, layouts);
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	eclCode.append(generateImports());
        eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
    	eclCode.append(eclBuilder.generateECL());

    	availablecols = new HashMap<String, HPCCColumnMetaData>();
    	
    	for (String table : sqlParser.getAllTables()) {
    		String tableName = table.contains(".")?table.replace(".", "::"):"i2b2demodata::"+table;
    		//tableName = checkForTempTable(tableName);

    		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tableName);
    		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	}
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
    	List<SelectExpressionItem> selectItems = finder.find(select);
    	ECLDataTypeParser parser = new ECLDataTypeParser(layouts, getSQLParser());
    	for (int i=0; i < selectItems.size(); i++) {
    		SelectExpressionItem selectItem = selectItems.get(i);
    		String dataType = parser.parse(selectItem.getExpression());
    		ECLNameParser namer = new ECLNameParser();
    		String name = namer.name(selectItem.getExpression());
    		if(selectItem.getAlias() != null) {
    			name = selectItem.getAlias().getName();
    		}
    		int sqlType = ECLLayouts.getSqlType(dataType);
    		expectedretcolumns.add(new HPCCColumnMetaData(name, i, sqlType));
    	}
    	
    	return eclCode.toString();
    }

	@Override
	protected SQLParserSelect getSQLParser() {
		return sqlParser;
	}
}
