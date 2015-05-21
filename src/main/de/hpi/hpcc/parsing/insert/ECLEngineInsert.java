package de.hpi.hpcc.parsing.insert;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.visitor.ECLDataTypeParser;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineInsert extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();
	private SQLParserInsert sqlParser;
	private Insert insert;
	
	public ECLEngineInsert(Insert insert, ECLLayouts layouts) {
		super(insert, layouts);
		this.insert = insert;
		this.sqlParser = new SQLParserInsert(insert, layouts);
	}
	
	public String generateECL() throws SQLException{
		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(insert);
		
    	ECLBuilderInsert eclBuilder = new ECLBuilderInsert(insert, layouts);
    	eclCode.append("#OPTION('expandpersistinputdependencies', 1);\n");
    	eclCode.append("#OPTION('outputlimit', " + outputLimit + ");\n");
    	
    	eclCode.append(generateImports());
		eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
		String tablePath = "i2b2demodata::"+ sqlParser.getTable().getName();
		String newTablePath = tablePath + "_" + Long.toString(System.currentTimeMillis());

		eclCode.append(eclBuilder.generateECL().replace("%NEWTABLE%",newTablePath));
		
//		add new subfile to superfile
		eclCode.append("SEQUENTIAL(\n Std.File.StartSuperFileTransaction(),\n"
				+ " Std.File.AddSuperFile('~"+tablePath+"', '~"+newTablePath);
		eclCode.append("'),\n Std.File.FinishSuperFileTransaction());");
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();
		String tableName;
    	for (String table : sqlParser.getAllTables()) {
    		if(table.contains(".")) {
    			tableName = table.replace(".", "::");
    		} else {
    			tableName = "i2b2demodata::"+table;
    		}
    		
    		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tableName);
    		
    		if(hpccQueryFile != null) {
        		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    		}
    	}

    	generateExpectedReturnColumns();
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserInsert getSQLParser() {
		return sqlParser;
	}
	
	private void generateExpectedReturnColumns() {
		expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
    	List<SelectExpressionItem> selectItems = finder.find(insert);
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
	}
	
	protected String generateLayouts() {
    	StringBuilder layoutsString = new StringBuilder();
    	Set<String> allTables = getSQLParser().getAllTables();
		for (String table : allTables) {
			layoutsString.append(layouts.getLayout(table));
			layoutsString.append("\n");
		}
		return layoutsString.toString();
    }
	
	//Logger methods
  	private static void log(Level loggingLevel, String infoMessage){
  		logger.log(loggingLevel, ECLEngineInsert.class.getSimpleName()+": "+infoMessage);
  	}
}
