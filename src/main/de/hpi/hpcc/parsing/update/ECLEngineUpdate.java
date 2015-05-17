package de.hpi.hpcc.parsing.update;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.update.Update;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLDataTypeParser;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;
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
    	ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
    	List<SelectExpressionItem> selectItems = finder.find(update);
    	for (int i=0; i < selectItems.size(); i++) {
        	ECLDataTypeParser parser = new ECLDataTypeParser(layouts, getSQLParser());
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
	protected SQLParserUpdate getSQLParser() {
		return sqlParser;
	}

	@Override
	public void setSQLParser(SQLParser parser) {
		this.sqlParser = (SQLParserUpdate) parser;
	}
}
