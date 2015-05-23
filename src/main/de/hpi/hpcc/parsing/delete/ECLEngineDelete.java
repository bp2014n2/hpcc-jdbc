package de.hpi.hpcc.parsing.delete;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.visitor.ECLDataTypeParser;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;
import de.hpi.hpcc.parsing.visitor.ECLTempTableParser;

public class ECLEngineDelete extends ECLEngine {

	private Delete delete;
	private SQLParserDelete sqlParser;
	private StringBuilder eclCode = new StringBuilder();
	
	public ECLEngineDelete(Delete delete, ECLLayouts layouts) {
		super(delete, layouts);
		this.delete = delete;
		this.sqlParser = new SQLParserDelete(delete, layouts);
	}

	@Override
	public String generateECL() throws SQLException {
		ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
		tempTableParser.replace(delete);
		
		ECLBuilderDelete eclBuilder = new ECLBuilderDelete(delete, layouts);
    	eclCode.append(generateImports());
    	eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
    	String tablePath = sqlParser.getFullName();
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
    	
		eclCode.append(eclBuilder.generateECL().toString().replace("%NEWTABLE%",newTablePath));
		
   		HPCCDFUFile hpccQueryFile = layouts.getDFUFile(tablePath);
		eclCode.append("SEQUENTIAL(\nStd.File.StartSuperFileTransaction(),\n Std.File.ClearSuperFile('~"+tablePath+"'),\n");
		for(String subfile : hpccQueryFile.getSubfiles()) {
			eclCode.append("Std.File.DeleteLogicalFile('~"+subfile+"'),\n");
		}
		eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
		eclCode.append("Std.File.FinishSuperFileTransaction());");
		System.out.println(eclCode.toString());
		
		outputCount += eclBuilder.getOutputCount();
    	
    	availablecols = new HashMap<String, HPCCColumnMetaData>();


   		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	
    	generateExpectedReturnColumns();
    	
    	return eclCode.toString();
	}

	@Override
	protected SQLParserDelete getSQLParser() {
		return sqlParser;
	}
	
	private void generateExpectedReturnColumns() {
		expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
    	List<SelectExpressionItem> selectItems = finder.find(delete);
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
}
