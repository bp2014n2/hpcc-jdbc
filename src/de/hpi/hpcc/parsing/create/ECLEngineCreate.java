package de.hpi.hpcc.parsing.create;

import java.sql.SQLException;
import java.util.LinkedList;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLTempTableParser;

public class ECLEngineCreate extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();	
	private SQLParserCreate sqlParser;
	private CreateTable create;
	
	
	public ECLEngineCreate(CreateTable create, ECLLayouts layouts) {
		super(create, layouts);
		this.create = create;
		sqlParser = new SQLParserCreate(create, layouts);
		if (sqlParser.isTempTable()) {
			layouts.addTempTable(layouts.getFullTableName(create.getTable().getName()));
			ECLTempTableParser tempTableParser = new ECLTempTableParser(layouts);
			tempTableParser.replace(create.getTable());
		}
	}

	public String generateECL() throws SQLException {
		String tablePath = sqlParser.getFullName();
		
		/*
		if (sqlParser.isTempTable()) {
			tablePath = eclLayouts.getTempTableName(tablePath);
			eclLayouts.addTempTable(tablePath);
		}
		*/
		
		HPCCDFUFile dfuFile = layouts.getDFUFile(tablePath);

		if(dfuFile == null) {
			ECLBuilderCreate eclBuilder = new ECLBuilderCreate(create, layouts);
	    	eclCode.append(generateImports());

			String newTablePath = tablePath + "_" + Long.toString(System.currentTimeMillis());
			eclCode.append(eclBuilder.generateECL().toString().replace("%NEWTABLE%",newTablePath));

			eclCode.append("\nSEQUENTIAL(Std.File.CreateSuperFile('~"+tablePath+"'),\n");
			eclCode.append("Std.File.StartSuperFileTransaction(),\n");
			eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
			eclCode.append("Std.File.FinishSuperFileTransaction());");
			
			String recordString = ((SQLParserCreate) sqlParser).getRecord();

	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
	    	int i=0;
	    	for (String column : recordString.split(",\\s*")) {
	    		i++;
	    		expectedretcolumns.add(new HPCCColumnMetaData(column.split("\\s+")[1], i, ECLLayouts.getSqlType(column.split("\\s+")[0])));
	    	}  	
		} else System.out.println("Table '"+tablePath+"' already exists. Query aborted.");
		
		return eclCode.toString();
	}


	@Override
	protected SQLParserCreate getSQLParser() {
		return sqlParser;
	}
}
