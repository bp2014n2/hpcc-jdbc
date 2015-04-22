package de.hpi.hpcc.parsing.create;

import java.sql.SQLException;
import java.util.LinkedList;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLEngineCreate extends ECLEngine {

	private StringBuilder eclCode = new StringBuilder();	
	private SQLParserCreate sqlParser;
	private CreateTable create;
	
	
	public ECLEngineCreate(CreateTable create, ECLLayouts layouts) {
		super(create, layouts);
		this.create = create;
	}

	public String generateECL() throws SQLException {
		
		sqlParser = new SQLParserCreate(create, layouts);
		
		String tablePath = sqlParser.getFullName();
		HPCCDFUFile dfuFile = layouts.getDFUFile(tablePath);
		if(dfuFile == null) {
			ECLBuilderCreate eclBuilder = new ECLBuilderCreate(create, layouts);
	    	eclCode.append(generateImports());
			String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
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
		} else {
			eclCode.append(EMPTY_QUERY);
		}
		
		return eclCode.toString();
	}


	@Override
	protected SQLParserCreate getSQLParser() {
		return sqlParser;
	}
}
