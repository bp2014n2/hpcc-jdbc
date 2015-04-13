/*##############################################################################

Copyright (C) 2011 HPCC Systems.

All rights reserved. This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

package de.hpi.hpcc.parsing;

import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import org.w3c.dom.NodeList;

import de.hpi.hpcc.main.*;

public class ECLEngine
{

    private NodeList                resultSchema = null;
    private SQLParser               sqlParser;
    private HPCCDatabaseMetaData    dbMetadata;
    private StringBuilder           eclCode = new StringBuilder();
	private HPCCConnection			conn;
    private List<HPCCColumnMetaData>    expectedretcolumns = null;
    private HashMap<String, HPCCColumnMetaData> availablecols = null;
    private static final String			HPCCEngine = "THOR";
    private String sub = null;
	private String sql;

    public ECLEngine(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
        this.dbMetadata = dbmetadata;
        this.conn = conn;
    }

    private String convertToAppropriateSQL() {
		if(sql.toLowerCase().contains("substring")){
			String substring = sql.toLowerCase().substring(sql.toLowerCase().indexOf("substring"), sql.toLowerCase().indexOf("substring")+52);
			substring = substring.replace("substring(", "").replace(" from ", "[").replace(" for ", "..").replace(")", "]");
			setSubstring(substring);
			String subRange = substring.substring(substring.indexOf("["), substring.indexOf("]")+1);
			substring = substring.replace(subRange,"");
			sql = sql.replace(sql.substring(sql.toLowerCase().indexOf("substring"), sql.toLowerCase().indexOf("substring")+52),substring);
		} else if(sql.toLowerCase().contains("nextval")){
			String sequence = sql.substring(sql.indexOf('(')+2, sql.indexOf(')')-1);
			ECLEngine updateEngine = new ECLEngineUpdate(conn, dbMetadata);
			conn.sendRequest(updateEngine.parseEclCode("update sequences set value = value + 1 where name = '"+sequence+"'"));
			sql = "select value as nextval from sequences where name = '"+sequence+"'";
		}
		return sql;
	}

	private void setSubstring(String substring) {
		sub = substring;
	}

	public List<HPCCColumnMetaData> getExpectedRetCols() {
        return expectedretcolumns;
    }
    
    
    /**
     * Returns the current ECLCode as String. Is used in tests to check the correctness of the code generation. 
     * @return		eclCode as String
     */
    public String getEclCode() {
		return eclCode.toString();
	}

    private void addFileColsToAvailableCols(HPCCDFUFile dfufile, HashMap<String, HPCCColumnMetaData> availablecols) {
    	Enumeration<?> fields = dfufile.getAllFields();
	    while (fields.hasMoreElements()) {
	        HPCCColumnMetaData col = (HPCCColumnMetaData) fields.nextElement();
	        availablecols.put(col.getTableName().toLowerCase() + "." + col.getColumnName().toLowerCase(), col);
	    }
    }

    public NodeList executeSelectConstant(){
        try {
            long startTime = System.currentTimeMillis();

            HttpURLConnection conn = this.conn.createHPCCESPConnection(this.conn.generateUrl());
            return this.conn.parseDataset(conn.getInputStream(), startTime);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }
    
    public void generateECL(String sqlQuery) throws SQLException {
    	sql = sqlQuery;
    	sqlQuery = convertToAppropriateSQL();
    	switch(SQLParser.sqlIsInstanceOf(sqlQuery)) {
    	case "Select":
    		this.sqlParser = new SQLParserSelect(sqlQuery);
    		generateSelectECL(sqlQuery);
    		break;    		
    	case "Insert":
    		this.sqlParser = new SQLParserInsert(sqlQuery);
    		generateInsertECL(sqlQuery);
    		break;
    	case "Update":
    		this.sqlParser = new SQLParserUpdate(sqlQuery);
    		generateUpdateECL(sqlQuery);
    		break;
    	case "Drop":
    		this.sqlParser = new SQLParserDrop(sqlQuery);
    		generateDropECL(sqlQuery);
    		break;
    	case "Create":
    		this.sqlParser = new SQLParserCreate(sqlQuery);
    		generateCreateECL(sqlQuery);
    		break;
    	default:
    		System.out.println("type of sql not recognized"+SQLParser.sqlIsInstanceOf(sqlQuery));
    	}
    }
    	
    private void generateCreateECL(String sqlQuery) throws SQLException {
		String tablePath = ((SQLParserCreate) sqlParser).getFullName();
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(tablePath);
		if(dfuFile == null) {
			ECLBuilderCreate eclBuilder = new ECLBuilderCreate();
			eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
	    	eclCode.append(generateImports());
	    	eclCode.append("TIMESTAMP := STRING25;\n");
			String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
			eclCode.append(eclBuilder.generateECL(sqlQuery).toString().replace("%NEWTABLE%",newTablePath));
			eclCode.append("\nSEQUENTIAL(Std.File.CreateSuperFile('~"+tablePath+"'),\n");
			eclCode.append("Std.File.StartSuperFileTransaction(),\n");
			eclCode.append("Std.File.AddSuperFile('~"+tablePath+"','~"+newTablePath+"'),\n");
			eclCode.append("Std.File.FinishSuperFileTransaction());");
			
			String tableName = ((SQLParserCreate) sqlParser).getTableName().toLowerCase();
			HashMap<String, ECLRecordDefinition> layouts = ECLLayouts.getLayouts();
			String recordString = layouts.get(tableName).toString();
			
			if(recordString == null) {
				recordString = ((SQLParserCreate) sqlParser).getRecord();
			} else {
				recordString = recordString.substring(7, recordString.length() - 6).replace(";", ",");
			}
	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
	    	int i=0;
	    	for (String column : recordString.split(",")) {
	    		i++;
	    		expectedretcolumns.add(new HPCCColumnMetaData(column.split(" ")[1], i, ECLLayouts.getSqlTypeOfColumn(column)));
	    	}  	
		} else System.out.println("Table '"+tablePath+"' already exists. Query aborted.");
	}

	private void generateUpdateECL(String sqlQuery) throws SQLException {
		ECLBuilderUpdate eclBuilder = new ECLBuilderUpdate();
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append(generateImports());
    	eclCode.append(generateLayouts(eclBuilder));
		eclCode.append(generateTables());
		
    	String tablePath = ((SQLParserUpdate) sqlParser).getFullName();
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
    	
		eclCode.append(eclBuilder.generateECL(sqlQuery).toString().replace("%NEWTABLE%",newTablePath));
		
   		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(((SQLParserUpdate) sqlParser).getFullName());
		
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
    	HashSet<String> columns = ECLLayouts.getAllColumns(((SQLParserUpdate) sqlParser).getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
    	}  	
	}

	private void generateDropECL(String sqlQuery) throws SQLException {
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append(generateImports());
//		eclCode.append(eclBuilder.generateECL(sqlQuery));
    	
    	String tablePath = ((SQLParserDrop) sqlParser).getFullName();
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();

   		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tablePath);
//   		addFileColsToAvailableCols(hpccQueryFile, availablecols);
   		if(hpccQueryFile != null) {
   			dbMetadata.removeDFUFile(tablePath);
   			if(hpccQueryFile.isSuperFile()) {
   				eclCode.append("Std.File.DeleteSuperFile('~"+tablePath+"', TRUE);\n");
   			} else {
   				eclCode.append("Std.File.DeleteLogicalFile('~"+tablePath+"');\n");
   			}
   			
   			/*
   			 * TODO: replace with much, much, much better solution
   			 */
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   			
   			
   	    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
   	    	HashSet<String> columns = ECLLayouts.getAllColumns(((SQLParserDrop) sqlParser).getName());
   	    	int i=0;
   	    	for (String column : columns) {
   	    		i++;
   	    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
   	    	}  	
   		} else {
   			/*
   			 * TODO: replace with much, much, much better solution
   			 */
   			eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
   		}
	}
	
	private void generateInsertECL(String sqlQuery) throws SQLException {
    	ECLBuilderInsert eclBuilder = new ECLBuilderInsert();
    	eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append("#OPTION('expandpersistinputdependencies', 1);\n");
//    	eclCode.append("#OPTION('targetclustertype', 'thor');\n");
//    	eclCode.append("#OPTION('targetclustertype', 'hthor');\n");
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	
    	long timeBeforeImports = System.nanoTime();
    	eclCode.append(generateImports());
		long timeAfterImports = System.nanoTime();
		long timeDifferenceImports = (timeAfterImports-timeBeforeImports)/1000000;
		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for creating Imports: "+timeDifferenceImports);
		
		long timeBeforeLayouts = System.nanoTime();
		eclCode.append(generateLayouts(eclBuilder, ((SQLParserInsert) sqlParser).getColumnNames()));
		long timeAfterLayouts = System.nanoTime();
		long timeDifferenceLayouts = (timeAfterLayouts-timeBeforeLayouts)/1000000;
		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for creating Layouts: "+timeDifferenceLayouts);
		
		long timeBeforeTables = System.nanoTime();
		eclCode.append(generateTables());
		long timeAfterTables = System.nanoTime();
		long timeDifferenceTables = (timeAfterTables-timeBeforeTables)/1000000;
		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for creating Tables: "+timeDifferenceTables);
		
		
		String tablePath = "i2b2demodata::"+ ((SQLParserInsert)sqlParser).getTable().getName();
		String newTablePath = tablePath + Long.toString(System.currentTimeMillis());
		
		
		long timeBeforeECLBuilder = System.nanoTime();
		eclCode.append(eclBuilder.generateECL(sqlQuery).replace("%NEWTABLE%",newTablePath));
		long timeAfterECLBuilder = System.nanoTime();
		long timeDifferenceECLBuilder = (timeAfterECLBuilder-timeBeforeECLBuilder)/1000000;
		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for creating ECL in ECLBuilder: "+timeDifferenceECLBuilder);
		
		

//		add new subfile to superfile
		eclCode.append("SEQUENTIAL(\n Std.File.StartSuperFileTransaction(),\n"
				+ " Std.File.AddSuperFile('~"+tablePath+"', '~"+newTablePath);
		eclCode.append("'),\n Std.File.FinishSuperFileTransaction());");
		
		System.out.println(eclCode.toString());
		
		availablecols = new HashMap<String, HPCCColumnMetaData>();
		String tableName;
    	for (String table : sqlParser.getAllTables()) {
    		if(table.contains(".")) {
    			tableName = table.replace(".", "::");
    		} else {
    			tableName = "i2b2demodata::"+table;
    		}
    		
    		long timeBeforeDFUFile = System.nanoTime();
    		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tableName);
    		long timeAfterDFUFile = System.nanoTime();
    		long timeDifferenceDFUFile = (timeAfterDFUFile-timeBeforeDFUFile)/1000000;
    		HPCCJDBCUtils.traceoutln(Level.INFO, "Time for getting DFUFile: "+timeDifferenceDFUFile);
    		
    		
    		
    		if(hpccQueryFile != null) {
        		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    		}
    	}

    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	HashSet<String> columns = ECLLayouts.getAllColumns(((SQLParserInsert) sqlParser).getTable().getName());
    	int i=0;
    	for (String column : columns) {
    		i++;
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
    		
    	}  			
	}
	
	private String eclMetaEscape(String sqlQuery) {
		sqlQuery = sqlQuery.replace("'", "\\'");
		sqlQuery = sqlQuery.replace("\n", " ");
		return sqlQuery;
	}

	private void generateSelectECL(String sqlQuery) throws SQLException {
    	ECLBuilderSelect eclBuilder = new ECLBuilderSelect();
    	eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sqlQuery)+"');\n");
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	eclCode.append(generateImports());
        eclCode.append(generateLayouts(eclBuilder));
		eclCode.append(generateTables());
		
		
		if (!((SQLParserSelect) sqlParser).isCount()) eclCode.append("OUTPUT(");
    	eclCode.append(eclBuilder.generateECL(sqlQuery));
    	if (!((SQLParserSelect) sqlParser).isCount()) eclCode.append(");");

    	availablecols = new HashMap<String, HPCCColumnMetaData>();
    	
    	for (String table : sqlParser.getAllTables()) {
    		String tableName = table.contains(".")?table.replace(".", "::"):"i2b2demodata::"+table;
    		HPCCDFUFile hpccQueryFile = dbMetadata.getDFUFile(tableName);
    		addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	}
    	
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
    	ArrayList<String> selectItems = (ArrayList<String>) ((SQLParserSelect) sqlParser).getAllSelectItemsInQuery();
    	for (int i=0; i<selectItems.size(); i++) {
    		String column = selectItems.get(i);
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, ECLLayouts.getSqlTypeOfColumn(column)));
    	}
    }
    
    private String generateImports() {
    	return "IMPORT STD;\n";
    }
    
    private String generateLayouts(ECLBuilder eclBuilder) {
		StringBuilder layoutsString = new StringBuilder("TIMESTAMP := STRING25;\n");
		
		for (String table : sqlParser.getAllTables()) {
			if (table.contains(".")) {
				table = table.split("\\.")[1];
			}
			
			layoutsString.append(table+"_record := ");
			layoutsString.append(ECLLayouts.getLayouts().get(table));
			layoutsString.append("\n");	
		}
		return layoutsString.toString();
	}
    
    private String generateLayouts(ECLBuilder eclBuilder, List<String> orderedColumns) {
    	StringBuilder layoutsString = new StringBuilder("TIMESTAMP := STRING25;\n");
    	List<String> allTables = sqlParser.getAllTables();
    	String table = allTables.get(0);
		if (table.contains(".")) {
			table = table.split("\\.")[1];
		}
		layoutsString.append(table+"_record := ");
		layoutsString.append(ECLLayouts.getLayouts().get(table).toString(orderedColumns));
		layoutsString.append("\n");
		
		for (int i = 1; i<allTables.size(); i++) {
			String otherTable = allTables.get(i);
			if (otherTable.contains(".")) {
				otherTable = otherTable.split("\\.")[1];
			}
			layoutsString.append(otherTable+"_record := ");
			layoutsString.append(ECLLayouts.getLayouts().get(otherTable).toString());
			layoutsString.append("\n");
		}
		
		return layoutsString.toString();
    }
    
    private String generateTables() {
    	StringBuilder datasetsString = new StringBuilder();
    	StringBuilder indicesString = new StringBuilder();
    	boolean usingIndices = false;
    	for (String table : sqlParser.getAllTables()) {
    		usingIndices = false;
    		String tableName = table;
    		if (table.contains(".")) {
    			tableName = tableName.split("\\.")[1];
			} else {
//				tableName = table;
				table = "i2b2demodata::"+tableName;
			}
    		usingIndices = getIndex(tableName, indicesString);
			datasetsString.append(tableName).append(usingIndices?"_table":"").append(" := ").append("DATASET(");
			datasetsString.append("'~").append(table.replaceAll("\\.", "::")).append("'");
			datasetsString.append(", ").append(tableName+"_record").append(",").append(HPCCEngine).append(");\n");			
		}
    	return datasetsString.toString() + indicesString.toString();
    }
    
    private boolean getIndex(String tableName, StringBuilder indicesString) {
		switch(tableName) {
		case "observation_fact":
			indicesString.append("observation_fact := INDEX(observation_fact_table, {concept_cd,encounter_num,patient_num,provider_id,start_date,modifier_cd,instance_num,valtype_cd,tval_char,valueflag_cd,vunits_cd,end_date,location_cd,update_date,download_date,import_date,sourcesystem_cd,upload_id}, {}, '~i2b2demodata::observation_fact_idx_all');\n");
//			if(sqlParser.hasWhereOf("observation_fact","concept_cd") && !sqlParser.hasWhereOf("observation_fact","provider_id")) {
//				indicesString.append("observation_fact := INDEX(observation_fact_table, {concept_cd}, {patient_num, modifier_cd, valtype_cd, tval_char, start_date}, '~i2b2demodata::observation_fact_idx_inverted_concept_cd');\n");
//			} else if(sqlParser.hasWhereOf("observation_fact","provider_id")) {
//				indicesString.append("observation_fact := INDEX(observation_fact_table, {provider_id}, {modifier_cd, valtype_cd, tval_char, start_date, patient_num}, '~i2b2demodata::observation_fact_idx_inverted_provider_id_all');\n");
//			} else
//				indicesString.append("observation_fact := INDEX(observation_fact_table, {start_date,concept_cd, modifier_cd,valtype_cd,tval_char,patient_num},{}, '~i2b2demodata::observation_fact_idx_start_date');\n");
			return true;
		case "provider_dimension": 
			indicesString.append("provider_dimension := INDEX(provider_dimension_table, {provider_path}, {provider_id}, '~i2b2demodata::provider_dimension_idx_inverted');\n");
			return true;
		case "concept_dimension": 
//			indicesString.append("concept_dimension := INDEX(concept_dimension_table, {concept_cd}, {}, '~i2b2demodata::concept_dimension_idx_concept_cd');\n");
			indicesString.append("concept_dimension := INDEX(concept_dimension_table, {concept_path}, {concept_cd}, '~i2b2demodata::concept_dimension_idx_inverted');\n");
			return true;
		default: return false; 
		}
	
	}

	public String parseEclCode(String sqlQuery){
		try {
			generateECL(sqlQuery);
			StringBuilder sb = new StringBuilder();

			sb.append("&eclText=\n");
			
			if (sub != null) {
				String subRange = sub.substring(sub.indexOf("["),
						sub.indexOf("]") + 1);
				String subOf = sub.substring(0, sub.indexOf("["));
				String subName = sub.substring(sub.indexOf("as") + 3,
						sub.length());
				String correctedEclCode = eclCode.toString().replace(
						subName + " := " + subOf,
						subName + " := " + subOf + subRange);
				eclCode = new StringBuilder(correctedEclCode);
			}
			sb.append(eclCode.toString());
			sb.append("\n\n//"+eclMetaEscape(sql));
//			System.out.println(sb.toString());
			return sb.toString();
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
    }

    public boolean hasResultSchema()
    {
        return (this.resultSchema != null && this.resultSchema.getLength() > 0);
    }

    public void setResultschema(NodeList resultschema)
    {
        this.resultSchema = resultschema;

        if (this.resultSchema != null && this.resultSchema.getLength() > 0)
        {
            HPCCJDBCUtils.traceoutln(Level.INFO,  "contains resultschema");
        }
    }

    public NodeList getResultschema()
    {
        return resultSchema;
    }
}
