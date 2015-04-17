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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.NodeList;

import de.hpi.hpcc.main.*;
import de.hpi.hpcc.parsing.create.ECLEngineCreate;
import de.hpi.hpcc.parsing.drop.ECLEngineDrop;
import de.hpi.hpcc.parsing.insert.ECLEngineInsert;
import de.hpi.hpcc.parsing.select.ECLEngineSelect;
import de.hpi.hpcc.parsing.update.ECLEngineUpdate;

public abstract class ECLEngine
{

    private NodeList                resultSchema = null;
    protected HPCCDatabaseMetaData    dbMetadata;
    private StringBuilder           eclCode = new StringBuilder();
	private HPCCConnection			conn;
	protected List<HPCCColumnMetaData>    expectedretcolumns = null;
    protected HashMap<String, HPCCColumnMetaData> availablecols = null;
    private static final String			HPCCEngine = "THOR";
    private ECLSubstringDefinition substring = null;
    protected ECLLayouts eclLayouts;

    public ECLEngine(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
        this.dbMetadata = dbmetadata;
        this.conn = conn;
        this.eclLayouts = new ECLLayouts(dbMetadata);
    }
    
    abstract protected SQLParser getSQLParser();
    abstract public SQLParser getSQLParserInstance(String sqlQuery);
    
    public static ECLEngine getInstance (HPCCConnection conn, HPCCDatabaseMetaData dbMetadata, String sqlQuery) throws SQLException{
    	sqlQuery = escapeToAppropriateSQL(sqlQuery);
    	switch(SQLParser.sqlIsInstanceOf(sqlQuery)) {
    	case SELECT:
    		return new ECLEngineSelect(conn, dbMetadata);
    	case INSERT:
    		return new ECLEngineInsert(conn, dbMetadata);
    	case UPDATE:
    		return new ECLEngineUpdate(conn, dbMetadata);
    	case DROP:
    		return new ECLEngineDrop(conn, dbMetadata);
    	case CREATE:
    		return new ECLEngineCreate(conn, dbMetadata);
    	default:
    		System.out.println("type of sql not recognized"+SQLParser.sqlIsInstanceOf(sqlQuery));
    		throw new SQLException();
    	}
    }

    public String parseEclCode(String sqlQuery){
		try {
			sqlQuery = convertToAppropriateSQL(sqlQuery);
			eclCode = new StringBuilder(generateECL(sqlQuery));
			StringBuilder sb = new StringBuilder();

			sb.append("&eclText=\n");
			
			if (substring != null) {
				eclCode = new StringBuilder(createSubstring());
			}
			sb.append(eclCode.toString());
			sb.append("\n\n//"+eclMetaEscape(sqlQuery));
//			System.out.println(sb.toString());
			return sb.toString();
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
    }
    
    protected String createSubstring() {
		String correctedEclCode = eclCode.toString().replace(
				substring.toReplaceString(),
				substring.toString());
		return correctedEclCode;
    }
    
    public static String escapeToAppropriateSQL(String sql) {
		Pattern pattern = Pattern.compile("substring\\s*\\(\\s*(\\w+)\\s+from\\s+(\\d+)\\s+for\\s+(\\d+)\\s*\\)(\\s+as\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sql);
		if(matcher.find()){
			String substring = sql.substring(matcher.start(),matcher.end());
			String replaceString = matcher.group(1);
			replaceString += matcher.group(4) != null? matcher.group(4) : "";
			sql = sql.replace(substring, replaceString);
		}
		return sql;
	}
    
    public String convertToAppropriateSQL(String sql) {
    	Pattern pattern = Pattern.compile("substring\\s*\\(\\s*(\\w+)\\s+from\\s+(\\d+)\\s+for\\s+(\\d+)\\s*\\)(\\s+as\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);
    	Pattern selectPattern = Pattern.compile("select\\s*(distinct\\s*)?(((count|sum|avg)\\(w*\\))?\\w*\\s*,\\s*)*\\s*substring\\s*\\(\\s*\\w+\\s+from\\s+\\d+\\s+for\\s+\\d+\\s*\\)(\\s+as\\s+\\w+)?", Pattern.CASE_INSENSITIVE);
    	Matcher matcher = pattern.matcher(sql);
    	Matcher selectMatcher = selectPattern.matcher(sql);
		if(matcher.find()){
			String column = matcher.group(1);
			String alias = matcher.group(5);
			int start = Integer.parseInt(matcher.group(2));
			int count = Integer.parseInt(matcher.group(3));
			if (selectMatcher.find() && alias == null) {
				alias = column + "_substring";
			}
			this.substring = new ECLSubstringDefinition(column, alias, start, count);
			String substring = sql.substring(matcher.start(),matcher.end());
			sql = sql.replace(substring, this.substring.toSql());
		}
		if(sql.toLowerCase().contains("nextval")){
			String sequence = sql.substring(sql.indexOf('(')+2, sql.indexOf(')')-1);
			ECLEngine updateEngine = new ECLEngineUpdate(conn, dbMetadata);
			conn.sendRequest(updateEngine.parseEclCode("update sequences set value = value + 1 where name = '"+sequence+"'"));
			sql = "select value as nextval from sequences where name = '"+sequence+"'";
		}
		return sql;
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

    protected void addFileColsToAvailableCols(HPCCDFUFile dfufile, HashMap<String, HPCCColumnMetaData> availablecols) {
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
    
    public abstract String generateECL(String sqlQuery) throws SQLException;
    
	protected String eclMetaEscape(String sqlQuery) {
		sqlQuery = sqlQuery.replace("'", "\\'");
		sqlQuery = sqlQuery.replace("\n", " ");
		return sqlQuery;
	}
    
    protected String generateImports() {
    	return "IMPORT STD;\n";
    }
    
    protected String generateLayouts() {
		StringBuilder layoutsString = new StringBuilder("TIMESTAMP := STRING25;\n");
		
		for (String table : getSQLParser().getAllTables()) {
			if (table.contains(".")) {
				table = table.split("\\.")[1];
			}
			
			layoutsString.append(eclLayouts.getLayout(table));
			layoutsString.append("\n");	
		}
		return layoutsString.toString();
	}
    
    protected String generateLayouts(List<String> orderedColumns) {
    	StringBuilder layoutsString = new StringBuilder();
    	List<String> allTables = getSQLParser().getAllTables();
    	String table = allTables.get(0);
		if (table.contains(".")) {
			table = table.split("\\.")[1];
		}
//		layoutsString.append(table+"_record := ");
		layoutsString.append(eclLayouts.getLayoutOrdered(table, orderedColumns));
		layoutsString.append("\n");
		
		for (int i = 1; i<allTables.size(); i++) {
			String otherTable = allTables.get(i);
			if (otherTable.contains(".")) {
				otherTable = otherTable.split("\\.")[1];
			}
//			layoutsString.append(otherTable+"_record := ");
			layoutsString.append(eclLayouts.getLayout(otherTable));
			layoutsString.append("\n");
		}
		
		return layoutsString.toString();
    }
    
    protected String generateTables() {
    	StringBuilder datasetsString = new StringBuilder();
    	StringBuilder indicesString = new StringBuilder();
    	boolean usingIndices = false;
    	for (String table : getSQLParser().getAllTables()) {
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
