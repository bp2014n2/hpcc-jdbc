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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

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
    protected ECLLayouts eclLayouts;

    public ECLEngine(HPCCConnection conn, HPCCDatabaseMetaData dbmetadata) {
        this.dbMetadata = dbmetadata;
        this.conn = conn;
        this.eclLayouts = new ECLLayouts(dbMetadata);
    }
    
    abstract protected SQLParser getSQLParser();
    abstract public SQLParser getSQLParserInstance(String sqlQuery);
    
    public static ECLEngine getInstance (HPCCConnection conn, HPCCDatabaseMetaData dbMetadata, String sqlQuery) throws SQLException{
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

    public String parseEclCode(String sqlQuery) throws SQLException{
		sqlQuery = convertToAppropriateSQL(sqlQuery);
		eclCode = new StringBuilder(generateECL(sqlQuery));
		StringBuilder sb = new StringBuilder();

		sb.append("&eclText=\n");
		sb.append(eclCode.toString());
		sb.append("\n\n//"+eclMetaEscape(sqlQuery));
//			System.out.println(sb.toString());
		return sb.toString();
    }
    
    public String convertToAppropriateSQL(String sql) throws SQLException {
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

    public NodeList executeSelectConstant() throws HPCCException{
        try {
            long startTime = System.currentTimeMillis();

            HttpURLConnection conn = this.conn.createHPCCESPConnection(this.conn.generateUrl());
            return this.conn.parseDataset(conn.getInputStream(), startTime);
        }
        catch (IOException e){
            throw new HPCCException("Failed to initialize Connection");
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
    
    protected String generateTables() {
    	StringBuilder datasetsString = new StringBuilder();
    	StringBuilder indicesString = new StringBuilder();
    	boolean usingIndices = false;
    	for (String table : getSQLParser().getAllTables()) {
    		String fullTableName = "i2b2demodata::"+table; //TODO: avoid hard coded i2b2demodata
    		usingIndices = getIndex(table, indicesString);
			datasetsString.append(table).append(usingIndices?"_table":"").append(" := ").append("DATASET(");
			datasetsString.append("'~").append(fullTableName).append("'");
			datasetsString.append(", ").append(table+"_record").append(",").append(HPCCEngine).append(");\n");			
		}
    	return datasetsString.toString() + indicesString.toString();
    }
    
    private boolean getIndex(String tableName, StringBuilder indicesString) {
		
    	/*
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
		*/
    	boolean hasIndex = eclLayouts.hasIndex(tableName);
    	if (hasIndex) {
        	List<String> indexes = eclLayouts.getListOfIndexes(tableName);
    		List<String> columns = getSQLParser().getQueriedColumns(tableName);
        	ArrayList<Integer> scores = new ArrayList<Integer>();
        	for (String index : indexes) {
            	List<Object> indexColumns = new ArrayList<Object>(eclLayouts.getKeyedColumns(index));
            	List<Object> nonKeyedColumns = new ArrayList<Object>(eclLayouts.getNonKeyedColumns(index));
            	indexColumns.addAll(nonKeyedColumns);
            	if (!indexColumns.containsAll(columns)) scores.add(0);
            	else scores.add(10 + columns.size() - indexColumns.size());
        	}
        	String selectedIndex = indexes.get(scores.indexOf(Collections.max(scores)));
        	indicesString.append(getIndexString(tableName, selectedIndex)+"\n");
    	}
    	
    	return hasIndex;
	
	}
   
    private String getIndexString(String tableName, String index) {
    	List<String> indexParameters = new ArrayList<String>();
    	indexParameters.add(tableName+"_table");
    	String keyedColumnList = ECLUtils.join(eclLayouts.getKeyedColumns(index), ", ");
    	keyedColumnList = ECLUtils.encapsulateWithCurlyBrackets(keyedColumnList);
    	indexParameters.add(keyedColumnList);
    	String nonKeyedColumnList = ECLUtils.join(eclLayouts.getNonKeyedColumns(index), ", ");
    	nonKeyedColumnList = ECLUtils.encapsulateWithCurlyBrackets(nonKeyedColumnList);
    	indexParameters.add(nonKeyedColumnList);
    	indexParameters.add(ECLUtils.encapsulateWithSingleQuote("~"+index));
    	
    	String joined = ECLUtils.join(indexParameters, ", ");
    	joined = ECLUtils.convertToIndex(joined);
    	
    	return tableName + " := " + joined + ";";
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
