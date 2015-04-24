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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import net.sf.jsqlparser.statement.Statement;

import org.w3c.dom.NodeList;

import de.hpi.hpcc.main.*;

public abstract class ECLEngine
{

    private NodeList                resultSchema = null;
	protected List<HPCCColumnMetaData>    expectedretcolumns = null;
    protected HashMap<String, HPCCColumnMetaData> availablecols = null;
    private static final String			HPCCEngine = "THOR";
	protected ECLLayouts layouts;
	protected final static String EMPTY_QUERY = "OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n";

    public ECLEngine(Statement statement, ECLLayouts layouts) {
        this.layouts = layouts;
    }

	public abstract String generateECL() throws SQLException;
    
    protected abstract SQLParser getSQLParser();
	
	 public List<HPCCColumnMetaData> getExpectedRetCols() {
        return expectedretcolumns;
    }

    protected void addFileColsToAvailableCols(HPCCDFUFile dfufile, HashMap<String, HPCCColumnMetaData> availablecols) {
    	Enumeration<?> fields = dfufile.getAllFields();
	    while (fields.hasMoreElements()) {
	        HPCCColumnMetaData col = (HPCCColumnMetaData) fields.nextElement();
	        availablecols.put(col.getTableName().toLowerCase() + "." + col.getColumnName().toLowerCase(), col);
	    }
    }
    
    protected String generateImports() {
    	return "IMPORT STD;\n";
    }
    
    protected String generateLayouts() {
		StringBuilder layoutsString = new StringBuilder();

		for (String table : getSQLParser().getAllTables()) {
			if (table.contains(".")) {
				table = table.split("\\.")[1];
			}
			
			layoutsString.append(layouts.getLayout(table));
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
//			if(getSQLParser().hasWhereOf("observation_fact","concept_cd") && !getSQLParser().hasWhereOf("observation_fact","provider_id")) {
//				indicesString.append("observation_fact := INDEX(observation_fact_table, {concept_cd}, {patient_num, modifier_cd, valtype_cd, tval_char, start_date}, '~i2b2demodata::observation_fact_idx_inverted_concept_cd');\n");
//			} else if(getSQLParser().hasWhereOf("observation_fact","provider_id")) {
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
    	boolean hasIndex = layouts.hasIndex(tableName);
    	if (hasIndex) {
        	List<String> indexes = layouts.getListOfIndexes(tableName);
    		Set<String> columns = getSQLParser().getQueriedColumns(tableName);
        	ArrayList<Integer> scores = new ArrayList<Integer>();
        	for (String index : indexes) {
            	List<Object> indexColumns = new ArrayList<Object>(layouts.getKeyedColumns(index));
            	List<Object> nonKeyedColumns = new ArrayList<Object>(layouts.getNonKeyedColumns(index));
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
    	String keyedColumnList = ECLUtils.join(layouts.getKeyedColumns(index), ", ");
    	keyedColumnList = ECLUtils.encapsulateWithCurlyBrackets(keyedColumnList);
    	indexParameters.add(keyedColumnList);
    	String nonKeyedColumnList = ECLUtils.join(layouts.getNonKeyedColumns(index), ", ");
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
    
    /*
    public String checkForTempTable(String tablePath) {
    	if (eclLayouts.isTempTable(tablePath)) {
    		tablePath = eclLayouts.getTempTableName(tablePath);
    	}
    	return tablePath;
    }
    */
}
