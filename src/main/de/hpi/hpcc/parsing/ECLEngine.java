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
import java.util.logging.Logger;

import net.sf.jsqlparser.statement.Statement;

import de.hpi.hpcc.logging.HPCCLogger;
import de.hpi.hpcc.main.*;

public abstract class ECLEngine
{
	private static final String	HPCCEngine = "THOR"; // TODO: use property from Connection
    
	protected List<HPCCColumnMetaData>    expectedretcolumns = null;
    protected HashMap<String, HPCCColumnMetaData> availablecols = null;
    protected ECLLayouts layouts;
	protected final static String EMPTY_QUERY = "OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n";
	protected static final Logger logger = HPCCLogger.getLogger();

    public ECLEngine(Statement statement, ECLLayouts layouts) {
        this.layouts = layouts;
    }

	public abstract String generateECL() throws SQLException;
    
	// TODO: remove
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
    
    /**
     * generates all layout definitions for the current query
     * @return returns the definitions as String
     */
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
    
    /**
     * generates ECL code for loading the accessed table, if available by using an index
     * @return return the ECL code as String
     */
    protected String generateTables() {
    	StringBuilder datasetsString = new StringBuilder();
    	StringBuilder indicesString = new StringBuilder();
    	for (String table : getSQLParser().getAllTables()) {
    		String fullTableName = "i2b2demodata::"+table; //TODO: avoid hard coded i2b2demodata
    		boolean hasIndex = layouts.hasIndex(table);
    		if (hasIndex) {
    			String index = getIndex(table);
    			if (index != null) {
    				indicesString.append(getIndexString(table, index) + "\n");
    			} else {
    				hasIndex = false;
    			}
    		}
			datasetsString.append(table).append(hasIndex?"_table":"").append(" := ").append("DATASET(");
			datasetsString.append("'~").append(fullTableName).append("'");
			datasetsString.append(", ").append(table+"_record").append(",").append(HPCCEngine).append(");\n");			
		}
    	return datasetsString.toString() + indicesString.toString();
    }
    
    /**
     * checks for a given table whether an appropriate index exists, that covers all necessary columns
     * @param tableName
     * @return returns the name of the index or null, if non exists
     */
    private String getIndex(String tableName) {
       	List<String> indexes = layouts.getListOfIndexes(tableName);
    	Set<String> columns = getSQLParser().getQueriedColumns(tableName);
       	ArrayList<Integer> scores = new ArrayList<Integer>();
       	for (String index : indexes) {
           	List<Object> indexColumns = layouts.getKeyedColumns(index);
           	indexColumns.addAll(layouts.getNonKeyedColumns(index));
           	if (!HPCCJDBCUtils.containsAllCaseInsensitive(indexColumns, columns)) {
           		scores.add(0);
           	} else {
           		scores.add((int) (100 * (double) columns.size() / (double) indexColumns.size()));
           	}
       	}
       	if (Collections.max(scores) == 0) {
       		return null;
       	} else {
        	return indexes.get(scores.indexOf(Collections.max(scores)));
       	}	
	}
   
    /**
     * generates the ECL code for loading an index based on the tableName and the indexName
     * @param tableName
     * @param index
     * @return returns the code for loading the index
     */
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
   
   
    //Logger methods
  	protected static void log(String infoMessage){
  		log(Level.INFO, infoMessage);
  	}
  	
  	private static void log(Level loggingLevel, String infoMessage){
  		logger.log(loggingLevel, ECLEngine.class.getSimpleName()+": "+infoMessage);
  	}

}
