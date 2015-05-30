package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLLayouts {
    
	HPCCDatabaseMetaData dbMetadata;
	private HashMap<String, List<String>> tableNameAliases = new HashMap<String, List<String>>();
	
	public ECLLayouts (HPCCDatabaseMetaData dbMetadata) {
		this.dbMetadata = dbMetadata;
	}
	
	public String getPublicSchema() {
		return this.dbMetadata.getPublicSchema();
	}
	
	public String getTableNameForAlias(String alias) {
		if (tableNameAliases.containsKey(alias)) {
			return alias;
		}
		for(String table : tableNameAliases.keySet()) {
			List<String> aliases = tableNameAliases.get(table);
			for (String currentAlias : aliases) {
				if (currentAlias.equalsIgnoreCase(alias)) {
					return table;
				}
			}
		}
		return null;
	}
	
	public void addAlias(String tableName, String alias) {
		if (tableNameAliases.containsKey(tableName) && alias != null) {
			tableNameAliases.get(tableName).add(alias);
		} else {
			List<String> newAlias = new ArrayList<String>();
			if (alias != null) newAlias.add(alias);
			tableNameAliases.put(tableName, newAlias);
		} 
	}
	
	/**
	 * checks whether tableName refers to an existing temporary table
	 * @param tableName
	 * @return
	 */
	public boolean isTempTable(String tableName) {
		return this.dbMetadata.isTempTable(getFullTableName(tableName));
	}
	
	/**
	 * adds table with name tableName to list of temporary tables
	 * @param tableName
	 */
	public void addTempTable(String tableName) {
		this.dbMetadata.addTempTable(tableName);
	}
	
	/**
	 * removes table from list of temporary tables
	 * @param tableName
	 */
	public void removeTempTable(String tableName) {
		this.dbMetadata.removeTempTable(tableName);
	}
	
	/**
	 * returns the ECL datatype of a given column by checking the DFUFile of the corresponding table
	 * @param table
	 * @param column
	 * @return
	 */
	public String getECLDataType(String table, String column){
		table = getFullTableName(table);
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
		if (dfuFile != null) {
			HPCCColumnMetaData columnMeta = dfuFile.getFieldMetaData(column);
			//TODO: only a quick fix, make it more general
			if (columnMeta == null && column == "__internal_fpos__") {
				return "unsigned8";
			}
			return columnMeta.getEclType();
		}
		return ""; //TODO: why not null?
	}
	
	/**
	 * returns 
	 * @param index
	 * @return
	 */
	public List<String> getKeyedColumns(String tableName, String index) {  //TODO: assure right order of keyed/non-keyed columns
		if (index != null) {
			index = getFullTableName(index);
			HPCCDFUFile file = dbMetadata.getDFUFile(index);
			if(file != null) {
				List<String> output = new ArrayList<String>();
				for (Object column : getSortedPropertyValues(file.getKeyedColumns())) {
					output.add(column.toString());
				}
				return output;
			} 
		}
		return getKeyedColumnsForTempIndex(tableName);
		
	}
	
	public List<String> getNonKeyedColumns(String tableName, String index) {
		if (index != null) {
			index = getFullTableName(index);
			HPCCDFUFile file = dbMetadata.getDFUFile(index);
			if(file != null) {
				List<String> output = new ArrayList<String>();
				for (Object column : getSortedPropertyValues(file.getNonKeyedColumns())) {
					output.add(column.toString());
				}
				return output;
			}
		}
		 
		return getNonKeyedColumnsForTempIndex(tableName);
	}
	
	public List<String> getKeyedColumnsForTempIndex(String tableName) {  //TODO: assure right order of keyed/non-keyed columns
		tableName = getFullTableName(tableName);
		HPCCDFUFile file = dbMetadata.getDFUFile(tableName);
		if(file != null && file.getTempIndex() != null) {
			return file.getTempIndex().getKeyedColumnNames();
		} 
		return null;
	}
	
	public List<String> getNonKeyedColumnsForTempIndex(String tableName) {
		tableName = getFullTableName(tableName);
		HPCCDFUFile file = dbMetadata.getDFUFile(tableName);
		if(file != null && file.getTempIndex() != null) {
			return file.getTempIndex().getNonKeyedColumnNames();
		} 
		return null;
	}
	
	//Properties are used with raw format
	@SuppressWarnings("unchecked")
	private List<Object> getSortedPropertyValues(Properties columns) {
		List<Comparable<Object>> sortedKeys = new ArrayList<Comparable<Object>>();
		for(Object key : columns.keySet()) {
			sortedKeys.add((Comparable<Object>) key);
		}
		Collections.sort(sortedKeys);
		List<Object> values = new ArrayList<Object>();
		for(Comparable<Object> key : sortedKeys) {
			values.add(columns.get(key));
		}
		return values;
	}
	
	public LinkedHashSet<String> getAllColumns(String table) {
		table = getFullTableName(table);
		HPCCDFUFile file = dbMetadata.getDFUFile(table);
		if(file != null) {
			List<String> list = Arrays.asList(file.getAllTableFieldsStringArray());
			return new LinkedHashSet<String>(list);
		}
		return null;	
	}
	
	public LinkedHashSet<String> getAllIndexColumns(String table, String index) {
		LinkedHashSet<String> set = new LinkedHashSet<String>();
		List<String> keyed = getKeyedColumns(table, index);
		List<String> nonKeyed = getNonKeyedColumns(table, index);

		if (keyed != null) {
			set.addAll(keyed);
		}
		if (nonKeyed != null) {
			set.addAll(nonKeyed);
		}
		
		return set;	
	}

	
	private boolean isInt(HPCCColumnMetaData column) {	
		return column.getEclType().toString().toLowerCase().matches("(unsigned.*|integer.*)");
	}
	
	public boolean isColumnOfIntInAnyTable(Set<String> tables, String column) {
		for (String table : tables) {
			table = getFullTableName(table);
			HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
			for(String field : dfuFile.getAllTableFieldsStringArray()){
				if(!field.equalsIgnoreCase(column)) continue; 
				if (isInt(dfuFile.getFieldMetaData(field))) return true;
			}
		}
		return false;
	}
	
	public String getFullTableName(String tableName) {
		Matcher matcher = Pattern.compile("(~?(\\w+)::)?([\\w\\-]+)", Pattern.CASE_INSENSITIVE).matcher(tableName);
		String schema = this.getPublicSchema();
		if (matcher.find()) {
			if (matcher.group(2) != null) {
				schema = matcher.group(2);
			}
			tableName = matcher.group(3);
		}
		return schema+"::"+tableName.toLowerCase();
	}
	
	public String getLayout(String tableName) {
		String name = getFullTableName(tableName);
		String layout = tableName.toLowerCase()+"_record";
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		
		if (dfuFile != null) {
			return dfuFile.getFileRecDef(layout);
		} 
		return null;
	}
	
	public static int getSqlType(String dataType) {
		String eclType = dataType.toLowerCase().replaceAll("(\\D*).*", "$1");
		switch (eclType) {
		case "integer":
		case "unsigned":
			return java.sql.Types.INTEGER;
		case "timestamp":
			return java.sql.Types.TIMESTAMP;
		default: 
			return java.sql.Types.VARCHAR;
		}
	}
	
	public List<String> getListOfIndexes(String tableName) {
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(getFullTableName(tableName));
		if (dfuFile != null) {
			return dfuFile.getRelatedIndexesList();
		}
    	return null;
    }
	
	public boolean hasIndex(String tableName) {
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(getFullTableName(tableName));
		if (dfuFile != null) {
			return dfuFile.hasRelatedIndexes();
		}
		return false;
	}
	
	public boolean allHaveNoIndex() {
		return false;
	}

	public HPCCDFUFile getDFUFile(String hpccfilename) {
		return dbMetadata.getDFUFile(hpccfilename);
	}

	public void removeDFUFile(String hpccfilename) {
		dbMetadata.removeDFUFile(hpccfilename);
	}
	
	public String checkForTempTable(String tablePath) {
    	if (isTempTable(tablePath)) {
    		tablePath = getFullTempTableName(tablePath);
    	}
    	return tablePath;
    }
	
	public String getFullTempTableName(String tableName) {
		return this.dbMetadata.getTableWithSessionID(tableName);
	}
	
	public String getShortTempTableName(String tableName) {
		String[] fullTempTableName = getFullTempTableName(tableName).split("::");
		if (fullTempTableName.length > 1) {
			return fullTempTableName[1];
		}
		return fullTempTableName[0];
	}

	public String getRecord(String tableName) {
		String name = getFullTableName(tableName);
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		
		if (dfuFile != null) {
			return dfuFile.getRecDef();
		} else {
			return null;
		}
	}

	public void setKeyedColumnsForTempIndex(String tableName, List<String> keyedColumns) {
		String name = getFullTableName(tableName);
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		
		TempIndex index = new TempIndex();
		if (dfuFile != null) {
			if (dfuFile.getTempIndex() != null) {
				index = dfuFile.getTempIndex();
			}
			List<Tuple<String, String>> keyed = new ArrayList<Tuple<String, String>>();
			for (String column : keyedColumns) {
				keyed.add(new Tuple<String, String>(column.toString(), getECLDataType(tableName, column)));
			}
			index.setKeyedColumns(keyed);
			dfuFile.setTempIndex(index);
		}
	}

	public void setNonKeyedColumnsForTempIndex(String tableName, List<String> nonKeyedColumns) {
		String name = getFullTableName(tableName);
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		
		TempIndex index = new TempIndex();
		if (dfuFile != null) {
			if (dfuFile.getTempIndex() != null) {
				index = dfuFile.getTempIndex();
			}
			List<Tuple<String, String>> nonKeyed = new ArrayList<Tuple<String, String>>();
			for (String column : nonKeyedColumns) {
				nonKeyed.add(new Tuple<String, String>(column.toString(), getECLDataType(tableName, column)));
			}
			
			index.setNonKeyedColumns(nonKeyed);
			dfuFile.setTempIndex(index);
		}
	}
}
