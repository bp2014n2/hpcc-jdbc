package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
	
	public ECLLayouts (HPCCDatabaseMetaData dbMetadata) {
		this.dbMetadata = dbMetadata;
	}
	
	public String getPublicSchema() {
		return this.dbMetadata.getPublicSchema();
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
			return columnMeta.getEclType();
		}
		return ""; //TODO: why not null?
	}
	
	/**
	 * returns 
	 * @param index
	 * @return
	 */
	public List<Object> getKeyedColumns(String index) {  //TODO: assure right order of keyed/non-keyed columns
		index = getFullTableName(index);
		HPCCDFUFile file = dbMetadata.getDFUFile(index);
		if(file != null) {
			return getSortedPropertyValues(file.getKeyedColumns());
		} 
		return null;
		
	}
	
	public List<Object> getNonKeyedColumns(String index) {
		index = getFullTableName(index);
		HPCCDFUFile file = dbMetadata.getDFUFile(index);
		if(file != null) {
			return getSortedPropertyValues(file.getNonKeyedColumns());
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
	
//	public String getLayoutOrdered(String table, List<String> orderedColumns) {
//		String name = getFullTableName(table);
//		StringBuilder layout = new StringBuilder(table.toLowerCase()+"_record := RECORD ");
//		
//		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
//		List<Object> fields = Collections.list(dfuFile.getAllFields());
//		
//		for (String columnName : orderedColumns) {
//			for (Object field : fields) {
//				HPCCColumnMetaData column = (HPCCColumnMetaData) field;
//				if (!column.getColumnName().equalsIgnoreCase(columnName)) {
//					continue;
//				}
//				layout.append(column.getEclType());
//				layout.append(" ");
//				layout.append(column.getColumnName());
//				layout.append("; ");
//			}
//		}
//		layout.append("END;");
//		
//		return layout.toString();
//	}

}
