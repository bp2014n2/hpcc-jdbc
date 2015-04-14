package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.parsing.ECLRecordDefinition;

public class ECLLayouts {
     
	/**
	 * is only used within tests to add test layouts
	 * @param key is the name of the corresponding table
	 * @param value is the layout definition itself
	 */
	
	public static String getECLDataType(String table, String column, HPCCDatabaseMetaData dbMetadata){
		//ECLRecordDefinition recordDefinition = getLayout(table);
		HPCCColumnMetaData columnMeta = dbMetadata.getDFUFile(table).getFieldMetaData(column);
		
		return columnMeta.getColumnType().toString();
		//return recordDefinition.findColumn(column).getDataType();
	}
	
	public static LinkedHashSet<String> getAllColumns(String table, HPCCDatabaseMetaData dbMetadata) {
		//ECLRecordDefinition recordDefinition = getLayouts(table);
//		if (recordDefinition == null) return null;
//		return recordDefinition.getColumnNames();
		List<String> list = Arrays.asList(dbMetadata.getDFUFile(table).getAllTableFieldsStringArray());
		return new LinkedHashSet<String>(list);
	}

	
	public static boolean isInt(HPCCColumnMetaData column) {	
		if (column.getColumnType().toString().toLowerCase().matches("(unsigned.*|integer.*)")) {
			return true;
		}
		return false;
	}
	
	public static boolean isColumnOfIntInAnyTable(List<String> tables, String column, HPCCDatabaseMetaData dbMetadata) {
		for (String table : tables) {
			HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
			for(String field : dfuFile.getAllTableFieldsStringArray()){
				if(!field.equalsIgnoreCase(column)) {
					continue;
				}
				if (isInt(dfuFile.getFieldMetaData(field))) return true;
			}
		}
		return false;
	}
	
	public static int getSqlTypeOfColumn (List<String> tables, String column, HPCCDatabaseMetaData dbMetadata) {
		for (String table : tables) {
			HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
			for(String field : dfuFile.getAllTableFieldsStringArray()){
				if(!field.equalsIgnoreCase(column)) {
					continue;
				}
				return getSqlType(dfuFile.getFieldMetaData(field).getColumnType().toString());
			}
		}
		return java.sql.Types.OTHER;
	}
	
	public static String getLayout(String tableName, HPCCDatabaseMetaData dbMetadata) {
		String name = "i2b2demodata::"+tableName;
		String layout = tableName.toLowerCase()+"_record";
		
		String recordDefinition = dbMetadata.getDFUFile(name).getFileRecDef(layout);
				
		
		return recordDefinition;
		
		
	}
	
	public static int getSqlType(String dataType) {
		String eclType = dataType.toLowerCase().replaceAll("(\\D*).*", "$1");
		switch (eclType) {
		case "integer":
			return java.sql.Types.INTEGER;
		case "unsigned":
			return java.sql.Types.INTEGER;
		case "timestamp":
			return java.sql.Types.TIMESTAMP;
		default: 
			return java.sql.Types.VARCHAR;
		}
	}

	public static Object getLayoutOrdered(String table,
			HPCCDatabaseMetaData dbMetadata, List<String> orderedColumns) {
		// TODO Auto-generated method stub
		return null;
	}

}
