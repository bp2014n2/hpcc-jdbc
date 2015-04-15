package de.hpi.hpcc.parsing;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.main.HPCCDFUFile;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;

public class ECLLayouts {
     
	/**
	 * is only used within tests to add test layouts
	 * @param key is the name of the corresponding table
	 * @param value is the layout definition itself
	 */
	
	HPCCDatabaseMetaData dbMetadata;
	
	public ECLLayouts (HPCCDatabaseMetaData dbMetadata) {
		this.dbMetadata = dbMetadata;
	}
	
	public String getECLDataType(String table, String column){
		HPCCColumnMetaData columnMeta = dbMetadata.getDFUFile(table).getFieldMetaData(column);
		return columnMeta.getColumnType().toString();
	}
	
	public LinkedHashSet<String> getAllColumns(String table) {
		//ECLRecordDefinition recordDefinition = getLayouts(table);
//		if (recordDefinition == null) return null;
//		return recordDefinition.getColumnNames();
		table = getFullTableName(table);
		HPCCDFUFile file = dbMetadata.getDFUFile(table);
		if(file != null) {
			List<String> list = Arrays.asList(file.getAllTableFieldsStringArray());
			return new LinkedHashSet<String>(list);
		} else {
			//throw new Exception("DFUFile not found: "+table);
			return null;
		}
		
	}

	
	public boolean isInt(HPCCColumnMetaData column) {	
		if (column.getColumnType().toString().toLowerCase().matches("(unsigned.*|integer.*)")) {
			return true;
		}
		return false;
	}
	
	public boolean isColumnOfIntInAnyTable(List<String> tables, String column) {
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
	
	public int getSqlTypeOfColumn (List<String> tables, String column) {
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
	
	private String getFullTableName(String tableName) {
		if (tableName.startsWith("i2b2demodata::")) {
			return tableName;
		} else {
			return "i2b2demodata::"+tableName;
		}
	}
	
	public String getLayout(String tableName) {
		String name = getFullTableName(tableName);
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

	public String getLayoutOrdered(String table, List<String> orderedColumns) {
		String name = getFullTableName(table);
		String layout = table.toLowerCase()+"_record";
		
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		List<Object> fields = Collections.list(dfuFile.getAllFields());
		
		for (Object field : fields) {
			HPCCColumnMetaData column = dfuFile.getFieldMetaData(field.toString());
		}
		/*
		 * TODO: implementation
		 */
		
		return null;
	}

}
