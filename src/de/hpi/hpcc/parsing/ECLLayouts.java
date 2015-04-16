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
		table = getFullTableName(table);
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
		if (dfuFile != null) {
			HPCCColumnMetaData columnMeta = dfuFile.getFieldMetaData(column);
			return columnMeta.getEclType();
		}
		return "";
	}
	
	public LinkedHashSet<String> getAllColumns(String table) {
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

	
	private boolean isInt(HPCCColumnMetaData column) {	
		return column.getEclType().toString().toLowerCase().matches("(unsigned.*|integer.*)");
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
			table = getFullTableName(table);
			HPCCDFUFile dfuFile = dbMetadata.getDFUFile(table);
			if (dfuFile == null) {
				return java.sql.Types.OTHER;
			}
			for(String field : dfuFile.getAllTableFieldsStringArray()){
				if(!field.equalsIgnoreCase(column)) {
					continue;
				}
				return getSqlType(dfuFile.getFieldMetaData(field).getEclType().toString());
			}
		}
		return java.sql.Types.OTHER;
	}
	
	private String getFullTableName(String tableName) {
		if (tableName.startsWith("i2b2demodata::")) {
			return tableName.toLowerCase();
		} else {
			return "i2b2demodata::"+tableName.toLowerCase();
		}
	}
	
	public String getLayout(String tableName) {
		String name = getFullTableName(tableName);
		String layout = tableName.toLowerCase()+"_record";
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		
		if (dfuFile != null) {
			return dfuFile.getFileRecDef(layout);
		} else {
			return null;
		}
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
		StringBuilder layout = new StringBuilder(table.toLowerCase()+"_record := RECORD ");
		
		HPCCDFUFile dfuFile = dbMetadata.getDFUFile(name);
		List<Object> fields = Collections.list(dfuFile.getAllFields());
		
		for (String columnName : orderedColumns) {
			for (Object field : fields) {
				HPCCColumnMetaData column = (HPCCColumnMetaData) field;
				if (!column.getColumnName().equals(columnName)) {
					continue;
				}
				layout.append(column.getEclType());
				layout.append(" ");
				layout.append(column.getColumnName());
				layout.append("; ");
			}
		}
		layout.append("END;");
		
		return layout.toString();
	}

}
