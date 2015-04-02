package de.hpi.hpcc.parsing;

import java.util.LinkedHashSet;
import java.util.List;

public class ECLRecordDefinition {
	
	LinkedHashSet<ECLColumnDefinition> columns = new LinkedHashSet<ECLColumnDefinition>();
	
	public LinkedHashSet<ECLColumnDefinition> getColumns() {
		return columns;
	}

	public void setColumns(LinkedHashSet<ECLColumnDefinition> columns) {
		this.columns = columns;
	}

	public ECLRecordDefinition(String recordDefinition) {
		recordDefinition = recordDefinition.trim().substring(7, recordDefinition.length());
		int lastIndex = recordDefinition.lastIndexOf("END")-1;
		recordDefinition = recordDefinition.substring(0, lastIndex);
		
		String[] recordStrings = recordDefinition.split(";");
		
				
		for (String recordString : recordStrings) {
			recordString = recordString.trim();
			String dataType = recordString.split(" ")[0];
			String columnName = recordString.split(" ")[1];
			this.addColumn(new ECLColumnDefinition(dataType, columnName)); 
		}
	}
	
	public void addColumn(ECLColumnDefinition column) {
		this.columns.add(column);
	}
	
	public String toString() {
		StringBuilder recordDefinition = new StringBuilder("RECORD ");
		for (ECLColumnDefinition column : columns) {
			recordDefinition.append(column.toString()+"; ");
		}
		recordDefinition.append("END;");
		return recordDefinition.toString();
	}
	
	public String toString(List<String> columnNames) {
		StringBuilder recordDefinition = new StringBuilder("RECORD ");
		for (String column : columnNames) {
			ECLColumnDefinition matchedDefinition = findColumn(column);
			if (matchedDefinition != null) {
				recordDefinition.append(matchedDefinition.toString()+"; ");
			}			
		}
		recordDefinition.append("END;");
		return recordDefinition.toString();
	}
	
	public ECLColumnDefinition findColumn(String columnName) {
		for (ECLColumnDefinition definition : columns) {
			if (definition.getColumnName().toLowerCase().equals(columnName.toLowerCase())){
				return definition;
			}
		}
		return null;
	}
	
	public LinkedHashSet<String> getColumnNames() {
		LinkedHashSet<String> columnNames = new LinkedHashSet<String>();
		for (ECLColumnDefinition definition : columns) {
			columnNames.add(definition.getColumnName());
		}
		
		return columnNames;
	}
	
	
}
