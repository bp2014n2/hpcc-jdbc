package de.hpi.hpcc.parsing;

public class ECLColumnDefinition {
	private String dataType;
	private String columnName;
	
	public ECLColumnDefinition(String dataType, String columnName) {
		super();
		this.dataType = dataType;
		this.columnName = columnName;
	}
	
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String toString() {
		return this.dataType+" "+columnName;
	}
}
