package de.hpi.hpcc.parsing;

public class ECLColumnDefinition {
	private String dataType;
	private String columnName;
	/* TODO: 
	 * replace with HPCCColumnMetaData
	 */
	public int getSqlType() {
		String eclType = this.dataType.toLowerCase().replaceAll("(\\D*).*", "$1");
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
