package de.hpi.hpcc.parsing;

public class ECLColumnDefinition {
	private String eclDataType;
	public String getEclDataType() {
		return eclDataType;
	}

	public void setEclDataType(String eclDataType) {
		this.eclDataType = eclDataType;
	}

	public String getSqlDataType() {
		return sqlDataType;
	}

	public void setSqlDataType(String sqlDataType) {
		this.sqlDataType = sqlDataType;
	}

	private String sqlDataType;
	private String columnName;
	
	public ECLColumnDefinition(String sqlDataType, String columnName) {
		super();
		if (sqlDataType.contains("NOT NULL")) {
			sqlDataType = sqlDataType.toLowerCase().split("NOT NULL")[0];
		}
		this.sqlDataType = sqlDataType.trim();
		this.columnName = columnName;
		this.eclDataType = translateSQLTypeToECLType(this.sqlDataType);
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String toString() {
		return this.eclDataType+" "+columnName;
	}
	
	private String translateSQLTypeToECLType(String sqlType) {
		String numberOfChars = "";
		
		if (sqlType.contains("varying")) {
			String[] type = sqlType.split("\\(");
			sqlType = type[0].split(" ")[0];
			numberOfChars = type[1].substring(0, type[1].indexOf(")"));
		} else if (sqlType.contains("varchar") || sqlType.contains("numeric")) {
			String[] type = sqlType.split("\\(");
			sqlType = type[0];
			numberOfChars = type[1].substring(0, type[1].indexOf(")"));
		} else if (sqlType.contains("(")) {
			String[] type = sqlType.split("\\(");
			sqlType = type[0];
			numberOfChars = type[1].substring(0, type[1].indexOf(")"));
		}
		switch (sqlType.toLowerCase()) {
		case "serial": return "UNSIGNED5"; 
		case "character": return "STRING"+numberOfChars;
		case "integer": return "UNSIGNED5";
		case "integer not null": return "UNSIGNED5";
		case "text": return "STRING";
		case "timestamp without time zone": return "STRING25";
		case "timestamp without time zone not null": return "STRING25";
		case "timestamp": return "STRING25";
		case "varchar": return "STRING"+numberOfChars;
		case "int": 
			if(numberOfChars.equals("")) {
				return "UNSIGNED5";
			} else {
				return "UNSIGNED"+numberOfChars;
			}
		case "bigint": return "UNSIGNED5";
		case "numeric": return "decimal"+numberOfChars;
		}
		return null;
	}
}
