package de.hpi.hpcc.parsing.create;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SQLParserCreate extends SQLParser {
	
	private CreateTable create;

	public SQLParserCreate(CreateTable statement, ECLLayouts eclLayouts) {
		super(eclLayouts);
		this.create = statement;
	}
	
	public String getTableName() {
		return create.getTable().getName();
	}
	
	public boolean isTempTable() {
		CreateTable create = (CreateTable) getStatement();
		List<String> createOptions = create.getCreateOptionsStrings();
		if (createOptions != null && HPCCJDBCUtils.containsStringCaseInsensitive(createOptions, "temp")) {
			return true;
		}
		return false;
	}
	
	public String getRecord() {
		List<ColumnDefinition> columns = create.getColumnDefinitions();
		String records = "";
		for(ColumnDefinition column : columns) {
			records += (records == ""?"":", ");
			records += parseDataType(column.getColDataType().toString())+" "+column.getColumnName();
		}
		return records;
	}

	private String parseDataType(String dataType) {
		dataType = dataType.toLowerCase();
		int charLength = 0;
		int precision = 0;
		int scale = 0;
		boolean hasScale = false;
		
		Matcher matcher = Pattern.compile("(varchar|character\\s+varying)\\s*\\(\\s*(\\d+\\s*)\\)", Pattern.CASE_INSENSITIVE).matcher(dataType);
		if (matcher.find()) {
			dataType = matcher.group(1);
			charLength = Integer.parseInt(matcher.group(2));
		} 
		matcher = Pattern.compile("(numeric)\\s*\\(\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*\\))?", Pattern.CASE_INSENSITIVE).matcher(dataType);
		if (matcher.find()) {
			dataType = matcher.group(1);
			precision = Integer.parseInt(matcher.group(2));
			if (matcher.group(4) != null) {
				scale = Integer.parseInt(matcher.group(4));
				hasScale = true;
			}
		}
		switch(dataType) {
		case "serial": 
		case "int": 
		case "integer": return "INTEGER5";
		case "bigint": 
		case "bigserial": return "INTEGER8";
		case "text": return "STRING";
		case "numeric": 
			String eclDataType = "DECIMAL"+precision;
			eclDataType += hasScale?"_"+scale : "";
			return eclDataType;
		case "varchar":
		case "character varying": return "STRING"+charLength;
		case "timestamp": return "STRING25";
		default: return "unknown";
		}
	}

	public String getFullName() {
		return this.eclLayouts.getPublicSchema()+"::"+getTableName();
	}

	@Override
	public Statement getStatement() {
		return create;
	}
}
