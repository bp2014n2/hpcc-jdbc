package de.hpi.hpcc.parsing.create;

import java.io.StringReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SQLParserCreate extends SQLParser {

	public SQLParserCreate(String sql, ECLLayouts eclLayouts) {
		super(sql, eclLayouts);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof CreateTable) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	public String getTableName() {
		return ((CreateTable) statement).getTable().getName();
	}
	
	public String getRecord() {
		List<ColumnDefinition> columns = ((CreateTable) statement).getColumnDefinitions();
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
		return "i2b2demodata::"+getTableName();
	}
}
