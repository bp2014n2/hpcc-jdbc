package connectionManagement;

import java.io.StringReader;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SQLParserCreate extends SQLParser {

	protected SQLParserCreate(String sql) {
		super(sql);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof CreateTable) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected SQLParserCreate(Expression expression) {
		super(expression);
		// TODO Auto-generated constructor stub
	}
	
	protected SQLParserCreate(Statement statement) {
		super(statement);
		// TODO Auto-generated constructor stub
	}

	public String getTableName() {
		return ((CreateTable) statement).getTable().getName();
	}
	
	public String getRecord() {
		List<ColumnDefinition> columns = ((CreateTable) statement).getColumnDefinitions();
		String records = "";
		for(ColumnDefinition column : columns) {
			records += parseDataType(column.getColDataType().toString())+" "+column.getColumnName()+"; ";
		}
		return records;
	}

	private String parseDataType(String dataType) {
		dataType = dataType.toLowerCase();
		String newDataType = "";
		String charLength = "";
		if (dataType.matches(Pattern.quote("varchar (")+"[0-9]*"+Pattern.quote(")"))) {
			charLength = getCharLength(dataType);
			dataType = "varchar";
		}
		switch(dataType) {
		case "int": newDataType = "UNSIGNED5"; break;
		case "varchar": newDataType = "STRING"+charLength; break;
		case "timestamp": newDataType = "STRING25"; break;
		default: newDataType = "unknown";
		}
		return newDataType;
	}

	private String getCharLength(String dataType) {
		return dataType.substring(9, dataType.length()-1);
	}

	public String getFullName() {
		return "i2b2demodata::"+getTableName();
	}
}
