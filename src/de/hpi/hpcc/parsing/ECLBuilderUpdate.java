package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import net.sf.jsqlparser.expression.Expression;
import de.hpi.hpcc.main.HPCCJDBCUtils;

public class ECLBuilderUpdate extends ECLBuilder {

	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		SQLParserUpdate sqlParser = new SQLParserUpdate(sql);
		StringBuilder eclCode = new StringBuilder();
		
		StringBuilder preSelection = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			Expression expression = sqlParser.getWhere();
			
			preSelection.append("(");
			preSelection.append(parseExpressionECL(expression));
			preSelection.append(")");			
		}
		
		
		eclCode.append("updates := ");
		StringBuilder updateTable = new StringBuilder();
		updateTable.append(sqlParser.getName());
		updateTable.append(preSelection.toString());
		updateTable.append(", ");
		ArrayList<String> columns = (ArrayList<String>) sqlParser.getColumns();
		LinkedHashSet<String> allColumns = sqlParser.getAllCoumns();
		String selectString = "";
		for(String column : allColumns){
			if (!HPCCJDBCUtils.containsStringCaseInsensitive(columns, column) || sqlParser.isIncrement()) {
				selectString += (selectString=="" ? "":", ");
				selectString += column;
			}
		}
		updateTable.append("{");
		updateTable.append(selectString);
		updateTable.append("}");
		
		convertToTable(updateTable);
		updateTable.append(", ");

		updateTable.append("{");
		String tableColumnString = "";
		for(String column : allColumns){
			
			tableColumnString += (tableColumnString.equals("")? "":", ");
			if (HPCCJDBCUtils.containsStringCaseInsensitive(columns, column)) {
				String expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(column)).toString();
				expr = expr.equals("NULL")? "''" : expr;
				tableColumnString += ECLLayouts.getECLDataType(sqlParser.getName(), column)+" "+column+" := "+expr;
			} else {
				tableColumnString += column;
			}
			
		}
		updateTable.append(tableColumnString);
		updateTable.append("}");
		
		convertToTable(updateTable);
		updateTable.append(";\n");
		eclCode.append(updateTable.toString());
		
		eclCode.append("OUTPUT(");
		StringBuilder outputTable = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			eclCode.append(sqlParser.getName());
			Expression expression = sqlParser.getWhere();
			outputTable.append("(NOT");
			outputTable.append("(");
			
			outputTable.append(parseExpressionECL(expression));
			outputTable.append("))+");
		}

		outputTable.append("updates,, '~%NEWTABLE%', overwrite);\n");
		eclCode.append(outputTable.toString());
		
		return eclCode.toString();
	}
}