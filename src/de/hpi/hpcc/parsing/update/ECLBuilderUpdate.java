package de.hpi.hpcc.parsing.update;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import net.sf.jsqlparser.expression.Expression;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;

public class ECLBuilderUpdate extends ECLBuilder {

	
	SQLParserUpdate sqlParser;
	
	
	public ECLBuilderUpdate(ECLLayouts eclLayouts) {
		super(eclLayouts);
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserUpdate(sql, eclLayouts);
		eclCode = new StringBuilder();
//		String tableName = sqlParser.getAllTables().get(0).toLowerCase();
//		StringBuilder transformFunction = new StringBuilder();
//		String transformResultType = tableName + "_record";
//		transformFunction.append(transformResultType + " update(" + transformResultType + " l, " + transformResultType + " r) := TRANSFORM\n");
//		for (String col : eclLayouts.getAllColumns(tableName)) {
//			if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumns(),col)) {
//				transformFunction.append("SELF." + col + " := IF(r." + col +" = " + sqlParser.isInt(col)?"0":"''" + ", l." + col + ", r." + col +");\n");
//			} else {
//				
//			}
//		}
		
		StringBuilder preSelection = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			Expression expression = sqlParser.getWhere();
			
			preSelection.append(parseExpressionECL(expression));
			preSelection = ECLUtils.encapsulateWithBrackets(preSelection);
		}
		
		eclCode.append("toUpdate := ");
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
		updateTable.append(ECLUtils.encapsulateWithCurlyBrackets(selectString));
		
		updateTable = ECLUtils.convertToTable(updateTable);
		updateTable.append(", ");

		updateTable.append("{");
		String tableColumnString = "";
		for(String column : allColumns){
			
			tableColumnString += (tableColumnString.equals("")? "":", ");
			if (HPCCJDBCUtils.containsStringCaseInsensitive(columns, column)) {
				String expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(column.toLowerCase())).toString();
				expr = expr.equals("NULL")? "''" : expr;
				tableColumnString += eclLayouts.getECLDataType(sqlParser.getName(), column)+" "+column+" := "+expr;
			} else {
				tableColumnString += column;
			}
			
		}
		updateTable.append(tableColumnString);
		updateTable.append("}");
		
		updateTable = ECLUtils.convertToTable(updateTable);
		updateTable.append(";\n");
		eclCode.append(updateTable.toString());
		
		eclCode.append("OUTPUT(");
		StringBuilder outputTable = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			eclCode.append(sqlParser.getName());
			Expression expression = sqlParser.getWhere();
			outputTable.append("(NOT");
			outputTable.append(ECLUtils.encapsulateWithBrackets(parseExpressionECL(expression)));
			outputTable.append(")+");
		}

		outputTable.append("toUpdate,, '~%NEWTABLE%', overwrite);\n");
		eclCode.append(outputTable.toString());
		
		return eclCode.toString();
	}

	@Override
	protected SQLParserUpdate getSqlParser() {
		// TODO Auto-generated method stub
		return sqlParser;
	}
}