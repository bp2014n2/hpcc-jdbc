package de.hpi.hpcc.parsing.update;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.select.SQLParserSelect;

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
		StringBuilder eclCode = new StringBuilder();
		
		String tableName = sqlParser.getAllTables().get(0).toLowerCase();
		String fullTableName = tableName.contains(".") ? tableName.replace(".", "::") : "i2b2demodata::" + tableName;
		ArrayList<String> tableList = new ArrayList<String>();
		tableList.add(fullTableName);

		Expression exist = sqlParser.getExist(sqlParser.getWhere());
		SQLParserSelect subParser = new SQLParserSelect(((SubSelect)((ExistsExpression) exist).getRightExpression()).getSelectBody().toString(), eclLayouts);
		
		Expression where = subParser.getWhere();
		String joinColumn = null;
		if(where instanceof EqualsTo) {
			String left = ((EqualsTo)where).getLeftExpression().toString();
			String right = ((EqualsTo)where).getRightExpression().toString();
			if(right.contains(".") && left.contains(".")) {
				if(!right.substring(0,right.indexOf(".") + 1).equals(left.substring(0,left.indexOf(".") + 1)) 
				&& right.substring(right.indexOf(".") + 1).equals(left.substring(left.indexOf(".") + 1))){
					joinColumn = right.substring(right.indexOf(".") + 1);
				}
			}
		}

		StringBuilder joinRecord = new StringBuilder();
		String updateColumn = sqlParser.getColumns().get(0);
		joinRecord.append("join_record := RECORD ")
			.append(eclLayouts.getECLDataType(tableName, joinColumn)+" "+joinColumn+"; ")
			.append(eclLayouts.getECLDataType(tableName, updateColumn)+" "+updateColumn+"; END;\n");
		eclCode.append(joinRecord.toString());
		
		StringBuilder transformFunction = new StringBuilder();
		String transformResultType = tableName + "_record";
		transformFunction.append(transformResultType + " update(" + transformResultType + " l, " + "join_record r) := TRANSFORM\n");
		for (String col : eclLayouts.getAllColumns(tableName)) {
			if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumns(),col)) {
				transformFunction.append("  SELF." + col + " := IF(r." + col +" = ")
					.append(eclLayouts.isColumnOfIntInAnyTable(tableList, col)?"0":"''")
					.append(", l." + col + ", r." + col +");\n");
			} else {
				transformFunction.append("  SELF." + col + " := l." + col + ";\n");
			}
		}
		transformFunction.append("END;\n");
		eclCode.append(transformFunction.toString());
		
		StringBuilder preSelection = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			preSelection.append(parseExpressionECL(sqlParser.getWhereWithoutExists()));
		}
		
		StringBuilder joinTable = new StringBuilder();
		String col = sqlParser.getColumns().get(0);
		String expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(col.toLowerCase())).toString();
		expr = expr.equals("NULL")? (eclLayouts.isColumnOfIntInAnyTable(tableList, col) ? "0" : "''") : expr;
		String update = eclLayouts.getECLDataType(tableName, col)+" "+col+" := "+expr;
		joinTable.append("TABLE(")
			.append(parseExpressionECL(exist))
			.append(", {")
			.append(joinColumn + ", ")
			.append(update + "})");
		
		
//		String tableColumnString = "";
//		for (String col : eclLayouts.getAllColumns(tableName)) {
//			tableColumnString += (tableColumnString.equals("")) ? "" : ", ";
//			if (HPCCJDBCUtils.containsStringCaseInsensitive(selectItems, col)) {
//				tableColumnString += col;
//			} else if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumns(),col)) {
//				String expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(col.toLowerCase())).toString();
//				expr = expr.equals("NULL")? (eclLayouts.isColumnOfIntInAnyTable(tableList, col) ? "0" : "''") : expr;
//				tableColumnString += eclLayouts.getECLDataType(tableName, col)+" "+col+" := "+expr;
//			} else {
//				tableColumnString += eclLayouts.getECLDataType(tableName, col)+" "+col+" := "+(eclLayouts.isColumnOfIntInAnyTable(tableList, col) ? "0" : "''");
//			}
//		}
		
		eclCode.append("OUTPUT(JOIN(")
			.append(tableName + "(" +preSelection.toString() + "), " + joinTable.toString() + ", ")
			.append("LEFT." + joinColumn + " = RIGHT." + joinColumn + ", ")
			.append("update(LEFT, RIGHT), LEFT OUTER) + " + tableName + "(NOT " + preSelection.toString() + "),,'~%NEWTABLE%',OVERWRITE);");
		
		
		
//		eclCode.append("toUpdate := ");
//		StringBuilder updateTable = new StringBuilder();
//		updateTable.append(sqlParser.getName());
//		updateTable.append(preSelection.toString());
//		updateTable.append(", ");
//		ArrayList<String> columns = (ArrayList<String>) sqlParser.getColumns();
//		LinkedHashSet<String> allColumns = sqlParser.getAllCoumns();
//		String selectString = "";
//		for(String column : allColumns){
//			if (!HPCCJDBCUtils.containsStringCaseInsensitive(columns, column) || sqlParser.isIncrement()) {
//				selectString += (selectString=="" ? "":", ");
//				selectString += column;
//			}
//		}
//		updateTable.append(encapsulateWithCurlyBrackets(selectString));
//		
//		convertToTable(updateTable);
//		updateTable.append(", ");
//
//		updateTable.append("{");
//		String tableColumnString = "";
//		for(String column : allColumns){
//			
//			tableColumnString += (tableColumnString.equals("")? "":", ");
//			if (HPCCJDBCUtils.containsStringCaseInsensitive(columns, column)) {
//				String expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(column.toLowerCase())).toString();
//				expr = expr.equals("NULL")? "''" : expr;
//				tableColumnString += eclLayouts.getECLDataType(sqlParser.getName(), column)+" "+column+" := "+expr;
//			} else {
//				tableColumnString += column;
//			}
//			
//		}
//		updateTable.append(tableColumnString);
//		updateTable.append("}");
//		
//		convertToTable(updateTable);
//		updateTable.append(";\n");
//		eclCode.append(updateTable.toString());
//		
//		eclCode.append("OUTPUT(");
//		StringBuilder outputTable = new StringBuilder();
//		if (sqlParser.getWhere() != null) {
//			eclCode.append(sqlParser.getName());
//			Expression expression = sqlParser.getWhere();
//			outputTable.append("(NOT");
//			outputTable.append(encapsulateWithBrackets(parseExpressionECL(expression)));
//			outputTable.append(")+");
//		}
//
//		outputTable.append("updates,, '~%NEWTABLE%', overwrite);\n");
//		eclCode.append(outputTable.toString());
		
		return eclCode.toString();
	}

	@Override
	protected SQLParserUpdate getSqlParser() {
		// TODO Auto-generated method stub
		return sqlParser;
	}
}
