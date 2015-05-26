package de.hpi.hpcc.parsing.update;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.select.SQLParserPlainSelect;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.select.SQLParserSelectVisitor;
import de.hpi.hpcc.parsing.visitor.ECLExpressionParser;
import de.hpi.hpcc.parsing.ECLUtils;

public class ECLBuilderUpdate extends ECLBuilder {

	SQLParserUpdate sqlParser;
	private Update update;

	public ECLBuilderUpdate(Update update, ECLLayouts eclLayouts) {
		super(update, eclLayouts);
		this.update = update;
	}

	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL() {
		sqlParser = new SQLParserUpdate(update, eclLayouts);

		StringBuilder eclCode = new StringBuilder();
		
		String tableName = sqlParser.getName();
		String fullTableName = sqlParser.getFullName();
		Set<String> tableList = new HashSet<String>();
		tableList.add(fullTableName);

		Expression exist = sqlParser.getExist(sqlParser.getWhere());
		//TODO: check for empty exist
		if (exist != null) {
			SQLParserSelectVisitor selectVisitor = new SQLParserSelectVisitor(eclLayouts);
			
			//TODO: avoid casting
			SQLParserPlainSelect subParser = (SQLParserPlainSelect) selectVisitor.find(((SubSelect)((ExistsExpression) exist).getRightExpression()).getSelectBody());
			
			Expression where = subParser.getWhere();
			String joinColumn = null;
			if(where instanceof EqualsTo) {
				String left = ((EqualsTo)where).getLeftExpression().toString();
				String right = ((EqualsTo)where).getRightExpression().toString();
				if(right.contains(".") && left.contains(".")) {
					if(!right.substring(0,right.indexOf(".") + 1).equals(left.substring(0,left.indexOf(".") + 1)) 
					&& right.substring(right.indexOf(".") + 1).equals(left.substring(left.indexOf(".") + 1))) {
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
			String updateString = eclLayouts.getECLDataType(tableName, col)+" "+col+" := "+expr;
			String existExpression = parseExpressionECL(exist);
			joinTable.append(existExpression)
				.append(", "+ECLUtils.encapsulateWithCurlyBrackets(joinColumn + ", " + updateString));
			String joinTableString = ECLUtils.convertToTable(joinTable.toString());
			
			eclCode.append("OUTPUT(JOIN(")
				.append(tableName + ECLUtils.encapsulateWithBrackets(preSelection) + ", " + joinTableString + ", ")
				.append("LEFT." + joinColumn + " = RIGHT." + joinColumn + ", ")
				.append("update(LEFT, RIGHT), LEFT OUTER) + " + tableName + "(NOT " + preSelection.toString() + "),,'~%NEWTABLE%',OVERWRITE");
			if(eclLayouts.isTempTable(update.getTables().get(0).getName())) {
				eclCode.append(", "+expireString);
			}
			eclCode.append(");\n");
		} else {
			StringBuilder updateTable = new StringBuilder();
			
			eclCode.append("toUpdate := ");
			updateTable.append(sqlParser.getName());
			updateTable.append(generatePreSelection());
			updateTable.append(", ");
			ArrayList<String> columns = (ArrayList<String>) sqlParser.getColumns();
			LinkedHashSet<String> allColumns = sqlParser.getAllCoumns();
			List<String> selectStrings = new ArrayList<String>();
			for(String column : allColumns){
				if (!HPCCJDBCUtils.containsStringCaseInsensitive(columns, column) || sqlParser.isIncrement()) {
					selectStrings.add(column);
				}
			}
			updateTable.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(selectStrings, ", ")));
			updateTable = ECLUtils.convertToTable(updateTable);
			updateTable.append(", ");

			List<String> tableColumnStrings = new ArrayList<String>();
			for(String column : allColumns){
				if (HPCCJDBCUtils.containsStringCaseInsensitive(columns, column)) {
					Expression expr = sqlParser.getExpressions().get(sqlParser.getColumnsToLowerCase().indexOf(column.toLowerCase()));
					ECLExpressionParser parser = new ECLExpressionParser(eclLayouts);
					tableColumnStrings.add(eclLayouts.getECLDataType(sqlParser.getName(), column)+" "+column+" := "+parser.parse(expr));
				} else {
					tableColumnStrings.add(column);
				}
			}
			updateTable.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(tableColumnStrings, ", ")));
			
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

			outputTable.append("toUpdate,, '~%NEWTABLE%', overwrite");
			if(eclLayouts.isTempTable(update.getTables().get(0).getName())) {
				outputTable.append(", "+expireString);
			}
			outputTable.append(");\n");
			eclCode.append(outputTable.toString());
		}
		
		outputCount++;
		return eclCode.toString();
	}

	@Override
	protected Update getStatement() {
		return update;
	}
	
	private String generatePreSelection() {
		StringBuilder preSelection = new StringBuilder();
		if (sqlParser.getWhere() != null) {
			Expression expression = sqlParser.getWhere();
			
			preSelection.append(parseExpressionECL(expression));
			preSelection = ECLUtils.encapsulateWithBrackets(preSelection);
		}
		return preSelection.toString();
	}
}
