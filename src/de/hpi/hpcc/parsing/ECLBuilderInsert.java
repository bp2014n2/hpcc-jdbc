package de.hpi.hpcc.parsing;

import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import de.hpi.hpcc.main.HPCCJDBCUtils;

public class ECLBuilderInsert extends ECLBuilder {
	public ECLBuilderInsert(ECLLayouts eclLayouts) {
		super(eclLayouts);
		// TODO Auto-generated constructor stub
	}
	
	SQLParserInsert sqlParser;

	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserInsert(sql, eclLayouts);
		
		StringBuilder eclCode = new StringBuilder();
		
		if (sqlParser.hasWith()) {
			for (WithItem withItem : sqlParser.getWithItemsList()) {
				eclCode.append(withItem.getName()+" := ");
				eclCode.append(new ECLBuilderSelect(eclLayouts).generateECL(withItem.getSelectBody().toString())+";\n");
			}
		}

		eclCode.append("OUTPUT(");
		generateNewDataset(sqlParser, eclCode);
		eclCode.append(",,'~%NEWTABLE%', overwrite);\n");
		
		
		/*
			 * TODO: replace with much, much, much better solution
			 */
		eclCode.append("OUTPUT(DATASET([{1}],{unsigned1 dummy})(dummy=0));\n");
		return eclCode.toString();
	}

	private void generateNewDataset(SQLParserInsert sqlParser, StringBuilder eclCode) {
		if (sqlParser.getColumns() == null || sqlParser.getColumns().size() == eclLayouts.getAllColumns(sqlParser.getTable().getName()).size()) {
			if (sqlParser.getItemsList() instanceof SubSelect) {
			eclCode.append(parseExpressionECL((Expression) sqlParser.getItemsList()).toString());
			} else {
				eclCode.append("DATASET([{");
				String valueString = "";
				for (Expression expression : sqlParser.getExpressions()) {
					valueString += (valueString=="" ? "":", ")+parseExpressionECL(expression);
				}
				eclCode.append(valueString);
				eclCode.append("}], ");
				eclCode.append(sqlParser.getTable().getName()+"_record)");
			}
		} else {
			eclCode.append("TABLE(");
			List<String> columns = sqlParser.getColumnNames();
			if (sqlParser.getSelect() != null) {
				eclCode.append(new ECLBuilderSelect(eclLayouts).generateECL(sqlParser.getSelect().getSelectBody().toString()));
			} else if (sqlParser.getItemsList() != null) {
				eclCode.append("DATASET([{");
				String valueString = "";
				for (Expression expression : sqlParser.getExpressions()) {
					valueString += (valueString=="" ? "":", ")+parseExpressionECL(expression);
				}
				eclCode.append(valueString);
				eclCode.append("}], {");
				String columnString = "";
				for(String column : columns){
					columnString += (columnString=="" ? "":", ");
					columnString += eclLayouts.getECLDataType(sqlParser.getTable().getName(), column)+" ";
					
					columnString += column;
				}
				eclCode.append(columnString + "})");
			}
			eclCode.append(",{");
			LinkedHashSet<String> allColumns = eclLayouts.getAllColumns(sqlParser.getTable().getName());
			String tableColumnString = "";
			for(String tableColumn : allColumns){
				tableColumnString += (tableColumnString=="" ? "":", ");
				if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumnNames(), tableColumn)) {
					tableColumnString += tableColumn;
				} else {
					String dataType = eclLayouts.getECLDataType(sqlParser.getTable().getName(), tableColumn);
					tableColumnString += dataType+" "+tableColumn+" := "+(dataType.startsWith("UNSIGNED")||dataType.startsWith("integer")?"0":"''");
				}
			}
			eclCode.append(tableColumnString)
				.append("})");
		}
	}

	@Override
	protected SQLParserInsert getSqlParser() {
		// TODO Auto-generated method stub
		return sqlParser;
	}	
}
