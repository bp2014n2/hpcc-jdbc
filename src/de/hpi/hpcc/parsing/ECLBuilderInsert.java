package de.hpi.hpcc.parsing;

import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.main.HPCCJDBCUtils;

public class ECLBuilderInsert extends ECLBuilder {
	public ECLBuilderInsert(HPCCDatabaseMetaData dbMetadata) {
		super(dbMetadata);
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
		sqlParser = new SQLParserInsert(sql);
		
		StringBuilder eclCode = new StringBuilder();
		
		if (sqlParser.hasWith()) {
			for (WithItem withItem : sqlParser.getWithItemsList()) {
				eclCode.append(withItem.getName()+" := ");
				eclCode.append(new ECLBuilderSelect(dbMetadata).generateECL(withItem.getSelectBody().toString())+";\n");
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
		if (sqlParser.getColumns() == null || sqlParser.getColumns().size() == ECLLayouts.getAllColumns(sqlParser.getTable().getName(), dbMetadata).size()) {
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
				eclCode.append(new ECLBuilderSelect(dbMetadata).generateECL(sqlParser.getSelect().getSelectBody().toString()));
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
					columnString += ECLLayouts.getECLDataType(sqlParser.getTable().getName(), column, dbMetadata)+" ";
					
					columnString += column;
				}
				eclCode.append(columnString + "})");
			}
			eclCode.append(",{");
			LinkedHashSet<String> allColumns = ECLLayouts.getAllColumns(sqlParser.getTable().getName(), dbMetadata);
			String tableColumnString = "";
			for(String tableColumn : allColumns){
				tableColumnString += (tableColumnString=="" ? "":", ");
				if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumnNames(), tableColumn)) {
					tableColumnString += tableColumn;
				} else {
					String dataType = ECLLayouts.getECLDataType(sqlParser.getTable().getName(), tableColumn, dbMetadata);
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
