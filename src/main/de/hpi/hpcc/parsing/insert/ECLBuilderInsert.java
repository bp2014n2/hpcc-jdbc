package de.hpi.hpcc.parsing.insert;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;

public class ECLBuilderInsert extends ECLBuilder {
	
	SQLParserInsert sqlParser;
	private Insert insert;
	
	
	public ECLBuilderInsert(Insert insert, ECLLayouts eclLayouts) {
		super(insert, eclLayouts);
		this.insert = insert;
	}
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL() {
		sqlParser = new SQLParserInsert(insert, eclLayouts);
		
		eclCode = new StringBuilder();
		
		if (sqlParser.hasWith()) {
			for (WithItem withItem : sqlParser.getWithItemsList()) {
				eclCode.append(withItem.getName()+" := ");
				eclCode.append(new ECLBuilderSelect(withItem.getSelectBody(), eclLayouts).generateECL()+";\n");
			}
		}

		eclCode.append("OUTPUT(");
		try {
			generateNewDataset();
		} catch (HPCCException e) {
			return null;
		}
		eclCode.append(",,'~%NEWTABLE%', overwrite);\n");
		
		return eclCode.toString();
	}

	private void generateNewDataset() throws HPCCException {
		List<Column> columns = sqlParser.getColumns();
		LinkedHashSet<String> allColumns = eclLayouts.getAllColumns(sqlParser.getTable().getName());
		if (allColumns == null) {
			throw new HPCCException();
		}
		if (columns == null || (allColumns != null && columns.size() == allColumns.size())) {
			if (sqlParser.getItemsList() instanceof SubSelect) {
				eclCode.append(parseExpressionECL((Expression) sqlParser.getItemsList()).toString());
			} else if (sqlParser.getSelect() != null) {
				eclCode.append(new ECLBuilderSelect(sqlParser.getSelect(), eclLayouts).generateECL());
			} else {
				eclCode.append("DATASET([{");
				List<String> columnNames = sqlParser.getColumnNames();
				LinkedHashSet<String> orderedColumns = eclLayouts.getAllColumns(sqlParser.getTable().getName());
				String valueString = "";
				List<Expression> expressions = sqlParser.getExpressions();
				for (String column : orderedColumns) {
					for (int i = 0; i < columnNames.size(); i++) {
						if (!columnNames.get(i).equalsIgnoreCase(column)) continue;
						valueString += (valueString=="" ? "":", ")+parseExpressionECL(expressions.get(i));
					}
				}
				eclCode.append(valueString);
				eclCode.append("}], ");
				eclCode.append(sqlParser.getTable().getName()+"_record)");
			}
		} else {
			eclCode.append("TABLE(");
			List<String> columnNames = sqlParser.getColumnNames();
			
			List<String> tableColumnStrings = new ArrayList<String>();
			if (sqlParser.getSelect() != null) {
				eclCode.append(new ECLBuilderSelect(sqlParser.getSelect(), eclLayouts).generateECL());
				ECLSelectItemFinder itemFinder = new ECLSelectItemFinder(eclLayouts);
				List<SelectExpressionItem> selectItems = itemFinder.find(sqlParser.getSelect());
				
				for(String tableColumn : allColumns){
					int indexOfElement = HPCCJDBCUtils.indexOfCaseInsensitive(sqlParser.getColumnNames(), tableColumn);
					if (indexOfElement != -1) {
						SelectExpressionItem sei = selectItems.get(indexOfElement);
						Alias alias = sei.getAlias();
						String dataType = eclLayouts.getECLDataType(sqlParser.getTable().getName(), tableColumn);
						String tableColumnString = dataType+" "+tableColumn+" := ";
						if (alias != null) {
							tableColumnString += alias.getName();
						} else {
							ECLNameParser parser = new ECLNameParser();
							tableColumnString += parser.name(sei.getExpression());
						}
						tableColumnStrings.add(tableColumnString);
					} else {
						String dataType = eclLayouts.getECLDataType(sqlParser.getTable().getName(), tableColumn);
						tableColumnStrings.add(dataType+" "+tableColumn+" := "+(dataType.startsWith("UNSIGNED")||dataType.startsWith("integer")?"0":"''"));
					}
				}
				
			} else if (sqlParser.getItemsList() != null) {
				eclCode.append("DATASET(");
				List<String> valueStrings = new ArrayList<String>();
				for (Expression expression : sqlParser.getExpressions()) {
					valueStrings.add(parseExpressionECL(expression));
				}
				eclCode.append(ECLUtils.encapsulateWithSquareBrackets(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(valueStrings, ", "))));
				eclCode.append(", ");
				List<String> columnStrings = new ArrayList<String>();
				for(String column : columnNames){
					columnStrings.add(eclLayouts.getECLDataType(sqlParser.getTable().getName(), column)+" "+column);
				}
				eclCode.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(columnStrings, ", ")) + ")");
				
				for(String tableColumn : allColumns){
					if (HPCCJDBCUtils.containsStringCaseInsensitive(sqlParser.getColumnNames(), tableColumn)) {
						tableColumnStrings.add(tableColumn);
					} else {
						String dataType = eclLayouts.getECLDataType(sqlParser.getTable().getName(), tableColumn);
						tableColumnStrings.add(dataType+" "+tableColumn+" := "+(dataType.startsWith("UNSIGNED")||dataType.startsWith("integer")?"0":"''"));
					}
				}
			}
			eclCode.append(",");
			eclCode.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(tableColumnStrings, ", ")))
				.append(")");
		}
	}
	

	@Override
	protected Insert getStatement() {
		return insert;
	}	
}
