package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;

public class ECLBuilder {
	private boolean hasAlias = false;
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		
	/*  TODO: CHECK FOR:
	 * 		UPDATE	
	 */
		switch(SQLParser.sqlIsInstanceOf(sql)) {
    	case "Select":
    		return generateSelectECL(new SQLParserSelect(sql));
		case "Insert":
    		return generateInsertECL(new SQLParserInsert(sql));
    	case "Update":
    		return generateUpdateECL(new SQLParserUpdate(sql));
    	case "Drop":
    		return generateDropECL(new SQLParserDrop(sql));
    	case "Create":
    		return generateCreateECL(new SQLParserCreate(sql));
		default:
    		System.out.println("type of sql not recognized "+SQLParser.sqlIsInstanceOf(sql));
    	}
		return null;
	}
	
	
	private String generateCreateECL(SQLParserCreate sqlParser) {
		StringBuilder eclCode = new StringBuilder();
		String tableName = ((SQLParserCreate) sqlParser).getTableName();
		eclCode.append("OUTPUT(DATASET([],{");
		//remove "RECORD " at beginning of Layout definition
		String recordString = ECLLayouts.getLayouts().get(tableName.toLowerCase());
		if(recordString == null) {
			recordString = ((SQLParserCreate) sqlParser).getRecord();
		} else {
			recordString = recordString.substring(7, recordString.length() - 6).replace(";", ",");
		}
		eclCode.append(recordString);
		eclCode.append("}),,'~%NEWTABLE%',OVERWRITE);");
		return eclCode.toString();
	}


	private String generateUpdateECL(SQLParserUpdate sqlParser) {
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
			if (!columns.contains(column)) {
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
			
			tableColumnString += (tableColumnString=="" ? "":", ");
			tableColumnString += (columns.contains(column)?ECLLayouts.getECLDataType(sqlParser.getName(), column)+" "+column+" := "+sqlParser.getExpressions().get(sqlParser.getColumns().indexOf(column)):column);
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


	private String generateDropECL(SQLParserDrop sqlParser) {
		StringBuilder eclCode = new StringBuilder();
		eclCode.append("Std.File.DeleteLogicalFile('~"+sqlParser.getFullName().replace(".", "::")+"', true)");
		
		return eclCode.toString();
	}


	/**
	 * Generates the ECL code for a insert statement 
	 * @return returns the ECL code for the given insert statement
	 */
	private String generateInsertECL(SQLParserInsert sqlParser) {
		StringBuilder eclCode = new StringBuilder();
		
		if (sqlParser.hasWith()) {
			for (WithItem withItem : sqlParser.getWithItemsList()) {
				eclCode.append(withItem.getName()+" := ");
				eclCode.append(new ECLBuilder().generateECL(withItem.getSelectBody().toString())+";\n");
			}
		}
//		create subfile
//		"%2B" is +
		eclCode.append("OUTPUT(");
		generateNewDataset(sqlParser, eclCode);
		eclCode.append(",,'~%NEWTABLE%', overwrite);\n");
		
		return eclCode.toString();
	}

	private void generateNewDataset(SQLParserInsert sqlParser, StringBuilder eclCode) {
		if (sqlParser.isAllColumns()) {
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
				eclCode.append(generateECL(sqlParser.getSelect().getSelectBody().toString()));
			} else if (sqlParser.getItemsList() != null) {
				eclCode.append("(DATASET([{");
				String valueString = "";
				for (Expression expression : sqlParser.getExpressions()) {
					valueString += (valueString=="" ? "":", ")+parseExpressionECL(expression);
				}
				eclCode.append(valueString);
				eclCode.append("}], {");
				String columnString = "";
				for(String column : columns){
					columnString += (columnString=="" ? "":", ");
					columnString += ECLLayouts.getECLDataType(sqlParser.getTable().getName(), column);
					columnString += " "+column;
				}
				eclCode.append(columnString + "})");
			}
			eclCode.append(",{");
			LinkedHashSet<String> allColumns = sqlParser.getAllCoumns();
			String tableColumnString = "";
			for(String column : allColumns){
				String dataType = ECLLayouts.getECLDataType(sqlParser.getTable().getName(), column);
				tableColumnString += (tableColumnString=="" ? "":", ");
				tableColumnString += (columns.contains(column)?column:dataType+" "+column+" := "+(dataType.startsWith("UNSIGNED")||dataType.startsWith("integer")?"0":"''"));
			}
			eclCode.append(tableColumnString)
				.append("})");
		}
	}
	/**
	 * Generates the ECL code for a select statement
	 * @return returns the ECL code for the given select statement
	 */
	
	private String generateSelectECL(SQLParserSelect sqlParser) {
		StringBuilder eclCode = new StringBuilder();
		
    	generateFrom(sqlParser, eclCode);
    	generateWhere(sqlParser, eclCode);    	
    	generateSelects(sqlParser, eclCode, true);
    	generateGroupBys(sqlParser, eclCode);
    	 
    	if (sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null) {
    		if (!((SQLParserSelect) sqlParser).isCount()) convertToTable(eclCode);
    	}
    	generateOrderBys(sqlParser, eclCode);
    	if(sqlParser.getOrderBys() != null) {
    		generateSelects(sqlParser, eclCode, false);
    		if (sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null) {
        		convertToTable(eclCode);
        	}
    	}
    	generateDistinct(sqlParser, eclCode);
    	generateLimit(sqlParser, eclCode);
    	
    	return(eclCode.toString());
	}
	
	private void convertToTable(StringBuilder eclCode) {
   		eclCode.insert(0, "TABLE(");
   		eclCode.append(")");
	}
	
	private void generateLimit(SQLParserSelect sqlParser, StringBuilder eclCode) {
		Limit limit = sqlParser.getLimit();
		if (limit != null) {
			eclCode.insert(0, "CHOOSEN(");
			eclCode.append(", ");
			eclCode.append(limit.getRowCount());
			eclCode.append(")");
    	}
	}
	
	private void generateDistinct(SQLParserSelect sqlParser, StringBuilder eclCode) {
		if (sqlParser.isDistinct()) {
			eclCode.insert(0, "DEDUP(");
			eclCode.append(", All)");
    	}
	}
	
	private void generateGroupBys(SQLParserSelect sqlParser, StringBuilder eclCode) {
		List<Expression> groupBys = sqlParser.getGroupBys(); 
		if (groupBys != null) {
			for (Expression expression : groupBys) {
				eclCode.append(", ");
				eclCode.append(parseExpressionECL(expression));
			}
		}
	}
	
	private void generateOrderBys(SQLParserSelect sqlParser, StringBuilder eclCode) {
		List<OrderByElement> orderBys = sqlParser.getOrderBys(); 	
    	if (orderBys != null) {
    		eclCode.insert(0, "SORT(");
    		eclCode.append(", ");
    		for (OrderByElement orderByElement : orderBys) {
    			/*  
    			 * TODO: multiple orderByElements
    			 * TODO: order by expression (e.g. count)
    			 */
    			Expression orderBy = orderByElement.getExpression();
    			if (orderBy instanceof Function) {
    				eclCode.append(nameFunction((Function) orderBy));
    			} else {
    				eclCode.append(parseExpressionECL(orderByElement.getExpression()));
    			}
    		}
    		eclCode.append(")");
    	}
	}
	
	private void generateFrom(SQLParserSelect sqlParser, StringBuilder from) {
		FromItem table = sqlParser.getFromItem();
		if (table instanceof Table) {
			from.append(((Table) table).getName());
    	} else if (table instanceof SubSelect){
    		from.append("(");
    		String innerStatement = sqlParser.trimInnerStatement(table.toString());
    		from.append(new ECLBuilder().generateECL(innerStatement));
    		from.append(")");
    	}
	}
	
	private void generateWhere(SQLParserSelect sqlParser, StringBuilder where) {
		Expression whereItems = sqlParser.getWhere();
    	if (whereItems != null) {
    		where.append("(");
    		where.append(parseExpressionECL(whereItems));
    		where.append(")");
    	}
	}
	
	/**
	 * Generates the SelectItems between "SELECT" and "FROM" in a SQL query.
	 * If a "SELECT * FROM" is used 
	 * 
	 * @return
	 */
	
	private void generateSelects(SQLParserSelect sqlParser, StringBuilder select, Boolean inner) { 
		if (sqlParser.isCount()) {
			select.insert(0, "COUNT(");
			select.append(")");
		} else {
			LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
			select.append(", ");
			select.append("{");
			if(inner) {
				if (sqlParser.getOrderBys() != null) {
					for (OrderByElement orderByElement : sqlParser.getOrderBys()) {
						Expression orderBy = orderByElement.getExpression();
						if (orderBy instanceof Function) {
							StringBuilder function = new StringBuilder();
							function.append(nameFunction((Function) orderBy));
							function.append(" := ");
							function.append(parseFunction((Function) orderBy));
							selectItemsStrings.add(function.toString());
						} else {
							selectItemsStrings.add(parseExpressionECL(orderByElement.getExpression()));
						}
					}
				}	
			}
    	
			if (sqlParser.isSelectAll()){
				if(sqlParser.getFromItem() instanceof SubSelect) {
					selectItemsStrings.addAll(sqlParser.getFromItemColumns());
				} else {
					selectItemsStrings.addAll(sqlParser.getAllColumns());
				}
			} else {
				ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
				for (SelectItem selectItem : selectItems) {
					if (selectItem instanceof SelectExpressionItem) {
						StringBuilder selectItemString = new StringBuilder();
						if (((SelectExpressionItem) selectItem).getAlias() != null) {
							setHasAlias(true);
							if ((Expression) ((SelectExpressionItem) selectItem).getExpression() instanceof LongValue) {
			   					selectItemString.append("INTEGER1 "); //necessary when parsing  for example "0 as panel_count"
			   				}
			   				selectItemString.append(((SelectExpressionItem) selectItem).getAlias().getName());
		   					selectItemString.append(" := ");
		   				}
		   				selectItemString.append(parseExpressionECL((Expression) ((SelectExpressionItem) selectItem).getExpression()));
		   				selectItemsStrings.add(selectItemString.toString());
		   			}
	    		}
	    	}
	    	String selectItemString = "";
    		for (String selectItem : selectItemsStrings) {
    			selectItemString += (selectItemString=="" ? "":", ")+selectItem;
    		}
    		select.append(selectItemString);
    		select.append("}");
		}
	}
	
	/**
	 * Generates for a given Expression the ECL code by a recursive approach
	 * Each Expression can be e.g. a BinaryExpression (And, or etc. ), but also a Column or a SubSelect.
	 * The break condition is accomplished if the Expression is a Column, Long or String
	 * @param expressionItem is the Expression under consideration
	 * @return returns the ECL code for the given expression
	 */
	private String parseExpressionECL(Expression expressionItem) {
		StringBuilder expression = new StringBuilder();
		
		if (expressionItem instanceof LikeExpression) {
			expression.append(parseLikeExpression((LikeExpression) expressionItem));
		} else if (expressionItem instanceof BinaryExpression) {
			expression.append(parseExpressionECL(((BinaryExpression) expressionItem).getLeftExpression()));
			expression.append(getSymbolOfExpression((BinaryExpression) expressionItem));
			expression.append(parseExpressionECL(((BinaryExpression) expressionItem).getRightExpression()));
		} else if (expressionItem instanceof InExpression) {
			expression.append(parseInExpression((InExpression) expressionItem));
		} else if (expressionItem instanceof Column) {
			if (((Column) expressionItem).getFullyQualifiedName() != null) {
//				expression.append(((Column) expressionItem).getTable().getName());
//				expression.append(".");
			}
			expression.append(((Column) expressionItem).getColumnName());
		} else if (expressionItem instanceof SubSelect) {
			expression.append("(");
			expression.append(parseSubSelect(expressionItem));
			expression.append(")");
		} else if (expressionItem instanceof Function) {
			if (!hasAlias()) {
				expression.append(nameFunction((Function) expressionItem));
				expression.append(" := ");
			}
			expression.append(parseFunction((Function) expressionItem));
		}  else if (expressionItem instanceof Between) {
			expression.append("(");
			expression.append(parseExpressionECL(((Between) expressionItem).getLeftExpression()));
			expression.append(" BETWEEN ");
			expression.append(parseExpressionECL(((Between) expressionItem).getBetweenExpressionStart()));
			expression.append(" AND ");
			expression.append(parseExpressionECL(((Between) expressionItem).getBetweenExpressionEnd()));
			expression.append(")");
		} else if (expressionItem instanceof StringValue) {
			expression.append("'");
			expression.append(((StringValue) expressionItem).getValue());
			expression.append("'");
		} else if (expressionItem instanceof LongValue) {
			expression.append(((LongValue) expressionItem).getValue());
		} else if (expressionItem instanceof IsNullExpression) {
			expression.append(parseExpressionECL(((IsNullExpression) expressionItem).getLeftExpression()));
			expression.append(" = "+((ECLLayouts.isInt(((Column)((IsNullExpression) expressionItem).getLeftExpression()).getColumnName()))?"0":"''"));
		} else if (expressionItem instanceof ExistsExpression) {		
			SQLParserSelect subParser = new SQLParserSelect(((SubSelect)((ExistsExpression) expressionItem).getRightExpression()).getSelectBody().toString());	
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
			expression.append(joinColumn+" IN SET(");
			if(subParser.getSelectItems().size() == 1) {
				if(subParser.getSelectItems().get(0).toString().equals("1"))
					if(subParser.getFromItem() instanceof SubSelect)
				expression.append(parseExpressionECL((Expression)subParser.getFromItem()));
			} else
				expression.append(parseExpressionECL(((ExistsExpression) expressionItem).getRightExpression()));
			expression.append(", "+joinColumn+")");
		} else if (expressionItem instanceof Parenthesis) {
			expression.append("("+parseExpressionECL(((Parenthesis) expressionItem).getExpression())+")");
		} else if (expressionItem instanceof JdbcParameter) {
			expression.append("?");
		} else if (expressionItem instanceof NullValue) {
				expression.append("''");
		}
		hasAlias();
		return expression.toString();
	}
	
	private boolean hasAlias() {
		if(hasAlias) {
			setHasAlias(false);
			return true;
		}
		return false;
	}


	private void setHasAlias(boolean b) {
		hasAlias = b;
	}


	private String parseSubSelect(Expression expression) {
		return new ECLBuilder().generateECL(((SubSelect) expression).getSelectBody().toString());
	}
	
	/**
	 * Parses the function and generates ECL code
	 * @param function can be e.g. an object representing "COUNT" or "AVG"
	 * @return returns the ECL for the given function
	 */

	private String parseFunction(Function function) {	
		return function.getName().toUpperCase()+"(GROUP)";
	}
	
	private String nameFunction(Function function) {
		StringBuilder innerFunctionString = new StringBuilder();
		if (!function.isAllColumns()) {
			for (Expression expression : function.getParameters().getExpressions()) {
				innerFunctionString.append(parseExpressionECL(expression));
			}
		}
		innerFunctionString.insert(0, function.getName().toLowerCase() + "_");
		return innerFunctionString.toString();
	}

	/**
	 * Parses a LikeExpression and generates ECL code
	 * @param expressionItem 
	 * @return returns the ECL for the given function
	 */
	
	private String parseLikeExpression(LikeExpression expressionItem) {
		StringBuilder likeString = new StringBuilder();
		String stringValue = ((StringValue) expressionItem.getRightExpression()).getValue();
		if (stringValue.endsWith("%")) stringValue = stringValue.replace("%", "");
		int count = stringValue.length();
		stringValue = stringValue.replace("\\", "\\\\");
		likeString.append("");
		likeString.append(parseExpressionECL(expressionItem.getLeftExpression()));
		likeString.append("[");
		
		likeString.append("1");
		likeString.append("..");
		likeString.append(count);
		likeString.append("] = '");
		likeString.append(stringValue);
		likeString.append("\'");
		return likeString.toString();
	}
	
	private String parseInExpression(InExpression expressionItem) {
		StringBuilder expression = new StringBuilder();
		String inColumn = parseExpressionECL(((InExpression) expressionItem).getLeftExpression());
		expression.append(inColumn);
		expression.append(" IN ");
		if (((InExpression) expressionItem).getRightItemsList() instanceof ExpressionList) {
			expression.append("[");
			for (Expression exp : ((ExpressionList) ((InExpression) expressionItem).getRightItemsList()).getExpressions()) {
				expression.append(parseExpressionECL(exp));
			expression.append("]");
			}
		} else if (((InExpression) expressionItem).getRightItemsList() instanceof SubSelect) {
			expression.append("SET(");
			expression.append(new ECLBuilder().generateECL(((SubSelect) ((InExpression) expressionItem).getRightItemsList()).getSelectBody().toString()));
			expression.append(","+inColumn+")");
		}
		return expression.toString();
	}

	private String getSymbolOfExpression(BinaryExpression whereItems) {
		if (whereItems instanceof AndExpression) {
			return " AND ";
		} else if (whereItems instanceof OrExpression) {
			return " OR ";
		} else if (whereItems instanceof MinorThan) {
			return " < ";
		} else if (whereItems instanceof MinorThanEquals) {
			return " <= ";
		} else if (whereItems instanceof GreaterThan) {
			return " > ";
		} else if (whereItems instanceof GreaterThanEquals) {
			return " >= ";
		} else if (whereItems instanceof EqualsTo) {
			return " = ";
		} else if (whereItems instanceof NotEqualsTo) {
			return " != ";
		} 
		return null;
	}
	
	private AndExpression findJoinCondition(AndExpression expression) {
		if (expression.getRightExpression() instanceof Column && expression.getLeftExpression() instanceof Column) return expression;
		if (expression.getRightExpression() instanceof AndExpression) return findJoinCondition(expression);
		if (expression.getLeftExpression() instanceof AndExpression) return findJoinCondition(expression);
		return null;
	}
	
	private Expression parseJoinCondition(SQLParserSelect sqlParser) {
		Expression where = sqlParser.getWhere();
		if (where instanceof EqualsTo) {
			sqlParser.setWhere(null);
			return where;
		} else if (where instanceof AndExpression) {
			
		}
		
		return null;
	}
}