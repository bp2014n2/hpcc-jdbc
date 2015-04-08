package de.hpi.hpcc.parsing;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
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
import net.sf.jsqlparser.statement.select.SubSelect;

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
		switch(SQLParser.sqlIsInstanceOf(sql)) {
    	case "Select":
    		return new ECLBuilderSelect().generateECL(sql);
		case "Insert":
			return new ECLBuilderInsert().generateECL(sql);
    	case "Update":
    		return new ECLBuilderUpdate().generateECL(sql);
    	case "Drop":
    		return new ECLBuilderDrop().generateECL(sql);
    	case "Create":
    		return new ECLBuilderCreate().generateECL(sql);
		default:
    		System.out.println("type of sql not recognized "+SQLParser.sqlIsInstanceOf(sql));
    	}
		return null;
	}
	
	protected void convertToTable(StringBuilder eclCode) {
   		eclCode.insert(0, "TABLE(");
   		eclCode.append(")");
	}
	
	/**
	 * Generates for a given Expression the ECL code by a recursive approach
	 * Each Expression can be e.g. a BinaryExpression (And, or etc. ), but also a Column or a SubSelect.
	 * The break condition is accomplished if the Expression is a Column, Long or String
	 * @param expressionItem is the Expression under consideration
	 * @return returns the ECL code for the given expression
	 */
	protected String parseExpressionECL(Expression expressionItem) {
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
		} else if (expressionItem instanceof IsNullExpression) {
			expression.append(parseExpressionECL(((IsNullExpression) expressionItem).getLeftExpression()));
			expression.append(" = "+((ECLLayouts.isColumnOfIntInAnyTable(((Column)((IsNullExpression) expressionItem).getLeftExpression()).getColumnName()))?"0":"''"));
		} else if (expressionItem instanceof Parenthesis) {
			expression.append("("+parseExpressionECL(((Parenthesis) expressionItem).getExpression())+")");
		} else if (expressionItem instanceof JdbcParameter) {
			expression.append("?");
		} else if (expressionItem instanceof NullValue) {
				expression.append("''");
		} else if (expressionItem instanceof SignedExpression) {
			expression.append(((SignedExpression) expressionItem).toString());
		}
		hasAlias();
		return expression.toString();
	}
	
	protected boolean hasAlias() {
		boolean oldHasAlias = this.hasAlias;
		setHasAlias(false);
		return oldHasAlias;	
	}

	private void setHasAlias(boolean b) {
		hasAlias = b;
	}


	private String parseSubSelect(Expression expression) {
		return new ECLBuilderSelect().generateECL(((SubSelect) expression).getSelectBody().toString());
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
		int count = stringValue.replace("\\\\", "\\").length();
//		stringValue = stringValue.replace("\\", "\\\\");
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
}
