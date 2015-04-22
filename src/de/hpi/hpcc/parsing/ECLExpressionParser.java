package de.hpi.hpcc.parsing;

import java.util.List;

import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.PostgreSQLFromForExpression;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLExpressionParser implements ExpressionVisitor {

	private String parsed;
	private List<String> allTables;
	private ECLLayouts eclLayouts;
	
	public ECLExpressionParser(ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}

	public String parse(Expression expression) {
		expression.accept(this);
		return parsed;
	}
	
	public void setAllTables(List<String> allTables) {
		this.allTables = allTables;
	}
	
	

	@Override
	public void visit(NullValue nullValue) {
		parsed = "''";	
	}

	@Override
	public void visit(Function function) {
		StringBuilder expressionBuilder = new StringBuilder();

		expressionBuilder.append(parseFunction(function));
		parsed = expressionBuilder.toString();
		
	}
	
	private String parseFunction(Function function) {
		String functionName = function.getName().toUpperCase();
		switch(functionName) {
		case "SUM": return parseSum(function);
		case "SUBSTRING": return parseSubstring(function);
		default: return parseDefaultFunction(function);
		}
	}

	private String parseSum(Function function) {
		String parameters = "";
		if (function.getName().toUpperCase().equals("SUM")) {
			if (function.getParameters().getExpressions().size() > 0) {
				for (Expression e : function.getParameters().getExpressions()) {
					if (e instanceof Column) parameters += ", " + parse(e);
				}
			}
		}
		return "SUM"+ECLUtils.encapsulateWithBrackets("GROUP"+parameters);
	}

	private String parseSubstring(Function function) {
		PostgreSQLFromForExpression fromFor = (PostgreSQLFromForExpression) function.getParameters().getExpressions().get(0);
		String column = parse(fromFor.getSourceExpression());
		int start = Integer.parseInt(parse(fromFor.getFromExpression()));
		int end = start + Integer.parseInt(parse(fromFor.getForExpression())) - 1;
		return column + "[" + start + ".." + end + "]";
	}

	private String parseDefaultFunction(Function function) {
		return function.getName().toUpperCase()+ECLUtils.encapsulateWithBrackets("GROUP");
	}

	@Override
	public void visit(SignedExpression signedExpression) {
		parsed = signedExpression.toString();
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
		parsed = "?";
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue longValue) {
		parsed = Long.toString(longValue.getValue());
	}

	@Override
	public void visit(DateValue dateValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue timeValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parsed = ECLUtils.encapsulateWithBrackets(parse(parenthesis.getExpression()));
	}

	@Override
	public void visit(StringValue stringValue) {
		parsed = ECLUtils.encapsulateWithSingleQuote(stringValue.getValue());
	}

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(AndExpression andExpression) {
		parsed = visitBinaryExpression(andExpression, "AND");
	}

	@Override
	public void visit(OrExpression orExpression) {
		parsed = visitBinaryExpression(orExpression, "OR");
	}

	@Override
	public void visit(Between between) {
		StringBuilder betweenBuilder = new StringBuilder();
		betweenBuilder.append(parse(between.getLeftExpression()));
		betweenBuilder.append(" BETWEEN ");
		betweenBuilder.append(parse(between.getBetweenExpressionStart()));
		betweenBuilder.append(" AND ");
		betweenBuilder.append(parse(between.getBetweenExpressionEnd()));
		parsed = ECLUtils.encapsulateWithBrackets(betweenBuilder.toString());
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		parsed = visitBinaryExpression(equalsTo, "=");
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		parsed = visitBinaryExpression(greaterThan, ">");
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		parsed = visitBinaryExpression(greaterThanEquals, ">=");
	}

	@Override
	public void visit(InExpression inExpression) {
		StringBuilder expressionBuilder = new StringBuilder();
		
		String inColumn = parse(inExpression.getLeftExpression());
		expressionBuilder.append(inColumn);
		expressionBuilder.append(" IN ");
		ECLItemsListParser itemsListParser = new ECLItemsListParser(eclLayouts);
		itemsListParser.setInColumn(inColumn);
		expressionBuilder.append(itemsListParser.parse(inExpression.getRightItemsList()));
		
		parsed = expressionBuilder.toString();
		
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		StringBuilder nullExpressionString = new StringBuilder();
		
		nullExpressionString.append(parse(isNullExpression.getLeftExpression()));
		nullExpressionString.append(" = "+((eclLayouts.isColumnOfIntInAnyTable(allTables, parse(isNullExpression.getLeftExpression())))?"0":"''"));
		parsed = nullExpressionString.toString();
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		StringBuilder likeString = new StringBuilder();
		String stringValue = ((StringValue) likeExpression.getRightExpression()).getValue();
		if (stringValue.endsWith("%")) {
			stringValue = stringValue.replace("%", "");
		}
		int count = stringValue.replace("\\\\", "\\").length();
		
		
		likeString.append(parse(likeExpression.getLeftExpression()));
		likeString.append("[1..");
		likeString.append(count);
		likeString.append("] = '");
		likeString.append(stringValue);
		likeString.append("\'");
		parsed = likeString.toString();
		
	}

	@Override
	public void visit(MinorThan minorThan) {
		parsed = visitBinaryExpression(minorThan, "<");
		
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		parsed = visitBinaryExpression(minorThanEquals, "<=");
		
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		parsed = visitBinaryExpression(notEqualsTo, "!=");
		
	}

	@Override
	public void visit(Column tableColumn) {
		parsed = tableColumn.getColumnName();
		
	}

	@Override
	public void visit(SubSelect subSelect) {
		ECLUtils.encapsulateWithBrackets(new ECLBuilderSelect(eclLayouts).generateECL((subSelect).getSelectBody().toString()));
	}

	@Override
	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		SQLParserSelect subParser = new SQLParserSelect(((SubSelect)existsExpression.getRightExpression()).getSelectBody().toString(), eclLayouts);	

		StringBuilder existString = new StringBuilder();

		if(subParser.getSelectItems().size() == 1) {
			if(subParser.getSelectItems().get(0).toString().equals("1")) {
				if(subParser.getFromItem() instanceof SubSelect) {
					existString.append(parse((Expression)subParser.getFromItem()));
				}
			}
		} else {
			existString.append(parse(existsExpression.getRightExpression()));
		}
		
		parsed = existString.toString();
		
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CastExpression cast) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Modulo modulo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithinGroupExpression wgexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression eexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntervalExpression iexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression oexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator rexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression jsonExpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator regExpMySQLOperator) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable var) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind bind) {
		// TODO Auto-generated method stub
		
	}
	
	private String visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
        StringBuilder expression = new StringBuilder();
		if (binaryExpression.isNot()) {
			expression.append(" NOT ");
        }
	
		expression.append(parse(binaryExpression.getLeftExpression()));
        expression.append(" " + operator + " ");
        expression.append(parse(binaryExpression.getRightExpression()));
        
        return expression.toString();

    }

	@Override
	public void visit(PostgreSQLFromForExpression postgreSQLFromForExpression) {
		// TODO Auto-generated method stub
		
	}

}
