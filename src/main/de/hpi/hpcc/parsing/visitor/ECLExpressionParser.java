package de.hpi.hpcc.parsing.visitor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.select.ECLSelectParser;
import de.hpi.hpcc.parsing.select.plainSelect.SQLParserPlainSelect;
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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ECLExpressionParser implements ExpressionVisitor, FromItemVisitor {

	protected String parsed = "";
	private Set<String> allTables;
	protected ECLLayouts eclLayouts;
	
	public ECLExpressionParser(ECLLayouts eclLayouts) {
		this.eclLayouts = eclLayouts;
	}

	public String parse(Expression expression) {
		expression.accept(this);
		return parsed;
	}

	private String parse(FromItem fromItem) {
		fromItem.accept(this);
		return parsed;
	}
	
	public void setAllTables(Set<String> allTables) {
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
		int i = 1;
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
		parsed = Double.toString(doubleValue.getValue());
	}

	@Override
	public void visit(LongValue longValue) {
		parsed = Long.toString(longValue.getValue());
	}

	@Override
	public void visit(DateValue dateValue) {
		parsed = dateValue.getValue().toString();
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
		String stringEscaped = stringValue.getValue();
		stringEscaped = stringEscaped.replace("\n", "");
		stringEscaped = stringEscaped.replace("\\", "\\\\");
		stringEscaped = stringEscaped.replace("\'\'", "\'");
		stringEscaped = stringEscaped.replace("\'", "\\'");
		stringEscaped = ECLUtils.encapsulateWithSingleQuote(stringEscaped);
		parsed = stringEscaped;
	}

	@Override
	public void visit(Addition addition) {
		parsed = visitBinaryExpression(addition, "+");
	}

	@Override
	public void visit(Division division) {
		parsed = visitBinaryExpression(division, "/");
	}

	@Override
	public void visit(Multiplication multiplication) {
		parsed = visitBinaryExpression(multiplication, "*");
	}

	@Override
	public void visit(Subtraction subtraction) {
		if (subtraction.getLeftExpression().toString().equalsIgnoreCase("CURRENT_DATE")) {
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			Calendar currentDate = Calendar.getInstance();
			// = dateFormat.format(date); //2014/08/06 15:59:48
			IntervalExpression interval = (IntervalExpression) subtraction.getRightExpression(); 
			String parameter = interval.getParameter();
			Matcher matcher = Pattern.compile("'(\\d+)\\.?\\d*\\s(\\w*)'").matcher(parameter);
			if (matcher.find()) {
				int number = Integer.parseInt(matcher.group(1));
				String type = matcher.group(2);
				currentDate.add(Calendar.DAY_OF_MONTH, -number);
			}
			parsed = ECLUtils.encapsulateWithSingleQuote(dateFormat.format(currentDate.getTime()));
			return;
		}
		
		parsed = visitBinaryExpression(subtraction, "-");
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
		ECLSelectParser selectParser = new ECLSelectParser(eclLayouts);
		parsed = ECLUtils.encapsulateWithBrackets(selectParser.parse(subSelect.getSelectBody()));
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
		ECLSelectParser selectVisitor = new ECLSelectParser(eclLayouts);
		//TODO: avoid casting, support exist with union
		SQLParserPlainSelect subParser = (SQLParserPlainSelect) selectVisitor.findParser(((SubSelect)existsExpression.getRightExpression()).getSelectBody());
		
		StringBuilder existString = new StringBuilder();
		//TODO:
		if(subParser.getSelectItems().size() == 1) {
			if (subParser.getSelectItems().get(0).toString().equalsIgnoreCase("1 as long_1")) {
				if (subParser.getFromItem() instanceof SubSelect) {
					existString.append(parse(subParser.getFromItem()));
				} else if (subParser.getFromItem() instanceof Table) {
					existString.append("EXISTS(");
					existString.append(parse(subParser.getFromItem()));
					ECLExpressionParserExists parser = new ECLExpressionParserExists(eclLayouts);
					String where = parser.parse(subParser.getWhere());
					existString.append(ECLUtils.encapsulateWithBrackets(where));
					existString.append(")");
				}
			}
		} else {		
			existString.append(parse(existsExpression.getRightExpression()));		
		}
		if (existsExpression.isNot()) {
			parsed = "NOT " + existString.toString();
		} else {
			parsed = existString.toString();
		}
		
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
		switch(aexpr.getName().toLowerCase()) {
		// TODO: better (need to implement a counter in order to enumerate the tuples)
		case "row_number": parsed = "0"; break;
		default: break;
		}
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
		parsed = "";
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

	@Override
	public void visit(Table tableName) {
		parsed = tableName.getName();
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesList valuesList) {
		// TODO Auto-generated method stub
		
	}

}
