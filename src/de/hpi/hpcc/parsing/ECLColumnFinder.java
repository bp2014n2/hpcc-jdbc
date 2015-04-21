package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLColumnFinder implements ExpressionVisitor {

	private List<String> columns;
	private List<String> tableNameAndAlias;
	private ECLLayouts  eclLayouts;
	private String statement;

	public List<String> find(Expression expression) {
		expression.accept(this);
		return columns;
	}
	
	public ECLColumnFinder(ECLLayouts eclLayouts, String statement, List<String> tableNameAndAlias) {
		this.eclLayouts = eclLayouts;
		this.statement = statement;
		this.tableNameAndAlias = tableNameAndAlias;
	}

	@Override
	public void visit(NullValue nullValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function function) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SignedExpression signedExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
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
		columns = find(parenthesis.getExpression());
	}

	@Override
	public void visit(StringValue stringValue) {
		// TODO Auto-generated method stub
		
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
		visitBinaryExpression(andExpression);
	}

	@Override
	public void visit(OrExpression orExpression) {
		visitBinaryExpression(orExpression);
	}

	@Override
	public void visit(Between between) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		visitBinaryExpression(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		visitBinaryExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		visitBinaryExpression(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) {
		List<String> tmpcolumns = find(inExpression.getLeftExpression());
		if (inExpression.getRightItemsList() instanceof SubSelect) {
			//TODO: fix cast (Expression) ItemsList
			tmpcolumns.addAll(find((Expression) inExpression.getRightItemsList()));
		}
		columns = tmpcolumns;
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan minorThan) {
		visitBinaryExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpression(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpression(notEqualsTo);
	}

	@Override
	public void visit(Column tableColumn) {
		String columnName = tableColumn.getColumnName();
		String tableName = tableColumn.getTable().getName();
		List<String> tmpcolumns = new ArrayList<String>();
		if (tableName != null) {
			if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase()) && !tmpcolumns.contains(columnName)) {
				tmpcolumns.add(columnName);
			}
		} else {
			Pattern selectPattern = Pattern.compile("select\\s*(distinct\\s*)?((((count|sum|avg)\\(\\w*\\))|\\w*)\\s*,\\s*)*("+ columnName +"\\s*|(count|sum|avg)\\(\\s*"+ columnName +"\\s*\\))\\s*(as\\s*\\w*\\s*)?(,\\s*((count|sum|avg)\\(\\w*\\)|\\w*)\\s*(as\\s*\\w*\\s*)?)*from\\s*(\\w*\\.)?(\\w*)",Pattern.CASE_INSENSITIVE);
			Pattern wherePattern = Pattern.compile("from\\s*(\\w*\\.)?(\\w*)(\\s*\\w*)?\\s*where\\s*(\\(?(\\w*\\.)?\\w*\\s*((=|<=|>=)\\s*'?\\w*'?|in\\s*\\([\\w\\s\\\\'%\\.\\-]*\\))\\s*\\)?\\s*(and|or)\\s*)*\\(?" + columnName,Pattern.CASE_INSENSITIVE);
			Matcher selectMatcher = selectPattern.matcher(statement.toString());
			Matcher whereMatcher = wherePattern.matcher(statement.toString());
			if (selectMatcher.find()) {
				tableName = selectMatcher.group(14);
			} else if (whereMatcher.find()) {
				tableName = whereMatcher.group(2);
			}
			if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase())) {
				tmpcolumns.add(columnName);
			}
		}
		columns = tmpcolumns;
	}

	@Override
	public void visit(SubSelect subSelect) {
		SQLParserSelect selectParser = new SQLParserSelect(subSelect.getSelectBody().toString(), eclLayouts);
		for (SelectItem selectItem : selectParser.getSelectItems()) {
			columns.addAll(find(((SelectExpressionItem) selectItem).getExpression()));
		}
		if (selectParser.getWhere() != null) {
			columns.addAll(find((Expression) selectParser.getWhere()));
		}
		if (selectParser.getFromItem() instanceof SubSelect) {
			columns.addAll(find((Expression) selectParser.getFromItem()));
		}
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
		columns = find(existsExpression.getRightExpression());
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
	
	private void visitBinaryExpression(BinaryExpression binaryExpression) {
		List<String> tmpcolumns = find(binaryExpression.getLeftExpression());
		tmpcolumns.addAll(find(binaryExpression.getRightExpression()));
		columns = tmpcolumns;
	}

}
