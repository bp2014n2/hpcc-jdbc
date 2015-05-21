package de.hpi.hpcc.parsing.visitor;

import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLDataTypeParser extends FullVisitorAdapter {

	private String dataType = "STRING50";
	private ECLLayouts layouts;
	private SQLParser sqlParser;
	private SelectExpressionItem alias;
	private String expressionString = "";

	public ECLDataTypeParser(ECLLayouts layouts, SQLParser sqlParser) {
		this.layouts = layouts;
		this.sqlParser = sqlParser;
	}
	
	public String parse(Expression expression) {
		dataType = "STRING50";
		expressionString = expression.toString();
		expression.accept(this);
		return dataType;
	}
	
	@Override
	public void visit(Function function) {
		String functionName = function.getName().toLowerCase();
		switch (functionName) {
		case "substring": dataType = "STRING50"; break;
		case "sum":
		case "count":
		default: dataType = "INTEGER8"; break;
		}
	}

	@Override
	public void visit(LongValue longValue) {
		dataType = "INTEGER5";
	}

	@Override
	public void visit(StringValue stringValue) {
		dataType = "STRING50";
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		if (selectExpressionItem.getAlias() != null) alias = selectExpressionItem;
		tryAccept(selectExpressionItem.getExpression());
	}
	
	@Override
	public void visit(Column tableColumn) {
		boolean columnFound = false;
		for(String table : sqlParser.getAllTables()) {
			ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
			List<SelectExpressionItem> selectExpressionItems = finder.find(sqlParser.getStatement());
			for(SelectExpressionItem selectItem : selectExpressionItems) {
				boolean aliasFound = alias != null && alias.getExpression().toString().equalsIgnoreCase(selectItem.toString()) && expressionString.equalsIgnoreCase(alias.getAlias().toString());
				if(selectItem.toString().equalsIgnoreCase(tableColumn.getColumnName()) || aliasFound) {
					dataType = layouts.getECLDataType(table, selectItem.toString());
					columnFound = true;
					break;
				} 
			}
		}
		if (!columnFound) {
			sqlParser.getStatement().accept(this);
		}
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		switch (aexpr.getName().toLowerCase()) {
		case "row_number": dataType = "INTEGER5"; break;
		default: dataType = "INTEGER5"; break;
		}
	}
}
