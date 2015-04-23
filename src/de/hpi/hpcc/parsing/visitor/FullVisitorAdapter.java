package de.hpi.hpcc.parsing.visitor;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.PostgreSQLFromForExpression;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public abstract class FullVisitorAdapter implements ExpressionVisitor, StatementVisitor, SelectItemVisitor, SelectVisitor, FromItemVisitor, OrderByVisitor, ItemsListVisitor {

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
		if(parenthesis.getExpression() != null) {
			parenthesis.getExpression().accept(this);
		}
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
		if(between.getLeftExpression() != null) {
			between.getLeftExpression().accept(this);
		}
		if(between.getBetweenExpressionStart() != null) {
			between.getBetweenExpressionStart().accept(this);
		}
		if(between.getBetweenExpressionEnd() != null) {
			between.getBetweenExpressionEnd().accept(this);
		}
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
		if(inExpression.getLeftExpression() != null) {
			inExpression.getLeftExpression().accept(this);
		}
		if(inExpression.getRightItemsList() != null) {
			inExpression.getRightItemsList().accept(this);
		}
		if(inExpression.getLeftItemsList() != null) {
			inExpression.getLeftItemsList().accept(this);
		}
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		if(isNullExpression.getLeftExpression() != null) {
			isNullExpression.getLeftExpression().accept(this);
		}
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		visitBinaryExpression(likeExpression);
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
		Table table = tableColumn.getTable();
		if (table != null && table.getName() != null) {
			table.accept(this);
		}
	}

	@Override
	public void visit(SubSelect subSelect) {
		if(subSelect.getSelectBody() != null) {
			subSelect.getSelectBody().accept(this);
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
		if (existsExpression.getRightExpression() != null) {
			existsExpression.getRightExpression().accept(this);
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
		if(binaryExpression.getLeftExpression() != null) {
			binaryExpression.getLeftExpression().accept(this);
		}
		if(binaryExpression.getRightExpression() != null) {
			binaryExpression.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(PostgreSQLFromForExpression postgreSQLFromForExpression) {
		postgreSQLFromForExpression.getSourceExpression().accept(this);
		postgreSQLFromForExpression.getFromExpression().accept(this);
		postgreSQLFromForExpression.getForExpression().accept(this);
	}

	@Override
	public void visit(Select select) {
		if(select.getWithItemsList() != null) {
			for(WithItem withItem : select.getWithItemsList()) {
				withItem.accept(this);
			}
		}
		if(select.getSelectBody() != null) {
			select.getSelectBody().accept(this);
		}
	}

	@Override
	public void visit(Delete delete) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Update update) {
		if(update.getColumns() != null) {
			for(Column column : update.getColumns()) {
				column.accept(this);
			}
		}
		if(update.getFromItem() != null) {
			update.getFromItem().accept(this);
		}
		if(update.getSelect() != null) {
			update.getSelect().accept(this);
		}
		if(update.getSelect() != null) {
			update.getSelect().accept(this);
		}
		if(update.getWhere() != null) {
			update.getWhere().accept(this);
		}
		if(update.getTables() != null) {
			for(Table table : update.getTables()) {
				table.accept(this);
			}
		}
	}

	@Override
	public void visit(Insert insert) {
		if(insert.getColumns() != null) {
			for(Column column : insert.getColumns()) {
				column.accept(this);
			}
		}
		if(insert.getSelect() != null) {
			insert.getSelect().accept(this);
		}
		Table table = insert.getTable();		
		if (table != null) {		
			table.accept(this);		
		}
	}

	@Override
	public void visit(Replace replace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop drop) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate truncate) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateIndex createIndex) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateTable createTable) {
		Table table = createTable.getTable();
		if (table != null) {
			table.accept(this);
		}
	}

	@Override
	public void visit(CreateView createView) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Alter alter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Statements stmts) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Execute execute) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SetStatement set) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		if(selectExpressionItem.getExpression() != null) {
			selectExpressionItem.getExpression().accept(this);
		}
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		if(plainSelect.getSelectItems() != null) {
			for(SelectItem selectItem : plainSelect.getSelectItems()) {
				selectItem.accept(this);
			}
		}
		if(plainSelect.getGroupByColumnReferences() != null) {
			for(Expression groupBy : plainSelect.getGroupByColumnReferences()) {
				groupBy.accept(this);
			}
		}
		if(plainSelect.getFromItem() != null) {
			plainSelect.getFromItem().accept(this);
		}
		if(plainSelect.getHaving() != null) {
			plainSelect.getHaving().accept(this);
		}
		if(plainSelect.getOrderByElements() != null) {
			for(OrderByElement orderBy : plainSelect.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		if(plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(this);
		}
	}

	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem withItem) {
		SelectBody sb = withItem.getSelectBody();		
		if (sb != null) {		
			sb.accept(this);		
		}
	}

	@Override
	public void visit(Table tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin subjoin) {
		if(subjoin.getLeft() != null) {
			subjoin.getLeft().accept(this);
		}
        if(subjoin.getJoin() != null) {
        	if(subjoin.getJoin().getRightItem() != null) {
        		subjoin.getJoin().getRightItem().accept(this);
        	}
        	if(subjoin.getJoin().getOnExpression() != null) {
        		subjoin.getJoin().getOnExpression().accept(this);
        	}
        }
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesList valuesList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrderByElement orderBy) {
		if(orderBy.getExpression() != null) {
			orderBy.getExpression().accept(this);
		}
	}

	@Override
	public void visit(ExpressionList expressionList) {
		if(expressionList.getExpressions() != null) {
			for(Expression expression : expressionList.getExpressions()) {
				expression.accept(this);
			}
		}
	}

	@Override
	public void visit(MultiExpressionList multiExprList) {
		// TODO Auto-generated method stub
		
	}

}
