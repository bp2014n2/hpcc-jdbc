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
import net.sf.jsqlparser.expression.WindowOffset;
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
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
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
import net.sf.jsqlparser.statement.Statement;
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
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
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
		tryAccept(function.getParameters());
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
		tryAccept(parenthesis.getExpression());
	}

	@Override
	public void visit(StringValue stringValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition addition) {
		visitBinaryExpression(addition);
	}

	@Override
	public void visit(Division division) {
		visitBinaryExpression(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		visitBinaryExpression(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		visitBinaryExpression(subtraction);
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
		tryAccept(between.getLeftExpression());
		tryAccept(between.getBetweenExpressionStart());
		tryAccept(between.getBetweenExpressionEnd());
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
		tryAccept(inExpression.getLeftExpression());
		tryAccept(inExpression.getRightItemsList());
		tryAccept(inExpression.getLeftItemsList());
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		tryAccept(isNullExpression.getLeftExpression());
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
		tryAccept(tableColumn.getTable());
	}

	@Override
	public void visit(SubSelect subSelect) {
		tryAccept(subSelect.getSelectBody());
	}

	@Override
	public void visit(CaseExpression caseExpression) {
		tryAccept(caseExpression.getElseExpression());
		tryAccept(caseExpression.getSwitchExpression());
		if(caseExpression.getWhenClauses() != null) {
			for(Expression whenClause : caseExpression.getWhenClauses()) {
				whenClause.accept(this);
			}
		}
	}

	@Override
	public void visit(WhenClause whenClause) {
		tryAccept(whenClause.getThenExpression());
		tryAccept(whenClause.getWhenExpression());
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		tryAccept(existsExpression.getRightExpression());
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		tryAccept((Expression) allComparisonExpression.getSubSelect());
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		tryAccept((Expression) anyComparisonExpression.getSubSelect());
	}

	@Override
	public void visit(Concat concat) {
		visitBinaryExpression(concat);
	}

	@Override
	public void visit(Matches matches) {
		visitBinaryExpression(matches);
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		visitBinaryExpression(bitwiseAnd);
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		visitBinaryExpression(bitwiseOr);
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		visitBinaryExpression(bitwiseXor);
	}

	@Override
	public void visit(CastExpression cast) {
		tryAccept(cast);
	}

	@Override
	public void visit(Modulo modulo) {
		visitBinaryExpression(modulo);
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		tryAccept(aexpr.getDefaultValue());
		tryAccept(aexpr.getExpression());
		tryAccept(aexpr.getOffset());
		if(aexpr.getOrderByElements() != null) {
			for(OrderByElement orderBy : aexpr.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		tryAccept(aexpr.getPartitionExpressionList());
		if(aexpr.getWindowElement() != null) {
			if(aexpr.getWindowElement().getOffset() != null) {
				visit(aexpr.getWindowElement().getOffset());
			}
			if(aexpr.getWindowElement().getRange() != null) {
				if(aexpr.getWindowElement().getRange().getStart() != null) {
					visit(aexpr.getWindowElement().getRange().getStart());
				}
				if(aexpr.getWindowElement().getRange().getEnd() != null) {
					visit(aexpr.getWindowElement().getRange().getEnd());
				}
			}
		}
	}

	@Override
	public void visit(WithinGroupExpression wgexpr) {
		tryAccept(wgexpr.getExprList());
		if(wgexpr.getOrderByElements() != null) {
			for(OrderByElement orderBy : wgexpr.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
	}

	@Override
	public void visit(ExtractExpression eexpr) {
		tryAccept(eexpr.getExpression());
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
		visitBinaryExpression(rexpr);
	}

	@Override
	public void visit(JsonExpression jsonExpr) {
		tryAccept(jsonExpr.getColumn());
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

	@Override
	public void visit(PostgreSQLFromForExpression postgreSQLFromForExpression) {
		tryAccept(postgreSQLFromForExpression.getSourceExpression());
		tryAccept(postgreSQLFromForExpression.getFromExpression());
		tryAccept(postgreSQLFromForExpression.getForExpression());
	}

	@Override
	public void visit(Select select) {
		if(select.getWithItemsList() != null) {
			for(WithItem withItem : select.getWithItemsList()) {
				withItem.accept(this);
			}
		}
		tryAccept(select.getSelectBody());
	}

	@Override
	public void visit(Delete delete) {
		tryAccept(delete.getTable());
		tryAccept(delete.getWhere());
	}

	@Override
	public void visit(Update update) {
		if(update.getColumns() != null) {
			for(Column column : update.getColumns()) {
				column.accept(this);
			}
		}		
		if(update.getTables() != null) {
			for(Table table : update.getTables()) {
				table.accept(this);
			}
		}		
		tryAccept(update.getFromItem());
		tryAccept(update.getSelect());	
		tryAccept(update.getWhere());
		
	}

	@Override
	public void visit(Insert insert) {
		if(insert.getColumns() != null) {
			for(Column column : insert.getColumns()) {
				column.accept(this);
			}
		}
		tryAccept(insert.getSelect());
		tryAccept(insert.getTable());
	}

	@Override
	public void visit(Replace replace) {
		if(replace.getColumns() != null) {
			for(Column column : replace.getColumns()) {
				column.accept(this);
			}
		}
		if(replace.getExpressions() != null) {
			for(Expression expression : replace.getExpressions()) {
				expression.accept(this);
			}
		}
		tryAccept(replace.getItemsList());
		tryAccept(replace.getTable());
	}

	@Override
	public void visit(Drop drop) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate truncate) {
		tryAccept(truncate.getTable());
	}

	@Override
	public void visit(CreateIndex createIndex) {
		tryAccept(createIndex.getTable());
	}

	@Override
	public void visit(CreateTable createTable) {
		if (createTable.getTable() != null) {
			createTable.getTable().accept(this);
		}
		tryAccept(createTable.getSelect());
	}

	@Override
	public void visit(CreateView createView) {
		tryAccept(createView.getSelectBody());
		if (createView.getView() != null) {
			createView.getView().accept(this);
		}
	}

	@Override
	public void visit(Alter alter) {
		tryAccept(alter.getTable());
	}

	@Override
	public void visit(Statements stmts) {
		if(stmts.getStatements() != null) {
			for(Statement statement : stmts.getStatements()) {
				statement.accept(this);
			}
		}
	}

	@Override
	public void visit(Execute execute) {
		tryAccept(execute.getExprList());
	}

	@Override
	public void visit(SetStatement set) {
		tryAccept(set.getExpression());
	}

	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		tryAccept(allTableColumns.getTable());
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		tryAccept(selectExpressionItem.getExpression());
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		tryAccept(plainSelect.getFromItem());		
		if(plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				join.getRightItem().accept(this);
			}
		}		
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
		tryAccept(plainSelect.getHaving());
		if(plainSelect.getOrderByElements() != null) {
			for(OrderByElement orderBy : plainSelect.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		tryAccept(plainSelect.getWhere());
		if(plainSelect.getDistinct() != null && plainSelect.getDistinct().getOnSelectItems() != null) {
			for(SelectItem selectItem : plainSelect.getDistinct().getOnSelectItems()) {
				selectItem.accept(this);
			}
		}
		tryAccept(plainSelect.getForUpdateTable());
		if(plainSelect.getIntoTables() != null) {
			for(Table table : plainSelect.getIntoTables()) {
				table.accept(this);
			}
		}
	}

	@Override
	public void visit(SetOperationList setOpList) {
		if(setOpList.getOrderByElements() != null) {
			for(OrderByElement orderBy : setOpList.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		if(setOpList.getSelects() != null) {
			for(SelectBody selectBody : setOpList.getSelects()) {
				selectBody.accept(this);
			}
		}
	}

	@Override
	public void visit(WithItem withItem) {
		tryAccept(withItem.getSelectBody());
		if(withItem.getWithItemList() != null) {
			for(SelectItem selectItem : withItem.getWithItemList()) {
				selectItem.accept(this);
			}
		}
	}

	@Override
	public void visit(Table tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin subjoin) {
		tryAccept(subjoin.getLeft());
        if(subjoin.getJoin() != null) {
        	tryAccept(subjoin.getJoin().getRightItem());
        	tryAccept(subjoin.getJoin().getOnExpression());
        	if(subjoin.getJoin().getUsingColumns() != null) {
        		for(Column column : subjoin.getJoin().getUsingColumns()) {
        			column.accept(this);
        		}
        	}
        }
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		tryAccept((Expression) lateralSubSelect.getSubSelect());
	}

	@Override
	public void visit(ValuesList valuesList) {
		tryAccept(valuesList.getMultiExpressionList());
	}

	@Override
	public void visit(OrderByElement orderBy) {
		tryAccept(orderBy.getExpression());
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
		if(multiExprList.getExprList() != null) {
			for(ExpressionList expressionList : multiExprList.getExprList()) {
				expressionList.accept(this);
			}
		}
	}
	
	protected void visitBinaryExpression(BinaryExpression binaryExpression) {
		tryAccept(binaryExpression.getLeftExpression());
		tryAccept(binaryExpression.getRightExpression());
	}
	
	public void visit(WindowOffset windowOffset) {
		tryAccept(windowOffset.getExpression());
	}
	
	protected void tryAccept(Expression expression) {
		if(expression != null) {
			expression.accept(this);
		}
	}
	
	protected void tryAccept(ItemsList itemsList) {
		if(itemsList != null) {
			itemsList.accept(this);
		}
	}
	
	protected void tryAccept(FromItem fromItem) {
		if(fromItem != null) {
			fromItem.accept(this);
		}
	}
	
	protected void tryAccept(SelectBody selectBody) {
		if(selectBody != null) {
			selectBody.accept(this);
		}
	}
	
	protected void tryAccept(Statement statement) {
		if(statement != null) {
			statement.accept(this);
		}
	}

}
