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
		if(function.getParameters() != null) {
			function.getParameters().accept(this);
		}
	}

	@Override
	public void visit(SignedExpression signedExpression) {
		if(signedExpression.getExpression() != null) {
			signedExpression.accept(this);
		}
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
		if(caseExpression.getElseExpression() != null) {
			caseExpression.getElseExpression().accept(this);
		}
		if(caseExpression.getSwitchExpression() != null) {
			caseExpression.getSwitchExpression().accept(this);
		}
		if(caseExpression.getWhenClauses() != null) {
			for(Expression whenClause : caseExpression.getWhenClauses()) {
				whenClause.accept(this);
			}
		}
	}

	@Override
	public void visit(WhenClause whenClause) {
		if(whenClause.getThenExpression() != null) {
			whenClause.getThenExpression().accept(this);
		}
		if(whenClause.getWhenExpression() != null) {
			whenClause.getWhenExpression().accept(this);
		}
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		if (existsExpression.getRightExpression() != null) {
			existsExpression.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		if(allComparisonExpression.getSubSelect() != null) {
			visit(allComparisonExpression.getSubSelect());
		}
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		if(anyComparisonExpression.getSubSelect() != null) {
			visit(anyComparisonExpression.getSubSelect());
		}
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
		if(cast.getLeftExpression() != null) {
			cast.accept(this);
		}
	}

	@Override
	public void visit(Modulo modulo) {
		visitBinaryExpression(modulo);
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		if(aexpr.getDefaultValue() != null) {
			aexpr.getDefaultValue().accept(this);
		}
		if(aexpr.getExpression() != null) {
			aexpr.getExpression().accept(this);
		}
		if(aexpr.getOffset() != null) {
			aexpr.getOffset().accept(this);
		}
		if(aexpr.getOrderByElements() != null) {
			for(OrderByElement orderBy : aexpr.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
		if(aexpr.getPartitionExpressionList() != null) {
			aexpr.getPartitionExpressionList().accept(this);
		}
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
		if(wgexpr.getExprList() != null) {
			wgexpr.getExprList().accept(this);
		}
		if(wgexpr.getOrderByElements() != null) {
			for(OrderByElement orderBy : wgexpr.getOrderByElements()) {
				orderBy.accept(this);
			}
		}
	}

	@Override
	public void visit(ExtractExpression eexpr) {
		if(eexpr.getExpression() != null) {
			eexpr.getExpression().accept(this);
		}
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
		if(jsonExpr.getColumn() != null) {
			jsonExpr.getColumn().accept(this);
		}
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
		if(postgreSQLFromForExpression.getSourceExpression() != null) {
			postgreSQLFromForExpression.getSourceExpression().accept(this);
		}
		if(postgreSQLFromForExpression.getFromExpression() != null) {
			postgreSQLFromForExpression.getFromExpression().accept(this);
		}
		if(postgreSQLFromForExpression.getForExpression() != null) {
			postgreSQLFromForExpression.getForExpression().accept(this);
		}
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
		if(delete.getTable() != null) {
			delete.getTable().accept(this);
		}
		if(delete.getWhere() != null) {
			delete.getWhere().accept(this);
		}
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
		if(update.getFromItem() != null) {
			update.getFromItem().accept(this);
		}
		if(update.getSelect() != null) {
			update.getSelect().accept(this);
		}	
		if(update.getWhere() != null) {
			update.getWhere().accept(this);
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
		if (insert.getTable() != null) {		
			insert.getTable().accept(this);		
		}
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
		if(replace.getItemsList() != null) {
			replace.getItemsList().accept(this);
		}
		if(replace.getTable() != null) {
			replace.getTable().accept(this);
		}
	}

	@Override
	public void visit(Drop drop) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate truncate) {
		if(truncate.getTable() != null) {
			truncate.getTable().accept(this);
		}
	}

	@Override
	public void visit(CreateIndex createIndex) {
		if(createIndex.getTable() != null) {
			createIndex.getTable().accept(this);
		}
	}

	@Override
	public void visit(CreateTable createTable) {
		if (createTable.getTable() != null) {
			createTable.getTable().accept(this);
		}
		if(createTable.getSelect() != null) {
			createTable.getSelect().accept(this);
		}
	}

	@Override
	public void visit(CreateView createView) {
		if(createView.getSelectBody() != null) {
			createView.getSelectBody().accept(this);
		}
		if (createView.getView() != null) {
			createView.getView().accept(this);
		}
	}

	@Override
	public void visit(Alter alter) {
		if(alter.getTable() != null) {
			alter.getTable().accept(this);
		}
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
		if(execute.getExprList() != null) {
			execute.getExprList().accept(this);
		}
	}

	@Override
	public void visit(SetStatement set) {
		if(set.getExpression() != null) {
			set.getExpression().accept(this);
		}
	}

	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		if(allTableColumns.getTable() != null) {
			allTableColumns.getTable().accept(this);
		}
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		if(selectExpressionItem.getExpression() != null) {
			selectExpressionItem.getExpression().accept(this);
		}
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		if(plainSelect.getFromItem() != null) {
			plainSelect.getFromItem().accept(this);
		}
		
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
		if(plainSelect.getDistinct() != null && plainSelect.getDistinct().getOnSelectItems() != null) {
			for(SelectItem selectItem : plainSelect.getDistinct().getOnSelectItems()) {
				selectItem.accept(this);
			}
		}
		if(plainSelect.getForUpdateTable() != null) {
			plainSelect.getForUpdateTable().accept(this);
		}
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
		if (withItem.getSelectBody() != null) {		
			withItem.getSelectBody().accept(this);		
		}
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
        	if(subjoin.getJoin().getUsingColumns() != null) {
        		for(Column column : subjoin.getJoin().getUsingColumns()) {
        			column.accept(this);
        		}
        	}
        }
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		if(lateralSubSelect.getSubSelect() != null) {
			visit(lateralSubSelect.getSubSelect());
		}
	}

	@Override
	public void visit(ValuesList valuesList) {
		if(valuesList.getMultiExpressionList() != null) {
			valuesList.getMultiExpressionList().accept(this);
		}
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
		if(multiExprList.getExprList() != null) {
			for(ExpressionList expressionList : multiExprList.getExprList()) {
				expressionList.accept(this);
			}
		}
	}
	
	public void visit(WindowOffset windowOffset) {
		if(windowOffset.getExpression() != null) {
			windowOffset.getExpression().accept(this);
		}
	}

}
