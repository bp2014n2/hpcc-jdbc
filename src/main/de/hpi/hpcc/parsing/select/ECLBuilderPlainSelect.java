package de.hpi.hpcc.parsing.select;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.visitor.ECLFromItemGenerator;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectExpressionParser;
import de.hpi.hpcc.parsing.visitor.ECLWhereExpressionOptimizer;

public class ECLBuilderPlainSelect extends ECLBuilderSelect {
	
	protected SQLParserPlainSelect sqlParser;
	private ECLSelectExpressionParser selectExpressionParser;
	private PlainSelect plainSelect;

	public ECLBuilderPlainSelect(PlainSelect selectBody, ECLLayouts eclLayouts) {
		super(selectBody, eclLayouts);
		plainSelect = selectBody;
	}

	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL() {
		sqlParser = new SQLParserPlainSelect(plainSelect, eclLayouts);
		selectExpressionParser = new ECLSelectExpressionParser(eclLayouts, sqlParser);
		eclCode = new StringBuilder();
		
    	generateFrom(sqlParser);
    	generateWhere(sqlParser);    	
    	generateSelects(sqlParser, true);
    	generateGroupBys(sqlParser);
    	 
    	if ((sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null)) {
    		//Why not always?
    		eclCode = ECLUtils.convertToTable(eclCode);
    	}
    	generateOrderBys(sqlParser);
    	if(sqlParser.getOrderBys() != null) {
    		generateSelects(sqlParser, false); //TODO: just select name of function here, as already selected
    		if (sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null) {
        		eclCode = ECLUtils.convertToTable(eclCode);
        	}
    	}
    	generateDistinct(sqlParser);
    	generateLimit(sqlParser);
    	
    	outputCount++;
    	
    	return(eclCode.toString());
	}
	
	private void generateLimit(SQLParserPlainSelect sqlParser) {
		Limit limit = sqlParser.getLimit();
		if (limit != null) {
			eclCode.insert(0, "CHOOSEN(");
			eclCode.append(", ");
			eclCode.append(limit.getRowCount());
			eclCode.append(")");
    	}
	}
	
	private void generateDistinct(SQLParserPlainSelect sqlParser) {
		if (sqlParser.isDistinct()) {
			eclCode.insert(0, "DEDUP(");
			eclCode.append(", All)");
    	}
	}
	
	private void generateGroupBys(SQLParserPlainSelect sqlParser) {
		List<Expression> groupBys = sqlParser.getGroupBys(); 
		if (groupBys != null) {
			for (Expression expression : groupBys) {
				eclCode.append(", ");
				eclCode.append(parseExpressionECL(expression));
			}
		}
	}
	
	private void generateOrderBys(SQLParserPlainSelect sqlParser) {
		List<OrderByElement> orderBys = sqlParser.getOrderBys(); 	
    	if (orderBys != null) {
    		eclCode.append(", ");
    		for (OrderByElement orderByElement : orderBys) {
    			/*  
    			 * TODO: multiple orderByElements
    			 * TODO: order by expression (e.g. count)
    			 */
    			Expression orderBy = orderByElement.getExpression();
    			ECLNameParser nameParser = new ECLNameParser();
    			eclCode.append(nameParser.name(orderBy));
    		}
    		eclCode = ECLUtils.convertToSort(eclCode);
    	}
	}
	
	private void generateFrom(SQLParserPlainSelect sqlParser) {	

		FromItem fromItem = sqlParser.getFromItem();
		if (sqlParser.getJoins() != null) {
			StringBuilder joinString = new StringBuilder();
			// TODO: Joins with more than two tables
			joinString.append("JOIN("+((Table)fromItem).getName()+", "+((Table) sqlParser.getJoins().get(0).getRightItem()).getName());
						
			EqualsTo joinCondition = findJoinCondition(sqlParser.getWhere());
			if (joinCondition != null) {
				String joinColumn = ((Column) joinCondition.getRightExpression()).getColumnName();
				joinString.append(", LEFT."+joinColumn+" = RIGHT."+joinColumn+", LOOKUP)");
			} else {
				joinString.append(", 1=1, ALL)");
				
			}
			eclCode.append(joinString.toString());
		} else {
			ECLFromItemGenerator generator = new ECLFromItemGenerator(eclLayouts);
			eclCode.append(generator.generate(fromItem));
		}
	}
	
	private void generateWhere(SQLParserPlainSelect sqlParser) {
		Expression whereItems = sqlParser.getWhere();
    	if (whereItems != null) {
    		eclCode.append("(");
    		ECLWhereExpressionOptimizer expressionOptimizer = new ECLWhereExpressionOptimizer();
    		expressionOptimizer.pushFromItem(sqlParser.getFromItem());
    		Expression newWhere = expressionOptimizer.optimize(whereItems);
    		eclCode.append(parseExpressionECL(newWhere));
    		eclCode.append(")");
    	}
	}
	
	/**
	 * Generates the SelectItems between "SELECT" and "FROM" in a SQL query.
	 * If a "SELECT * FROM" is used all columns of corresponding tables are added.
	 * 
	 * @return
	 */
	
	private void generateSelects(SQLParserPlainSelect sqlParser, Boolean inner) {
		LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
		eclCode.append(", ");
		if(inner) {
			selectItemsStrings.addAll(createInnerSelectItemsString(sqlParser));
			if (sqlParser.isSelectAll()){
				if(sqlParser.getFromItem() instanceof SubSelect) {
					selectItemsStrings.addAll(sqlParser.getFromItemColumns());
				} else {
					selectItemsStrings.addAll(sqlParser.getAllColumns());
				}
			} else {
				selectItemsStrings.addAll(createSelectItems(sqlParser));
			}
		} else {
			if (sqlParser.isSelectAll()){
				if(sqlParser.getFromItem() instanceof SubSelect) {
					selectItemsStrings.addAll(sqlParser.getFromItemColumns());
				} else {
					selectItemsStrings.addAll(sqlParser.getAllColumns());
				}
			} else {
				selectItemsStrings.addAll(createOuterSelectItemsString(sqlParser));
			}
		}
    
		
    	eclCode.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(selectItemsStrings, ", ")));
	}
	
	private Collection<? extends String> createSelectItems(SQLParserPlainSelect sqlParser) {
		LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
		ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) selectItem;
				String selectItemString = selectExpressionParser.parse(sei);
				if (!HPCCJDBCUtils.containsStringCaseInsensitive(selectItemsStrings, selectItemString)) {
					selectItemsStrings.add(selectItemString);
				}
   			}
		}
		return selectItemsStrings;
	}

	private LinkedHashSet<String> createInnerSelectItemsString(SQLParserPlainSelect sqlParser) {
		LinkedHashSet<String> innerItems = new LinkedHashSet<String>();
		if (sqlParser.getOrderBys() != null) {
			for (OrderByElement orderByElement : sqlParser.getOrderBys()) {
				Expression orderBy = orderByElement.getExpression();
				ECLSelectExpressionParser selectExpressionParser = new ECLSelectExpressionParser(eclLayouts, sqlParser);
				String select = selectExpressionParser.parse(orderBy);
				innerItems.add(select);
			}
		}
		return innerItems;
	}
	
	private LinkedHashSet<String> createOuterSelectItemsString(SQLParserPlainSelect sqlParser) {
		LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
		ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem) selectItem;
				String selectItemString = selectExpressionParser.parseAlias(sei);
				if (!HPCCJDBCUtils.containsStringCaseInsensitive(selectItemsStrings, selectItemString)) {
					selectItemsStrings.add(selectItemString);
				}
   				
   			}
		}
		return selectItemsStrings;
	}

	/**
	 * 
	 * @param expression
	 * @return
	 */
	private EqualsTo findJoinCondition(Expression expression) {
		if(expression instanceof EqualsTo) {
			String left = ((EqualsTo)expression).getLeftExpression().toString();
			String right = ((EqualsTo)expression).getRightExpression().toString();
			if(right.contains(".") && left.contains(".")) {
				if(!right.substring(0,right.indexOf(".") + 1).equals(left.substring(0,left.indexOf(".") + 1)) 
				&& right.substring(right.indexOf(".") + 1).equals(left.substring(left.indexOf(".") + 1))){
					return (EqualsTo) expression;
				}
			}
		}
		if (expression instanceof BinaryExpression) {
			if (((BinaryExpression) expression).getRightExpression() instanceof BinaryExpression) { 
				EqualsTo right = findJoinCondition(((BinaryExpression) expression).getRightExpression());
				if (right != null) return right;
			}
			if (((BinaryExpression) expression).getLeftExpression() instanceof BinaryExpression) {
				EqualsTo left = findJoinCondition(((BinaryExpression) expression).getLeftExpression());
				if (left != null) return left;
			}
		}
		return null;
	}

	@Override
	protected Select getStatement() {
		return select;
	}
}
