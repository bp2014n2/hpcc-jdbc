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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLNameParser;
import de.hpi.hpcc.parsing.ECLSelectParser;
import de.hpi.hpcc.parsing.ECLUtils;

public class ECLBuilderSelect extends ECLBuilder {

	
	protected SQLParserSelect sqlParser;
	
	
	public ECLBuilderSelect(ECLLayouts eclLayouts) {
		super(eclLayouts);
		// TODO Auto-generated constructor stub
	}
	

	//	private boolean hasAlias = false;
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		sqlParser = new SQLParserSelect(sql, eclLayouts);
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
    		generateSelects(sqlParser, false);
    		if (sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null) {
        		eclCode = ECLUtils.convertToTable(eclCode);
        	}
    	}
    	generateDistinct(sqlParser);
    	generateLimit(sqlParser);
    	
    	return(eclCode.toString());
	}
	
	private void generateLimit(SQLParserSelect sqlParser) {
		Limit limit = sqlParser.getLimit();
		if (limit != null) {
			eclCode.insert(0, "CHOOSEN(");
			eclCode.append(", ");
			eclCode.append(limit.getRowCount());
			eclCode.append(")");
    	}
	}
	
	private void generateDistinct(SQLParserSelect sqlParser) {
		if (sqlParser.isDistinct()) {
			eclCode.insert(0, "DEDUP(");
			eclCode.append(", All)");
    	}
	}
	
	private void generateGroupBys(SQLParserSelect sqlParser) {
		List<Expression> groupBys = sqlParser.getGroupBys(); 
		if (groupBys != null) {
			for (Expression expression : groupBys) {
				eclCode.append(", ");
				eclCode.append(parseExpressionECL(expression));
			}
		}
	}
	
	private void generateOrderBys(SQLParserSelect sqlParser) {
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
	
	private void generateFrom(SQLParserSelect sqlParser) {	

		FromItem table = sqlParser.getFromItem();
		if (sqlParser.getJoins() != null) {
			EqualsTo joinCondition = findJoinCondition(sqlParser.getWhere());
			String joinColumn = ((Column) joinCondition.getRightExpression()).getColumnName();
			eclCode.append("JOIN("+((Table)table).getName()+", "+((Table) sqlParser.getJoins().get(0).getRightItem()).getName());
			eclCode.append(", LEFT."+joinColumn+" = RIGHT."+joinColumn+", LOOKUP)");
		} else {
			
			if (table instanceof Table) {
				eclCode.append(((Table) table).getName());
	    	} else if (table instanceof SubSelect){
	    		eclCode.append("(");
	    		String innerStatement = sqlParser.trimInnerStatement(table.toString());
	    		eclCode.append(new ECLBuilderSelect(eclLayouts).generateECL(innerStatement));
	    		eclCode.append(")");
	    	}
		}
	}
	
	private void generateWhere(SQLParserSelect sqlParser) {
		Expression whereItems = sqlParser.getWhere();
    	if (whereItems != null) {
    		eclCode.append("(");
    		eclCode.append(parseExpressionECL(whereItems));
    		eclCode.append(")");
    	}
	}
	
	/**
	 * Generates the SelectItems between "SELECT" and "FROM" in a SQL query.
	 * If a "SELECT * FROM" is used all columns of corresponding tables are added.
	 * 
	 * @return
	 */
	
	private void generateSelects(SQLParserSelect sqlParser, Boolean inner) {
		LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
		eclCode.append(", ");
		if(inner) {
			selectItemsStrings.addAll(createInnerSelectItemsString(sqlParser));
		}
    
		if (sqlParser.isSelectAll()){
			if(sqlParser.getFromItem() instanceof SubSelect) {
				selectItemsStrings.addAll(sqlParser.getFromItemColumns());
			} else {
				selectItemsStrings.addAll(sqlParser.getAllColumns());
			}
		} else {
			selectItemsStrings.addAll(createSelectItems(sqlParser));
		}
    	eclCode.append(ECLUtils.encapsulateWithCurlyBrackets(ECLUtils.join(selectItemsStrings, ", ")));
	}
	
	private Collection<? extends String> createSelectItems(SQLParserSelect sqlParser) {
		LinkedHashSet<String> selectItemsStrings = new LinkedHashSet<String>();
		ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				StringBuilder selectItemString = new StringBuilder();
				SelectExpressionItem sei = (SelectExpressionItem) selectItem;
				ECLSelectParser selectParser = new ECLSelectParser(eclLayouts, sqlParser);
				selectItemString.append(selectParser.parse(sei));
   				selectItemsStrings.add(selectItemString.toString());
   			}
		}
		return selectItemsStrings;
	}

	private LinkedHashSet<String> createInnerSelectItemsString(SQLParserSelect sqlParser) {
		LinkedHashSet<String> innerItems = new LinkedHashSet<String>();
		if (sqlParser.getOrderBys() != null) {
			for (OrderByElement orderByElement : sqlParser.getOrderBys()) {
				Expression orderBy = orderByElement.getExpression();
				ECLSelectParser selectParser = new ECLSelectParser(eclLayouts, sqlParser);
				String select = selectParser.parse(orderBy);
				innerItems.add(select);
			}
		}
		return innerItems;
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
	protected SQLParserSelect getSqlParser() {
		// TODO Auto-generated method stub
		return sqlParser;
	}
}
