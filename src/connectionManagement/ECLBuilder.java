package connectionManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;

public class ECLBuilder {
        
    private static final String			Layout_ConceptDimension = "RECORD STRING700 concept_path;  STRING50 concept_cd;  STRING2000 name_char;  STRING concept_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientDimension = "RECORD UNSIGNED5 patient_num;STRING50 vital_status_cd;STRING25 birth_date;STRING25 death_date;STRING50 sex_cd;UNSIGNED2 age_in_years_num;STRING50 language_cd;STRING50 race_cd;STRING50 marital_status_cd;STRING50 religion_cd;STRING10 zip_cd;STRING700 statecityzip_path;STRING50 income_cd;STRING patient_blob;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ObservationFact = "RECORD UNSIGNED5 encounter_num;UNSIGNED5 patient_num;STRING50 concept_cd;STRING50 provider_id;STRING25 start_date;STRING100 modifier_cd;UNSIGNED5 instance_num;STRING50 valtype_cd;STRING255 tval_char;DECIMAL18_5 nval_num;STRING50 valueflag_cd;DECIMAL18_5 quantity_num;STRING50 vunits_cd;STRING25 end_date;STRING50 location_cd;STRING observation_blob;DECIMAL18_5 confidence;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientMapping ="RECORD STRING200 patient_ide;  STRING50 patient_ide_source;  UNSIGNED5 patient_num;  STRING50 patient_ide_status;  STRING50 project_id;  STRING25 upload_date;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ProviderDimension ="RECORD STRING50 provider_id;  STRING700 provider_path;  STRING850 name_char;  STRING provider_blob; STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_VisitDimension = "RECORD UNSIGNED5 encounter_num;  UNSIGNED5 patient_num;  STRING50 active_status_cd;  STRING25 start_date;  STRING25 end_date;  STRING50 inout_cd;  STRING50 location_cd;  STRING900 location_path;  UNSIGNED5 length_of_stay;  STRING visit_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id; END;";
    public static HashMap<String, String> layouts = new HashMap<String, String> ();

    
   
	public ECLBuilder() {
		setupLayouts();
	}

	/**
	 * 
	 * @return returns a HashMap with all layouts from i2b2demodata referenced by the table name
	 */
	public static HashMap<String, String> getLayouts() {
		setupLayouts();
		return layouts;
		
	}
	
	private static void setupLayouts () {
		layouts.put("concept_dimension", Layout_ConceptDimension);
		layouts.put("patient_dimension", Layout_PatientDimension);
		layouts.put("observation_fact", Layout_ObservationFact);
		layouts.put("patient_mapping", Layout_PatientMapping);
		layouts.put("provider_dimension", Layout_ProviderDimension);
		layouts.put("visit_dimension", Layout_VisitDimension);
	}
	
	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	public String generateECL(String sql) {
		
	/*  TODO: CHECK FOR:
	 * 		SELECT
	 * 		INSERT
	 * 		UPDATE	
	 */
		SQLParser sqlParser = new SQLParser(sql);
		if (sqlParser.statement instanceof Select) {
			return generateSelectECL(sqlParser);
		} else if (sqlParser.statement instanceof Insert) {
//			return generateInsertECL();
		} else if (sqlParser.statement instanceof Update) {
//			return generateUpdateECL();
		} else if (sqlParser.statement instanceof Drop) {
//			return generateDropECL();
		} else {
			System.out.println("error, found:"+sqlParser.statement.getClass());
			return "";
		}
		return null;
	}
	
	/**
	 * Generates the ECL code for a select statement (saved in global variable)
	 * @return returns the ECL code for the given select statement
	 */
	
	private String generateSelectECL(SQLParser sqlParser) {
		StringBuilder eclCode = new StringBuilder();
		  	
    	generateFrom(sqlParser, eclCode);
    	generateWhere(sqlParser, eclCode);    	
    	generateSelects(sqlParser, eclCode);   	
    	generateGroupBys(sqlParser, eclCode);
    	convertToTable(sqlParser, eclCode); 	
    	generateDistinct(sqlParser, eclCode);
    	generateOrderBys(sqlParser, eclCode);
    	
    	return(eclCode.toString());
	}
	
	private void convertToTable(SQLParser sqlParser, StringBuilder eclCode) {
		if (sqlParser.getGroupBys() != null || sqlParser.getSelectItems() != null && !sqlParser.isSelectAll()) {
    		eclCode.insert(0, "Table(");
    		eclCode.append(")");
    	}
	}
	
	private void generateDistinct(SQLParser sqlParser, StringBuilder eclCode) {
		if (sqlParser.isDistinct()) {
			eclCode.insert(0, "DEDUP(");
			eclCode.append(", All)");
    	}
	}
	
	private void generateGroupBys(SQLParser sqlParser, StringBuilder eclCode) {
		List<Expression> groupBys = sqlParser.getGroupBys(); 
		if (groupBys != null) {
			eclCode.append(", ");
			for (Expression expression : groupBys) {
				eclCode.append(parseExpressionECL(expression));
			}
		}
	}
	
	private void generateOrderBys(SQLParser sqlParser, StringBuilder eclCode) {
		List<OrderByElement> orderBys = sqlParser.getOrderBys(); 	
    	if (orderBys != null) {
    		eclCode.insert(0, "SORT(");
    		eclCode.append(", ");
    		for (OrderByElement orderByElement : orderBys) {
    			/*  
    			 * TODO: multiple orderByElements
    			 * TODO: order by expression (e.g. count)
    			 */
    			Expression orderBy = orderByElement.getExpression();
    			if (orderBy instanceof Function) {
    				eclCode.append(parseFunction((Function) orderBy));
    			} else {
    				eclCode.append(parseExpressionECL(orderByElement.getExpression()));
    			}
    		}
    		eclCode.append(")");
    	}
	}
	
	private void generateFrom(SQLParser sqlParser, StringBuilder from) {
		FromItem table = sqlParser.getTable();
		if (table instanceof Table) {
			from.append(((Table) table).getName());
    	} else if (table instanceof SubSelect){
    		from.append("(");
    		String innerStatement = trimInnerStatement(table.toString());
    		from.append(new ECLBuilder().generateECL(innerStatement));
    		from.append(")");
    	}
	}
	
	private void generateWhere(SQLParser sqlParser, StringBuilder where) {
		Expression whereItems = sqlParser.getWhere();
    	if (whereItems != null) {
    		where.append("(");
    		where.append(parseExpressionECL(whereItems));
    		where.append(")");
    	}
	}
	
	/**
	 * Generates the SelectItems between "SELECT" and "FROM" in a SQL query.
	 * If a "SELECT * FROM" is used 
	 * 
	 * @return
	 */
	
	private void generateSelects(SQLParser sqlParser, StringBuilder select) {
		ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
    	
    	if (!selectItems.isEmpty() && !sqlParser.isSelectAll()) {
    		select.append(", ");
    		select.append("{");
    	}
    	for (int i = 0; i<selectItems.size(); i++) {
    		SelectItem selectItem = selectItems.get(i);
    		if (selectItem instanceof SelectExpressionItem) {
    			if (((SelectExpressionItem) selectItem).getAlias() != null) {
    				select.append(((SelectExpressionItem) selectItem).getAlias().getName());
    				select.append(" := ");
    			}
    			select.append(parseExpressionECL((Expression) ((SelectExpressionItem) selectItem).getExpression()));
    		}
    		if (i != (selectItems.size()-1)) select.append(", ");
    	}
    	if (!selectItems.isEmpty() && !sqlParser.isSelectAll()) {
    		select.append("}");
    	}
	}
	
	/**
	 * Generates for a given Expression the ECL code by a recursive approach
	 * Each Expression can be e.g. a BinaryExpression (And, or etc. ), but also a Column or a SubSelect.
	 * The break condition is accomplished if the Expression is a Column, Long or String
	 * @param expressionItem is the Expression under consideration
	 * @return returns the ECL code for the given expression
	 */
	private String parseExpressionECL(Expression expressionItem) {
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
			if (((Column) expressionItem).getFullyQualifiedName() != null) {
//				expression.append(((Column) expressionItem).getTable().getName());
//				expression.append(".");
			}
			expression.append(((Column) expressionItem).getColumnName());
		} else if (expressionItem instanceof SubSelect) {
			expression.append("(");
			expression.append(new ECLBuilder().generateECL(((SubSelect) expressionItem).getSelectBody().toString()));
			expression.append(")");
		} else if (expressionItem instanceof Function) {
			expression.append(nameFunction((Function) expressionItem));
			expression.append(" := ");
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
		} else if (expressionItem instanceof IsNullExpression) {
			expression.append(parseExpressionECL(((IsNullExpression) expressionItem).getLeftExpression()));
			expression.append(" = ''");
		}
		return expression.toString();
	}
	
	/**
	 * Parses the function and generates ECL code
	 * @param function can be e.g. an object representing "COUNT" or "AVG"
	 * @return returns the ECL for the given function
	 */

	private String parseFunction(Function function) {	
		return function.getName()+"(group)";
	}
	
	private String nameFunction(Function function) {
		StringBuilder innerFunctionString = new StringBuilder();
//		StringBuilder functionString = new StringBuilder();
		if (function.isAllColumns()) {
//			innerFunctionString.append();
		} else {
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
		stringValue = stringValue.replace("\\", "\\\\");
		likeString.append("");
		likeString.append(parseExpressionECL(expressionItem.getLeftExpression()));
		likeString.append("[");
		if (stringValue.endsWith("%")) stringValue = stringValue.replace("%", "");
		likeString.append("1");
		likeString.append("..");
		likeString.append(stringValue.length());
		likeString.append("] = '");
		likeString.append(stringValue);
		likeString.append("\'");
		return likeString.toString();
	}
	
	private String parseInExpression(InExpression expressionItem) {
		StringBuilder expression = new StringBuilder();
		expression.append(parseExpressionECL(((InExpression) expressionItem).getLeftExpression()));
		expression.append(" in ");
		expression.append("set(");
		if (((InExpression) expressionItem).getRightItemsList() instanceof ExpressionList) {
			for (Expression exp : ((ExpressionList) ((InExpression) expressionItem).getRightItemsList()).getExpressions()) {
				expression.append(parseExpressionECL(exp));
			}
		} else if (((InExpression) expressionItem).getRightItemsList() instanceof SubSelect) {
			expression.append(new ECLBuilder().generateECL(((SubSelect) ((InExpression) expressionItem).getRightItemsList()).getSelectBody().toString()));
		}
		expression.append(")");
		return expression.toString();
	}

	private String getSymbolOfExpression(BinaryExpression whereItems) {
		if (whereItems instanceof AndExpression) {
			return " && ";
		} else if (whereItems instanceof OrExpression) {
			return " || ";
		} else if (whereItems instanceof MinorThan) {
			return " < ";
		} else if (whereItems instanceof EqualsTo) {
			return " = ";
		} else if (whereItems instanceof GreaterThan) {
			return " > ";
		} else if (whereItems instanceof NotEqualsTo) {
			return " != ";
		} 
		return null;
	}

	private String trimInnerStatement(String innerStatement) {
		if (innerStatement.charAt(0) == '(') {
			int end = innerStatement.lastIndexOf(")");
			innerStatement = innerStatement.substring(1, end);
		}
		return innerStatement;
	}
}
