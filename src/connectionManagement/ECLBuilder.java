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
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ECLBuilder {
	
	private StringBuilder         		eclCode = new StringBuilder();
	private SQLParser 					sqlParser;
        
    private static final String			Layout_ConceptDimension = "RECORD STRING700 concept_path;  STRING50 concept_cd;  STRING2000 name_char;  STRING concept_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientDimension = "RECORD UNSIGNED5 patient_num;STRING50 vital_status_cd;STRING25 birth_date;STRING25 death_date;STRING50 sex_cd;UNSIGNED2 age_in_years_num;STRING50 language_cd;STRING50 race_cd;STRING50 marital_status_cd;STRING50 religion_cd;STRING10 zip_cd;STRING700 statecityzip_path;STRING50 income_cd;STRING patient_blob;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ObservationFact = "RECORD UNSIGNED5 encounter_num;UNSIGNED5 patient_num;STRING50 concept_cd;STRING50 provider_id;STRING25 start_date;STRING100 modifier_cd;UNSIGNED5 instance_num;STRING50 valtype_cd;STRING255 tval_char;DECIMAL18_5 nval_num;STRING50 valueflag_cd;DECIMAL18_5 quantity_num;STRING50 vunits_cd;STRING25 end_date;STRING50 location_cd;STRING observation_blob;DECIMAL18_5 confidence;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientMapping ="RECORD STRING200 patient_ide;  STRING50 patient_ide_source;  UNSIGNED5 patient_num;  STRING50 patient_ide_status;  STRING50 project_id;  STRING25 upload_date;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ProviderDimension ="RECORD STRING50 provider_id;  STRING700 provider_path;  STRING850 name_char;  STRING provider_blob; STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_VisitDimension = "RECORD UNSIGNED5 encounter_num;  UNSIGNED5 patient_num;  STRING50 active_status_cd;  STRING25 start_date;  STRING25 end_date;  STRING50 inout_cd;  STRING50 location_cd;  STRING900 location_path;  UNSIGNED5 length_of_stay;  STRING visit_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id; END;";
    public static HashMap<String, String> layouts = new HashMap<String, String> ();

    private List<OrderByElement> orderBys;
   
	public ECLBuilder() {
		setupLayouts();
	}

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
	
	public String generateECL(String sql) {
		
	/*  TODO: CHECK FOR:
	 * 		SELECT
	 * 		INSERT
	 * 		UPDATE	
	 */
		sqlParser = new SQLParser(sql);
		if (sqlParser.statement instanceof Select) {
			return generateSelectECL(sql);
		} else {
			System.out.println("error, found:"+sqlParser.statement.getClass());
			return "";
		}
	}
	
	private String generateSelectECL(String statement) {
		
    	orderBys = sqlParser.getOrderBys();
    	
//    	TODO: FIRST CHECK FOR ORDERBY
    	
    	if (orderBys != null) {
    		eclCode.append("SORT(");
    	}
    	
    /*	TODO: CHECK FOR FROM CONDITION
     *  if fromItem is table
     *  	then print tablename
     *  if subselect
     *  	then call ECLBuilder with subselect(if possible as string)
     */
    	FromItem table = sqlParser.getTable();
    	
    	if (table instanceof Table) {
    		eclCode.append(((Table) table).getName());
    	} else if (table instanceof SubSelect){
    		eclCode.append("(");
    		String innerStatement = trimInnerStatement(table.toString());
    		eclCode.append(new ECLBuilder().generateECL(innerStatement));
    		eclCode.append(")");
    	}
    	
    	
    /*	TODO: CHECK FOR WHERE
     *  getExpressionECL()
     */
    	
    	Expression whereItems = sqlParser.getWhere();
    	if (whereItems != null) {
    		String where = generateExpressionECL(whereItems);
        	eclCode.append("(");
        	eclCode.append(where);
        	eclCode.append(")");
    	}
    	
    	
    	
    	
    	/*	TODO: CHECK FOR GROUP BY
         *  not yet checked
         */
    	
    	/*
    	 *	 Close OrderBy: FromItem should be finished, add column/expression for ordering 
    	 */
    	
    	if (orderBys != null) {
    		eclCode.append(", ");
    		for (OrderByElement orderByElement : orderBys) {
    			/*  
    			 * TODO: multiple orderByElements
    			 */
    			eclCode.append(orderByElement.getExpression().toString());
    		}
    		eclCode.append(")");
    	}
    	

        
    	/*	TODO: CHECK SELECTS
         *  for SelectExpressionItem
         *  	do getExpressionECL()
         */	
    	
    	ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) sqlParser.getSelectItems();
    	StringBuilder select = new StringBuilder();
    	Boolean isNotSelectAll = false;
    	for (SelectItem selectItem : selectItems) {
    		if(!(selectItem instanceof AllColumns)) isNotSelectAll = true;
    	}
    	if (!selectItems.isEmpty() && isNotSelectAll) {
    		eclCode.append(", ");
    		select.append("{");
    	}
    	for (int i = 0; i<selectItems.size(); i++) {
    		SelectItem selectItem = selectItems.get(i);
    		if (selectItem instanceof AllColumns) {
//    			no filtering
    		} else if (selectItem instanceof SelectExpressionItem) {
    			select.append(generateExpressionECL((Expression) ((SelectExpressionItem) selectItem).getExpression()));
    		}
    		if (i != (selectItems.size()-1)) {
    			select.append(", ");
    		}
    	}
    	if (!selectItems.isEmpty() && isNotSelectAll) {
    		select.append("}");
    	}
    	eclCode.append(select.toString());
    	return(eclCode.toString());
	}
	private String generateExpressionECL(Expression expressionItem) {
		StringBuilder expression = new StringBuilder();
		
		if (expressionItem instanceof LikeExpression) {
			expression.append(parseLikeExpression((LikeExpression) expressionItem));
		} else if (expressionItem instanceof BinaryExpression) {
			expression.append(generateExpressionECL(((BinaryExpression) expressionItem).getLeftExpression()));
			expression.append(getSymbolOfExpression((BinaryExpression) expressionItem));
			expression.append(generateExpressionECL(((BinaryExpression) expressionItem).getRightExpression()));
		} else if (expressionItem instanceof InExpression) {
			expression.append(parseInExpression((InExpression) expressionItem));
		} else if (expressionItem instanceof Column) {
			if (((Column) expressionItem).getTable().getName() != null) {
//				expression.append(((Column) expressionItem).getTable().getName());
//				expression.append(".");
			}
			expression.append(((Column) expressionItem).getColumnName());
		} else if (expressionItem instanceof SubSelect) {
			expression.append("(");
			expression.append(new ECLBuilder().generateECL(((SubSelect) expressionItem).getSelectBody().toString()));
			expression.append(")");
		} else if (expressionItem instanceof Function) {
			expression.append(parseFunction((Function) expressionItem));
		}  else if (expressionItem instanceof Between) {
			expression.append("(");
			expression.append(generateExpressionECL(((Between) expressionItem).getLeftExpression()));
			expression.append(" BETWEEN ");
			expression.append(generateExpressionECL(((Between) expressionItem).getBetweenExpressionStart()));
			expression.append(" AND ");
			expression.append(generateExpressionECL(((Between) expressionItem).getBetweenExpressionEnd()));
			expression.append(")");
		} else if (expressionItem instanceof StringValue) {
			expression.append("'");
			expression.append(((StringValue) expressionItem).getValue());
			expression.append("'");
		} else if (expressionItem instanceof LongValue) {
			expression.append(((LongValue) expressionItem).getValue());
		} else if (expressionItem instanceof SelectExpressionItem) {
			
		}
		return expression.toString();
	}

	private String parseFunction(Function function) {
		StringBuilder innerFunctionString = new StringBuilder();
		StringBuilder functionString = new StringBuilder();
		if (function.isAllColumns()) {
//			innerFunctionString.append();
		} else {
			for (Expression expression : function.getParameters().getExpressions()) {
				innerFunctionString.append(generateExpressionECL(expression));
			}
		}
		functionString.append(function.getName().toLowerCase()+"_"+ innerFunctionString.toString() +" := ");
		functionString.append(function.getName());
		functionString.append("(");
		functionString.append(innerFunctionString.toString());
		functionString.append(")");
		
	
		return functionString.toString();
	}

	private String parseLikeExpression(LikeExpression expressionItem) {
		StringBuilder likeString = new StringBuilder();
		String stringValue = ((StringValue) expressionItem.getRightExpression()).getValue();
		likeString.append("(");
		if (stringValue.endsWith("%")) {
			System.out.println(stringValue);
			stringValue = stringValue.substring(0, stringValue.length()-1);
			stringValue = stringValue.replace("\\", "\\\\");
			
			System.out.println(stringValue);
			likeString.append("Std.Str.StartsWith(");
		}
		likeString.append(generateExpressionECL(expressionItem.getLeftExpression()));
		likeString.append(", '");
		likeString.append(stringValue);
		likeString.append("\'))");
		
		return likeString.toString();
	}
	
	private String parseInExpression(InExpression expressionItem) {
		StringBuilder expression = new StringBuilder();
		expression.append(generateExpressionECL(((InExpression) expressionItem).getLeftExpression()));
		expression.append(" IN ");
		expression.append("(");
		if (((InExpression) expressionItem).getRightItemsList() instanceof ExpressionList) {
			for (Expression exp : ((ExpressionList) ((InExpression) expressionItem).getRightItemsList()).getExpressions()) {
				expression.append(generateExpressionECL(exp));
			}
		} else if (((InExpression) expressionItem).getRightItemsList() instanceof SubSelect) {
			expression.append("(");
			expression.append(new ECLBuilder().generateECL(((SubSelect) ((InExpression) expressionItem).getRightItemsList()).getSelectBody().toString()));
			expression.append(")");
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
