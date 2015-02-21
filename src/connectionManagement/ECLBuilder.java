package connectionManagement;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;
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
    static HashMap<String, String> layouts = new HashMap<String, String> ();
 
    private FromItem table;
    private List<OrderByElement> orderBys;
    private List<SelectItem> selects;
   
	public ECLBuilder() {
		setupLayouts();
//		table = sqlParser.getTable();
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
			return "";
		}
	}
	
	private String generateSelectECL(String statement) {
		
    	orderBys = sqlParser.getOrderBys();
//    	selects = sqlParser.getSelects();
    	
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
    /*
     * TODO: remove brackets around tableString
     */	
    		String innerStatement = trimInnerStatement(table.toString());
    		eclCode.append(new ECLBuilder().generateECL(innerStatement));
    		eclCode.append(")");
    	}
    	
    	
    /*	TODO: CHECK FOR WHERE
     *  getExpressionECL()
     */
    	
    	Expression whereItems = sqlParser.getWhere();
    	String where = getExpressionECL(whereItems);
    	
    	
    	
    
    	/*	TODO: CHECK SELECTS
         *  for SelectExpressionItem
         *  	do getExpressionECL()
         */	
    	
    	/*	TODO: CHECK FOR GROUP BY
         *  not yet checkd
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
    	/*
    	eclCode.append("OUTPUT(");
    	StringBuilder tableString = new StringBuilder();
    	if (!orderBys.isEmpty()) {
    		tableString.append("SORT(").append(.split("::")[1]).append(",");
    		tableString.append(orderBys.get(0));
    		for (int i = 1; i<orderBys.size(); i++) {
    			tableString.append(",").append(orderBys.get(i));
    		}
    		tableString.append(")");
    	} else {
    		tableString.append(.split("::")[1]);
    	}
	
    	// is select * or select `any_column`?
    	if (!selects.isEmpty()) {
    		/*only Java 8
    		eclCode.append(",{").append(String.join(",", selects)).append("}");
//    		code for Java 7
    		eclCode.append(tableString.toString());
    		eclCode.append(",{");
    		eclCode.append(selects.get(0));
    				
       		for (int i = 1; i<selects.size(); i++) {
       			eclCode.append(",");
       			eclCode.append(selects.get(i));
       		}
       		eclCode.append("}");
    	} else {
    		// get all columns from layout
    		String layout = layouts.get(tables.get(0).split("::")[1]);
    		String[] columns = layout.split(";");
    		for (String column : columns) {
    			if (column.split(" ").length > 1) selects.add(column.split(" ")[1]);
    		}
    	}
    	eclCode.append(");");
    	*/
    	System.out.println("eclCode for "+sqlParser.statement.toString()+": \n"+eclCode.toString());
    	return(eclCode.toString());
	}
	private String getExpressionECL(Expression expressionItem) {
		StringBuilder expression = new StringBuilder();
		if (expressionItem instanceof LikeExpression) {
			
		} else if (expressionItem instanceof BinaryExpression) {
			expression.append(new ECLBuilder().getExpressionECL(((BinaryExpression) expressionItem).getLeftExpression()));
			expression.append(getSymbolOfExpression((BinaryExpression) expressionItem));
			expression.append(new ECLBuilder().getExpressionECL(((BinaryExpression) expressionItem).getRightExpression()));
		} else if (expressionItem instanceof InExpression) {
			expression.append(new ECLBuilder().getExpressionECL(((InExpression) expressionItem).getLeftExpression()));
			expression.append(" IN ");
			expression.append("(");
			if (((InExpression) expressionItem).getRightItemsList() instanceof ExpressionList) {
				for (Expression exp : ((ExpressionList) ((InExpression) expressionItem).getRightItemsList()).getExpressions()) {
					expression.append(new ECLBuilder().getExpressionECL(exp));
				}
			} else if (((InExpression) expressionItem).getRightItemsList() instanceof SubSelect) {
				expression.append("(");
				expression.append(new ECLBuilder().generateECL(((SubSelect) ((InExpression) expressionItem).getRightItemsList()).getSelectBody().toString()));
				expression.append(")");
			}
			expression.append(")");
		} else if (expressionItem instanceof Column) {
			expression.append(((Column) expressionItem).getColumnName());
		} else if (expressionItem instanceof SubSelect) {
			expression.append("(");
			expression.append(new ECLBuilder().generateECL(((SubSelect) expressionItem).getSelectBody().toString()));
			expression.append(")");
		} else if (expressionItem instanceof Function) {
			
		} 
		return expression.toString();
	}

	private String getSymbolOfExpression(BinaryExpression whereItems) {
		if (whereItems instanceof AndExpression) {
			return "&&";
		} else if (whereItems instanceof OrExpression) {
			return "||";
		} else if (whereItems instanceof MinorThan) {
			return "<";
		} else if (whereItems instanceof EqualsTo) {
			return "=";
		} else if (whereItems instanceof GreaterThan) {
			return ">";
		} else if (whereItems instanceof NotEqualsTo) {
			return "!=";
		} 
		return null;
	}

	private String trimInnerStatement(String innerStatement) {
//		System.out.println("innerStatement before trimming: "+innerStatement);
		if (innerStatement.charAt(0) == '(') {
			int end = innerStatement.lastIndexOf(")");
			innerStatement = innerStatement.substring(1, end);
		}
//		System.out.println("innerStatement after trimming: "+innerStatement);
		return innerStatement;
	}
}
