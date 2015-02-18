package connectionManagement;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ECLBuilder {
	
	private StringBuilder           eclCode = new StringBuilder();
	private final String				sql;
	
	private static final String			IMPORTSTD = "import std;\n";
    private static final String 		OUTPUTLIMIT = "#OPTION('outputlimit',2000);\n";
    private static final String			Layout_ConDim = "RECORD\n"+
										  "STRING700 concept_path;\n"+
										  "STRING50 concept_cd;\n"+
										  "STRING2000 name_char;\n"+
										  "STRING concept_blob;\n"+
										  "STRING25 update_date;\n"+
										  "STRING25 download_date;\n"+
										  "STRING25 import_date;\n"+
										  "STRING50 sourcesystem_cd;\n"+
										  "UNSIGNED5 upload_id;\n"+
										"END;\n";
    private static final String			HPCCEngine = "THOR";
    
    private List<String> tables;
    private List<String> orderBys;
    private List<String> selects;
    public List<String> getTables() {
		return tables;
	}

	public List<String> getOrderBys() {
		return orderBys;
	}

	public List<String> getSelects() {
		return selects;
	}

	
    
    
    HashMap<String, String> layouts = new HashMap<String, String> ();
	
	public ECLBuilder(String sql) {
		layouts.put("Layout_ConDim", Layout_ConDim);
		this.sql = sql;
	}
	
	public String generateECL() {
		SQLParser sqlParser = new SQLParser();
		sqlParser.generateObjectTree(sql);
		
		
		String layoutName = "Layout_ConDim";
    	String tmpTable = "test";
    	tables = sqlParser.getTables();
    	orderBys = sqlParser.getOrderBys();
    	selects = sqlParser.getSelects();
    	
    	eclCode.append(IMPORTSTD).append(OUTPUTLIMIT);
    	eclCode.append(layoutName).append(" := ").append(layouts.get(layoutName));
    	eclCode.append(tmpTable).append(" := ").append("DATASET(");
    	
    	eclCode.append("'~").append(tables.get(0)).append("'");
    	eclCode.append(", ").append(layoutName).append(",").append(HPCCEngine).append(");\n");
    	
    	eclCode.append("OUTPUT(");
    	StringBuilder tableString = new StringBuilder();
    	if (!orderBys.isEmpty()) {
    		tableString.append("SORT(").append(tmpTable).append(",");
    		tableString.append(orderBys.get(0));
    		for (int i = 1; i<orderBys.size(); i++) {
    			tableString.append(",").append(orderBys.get(i));
    		}
    		tableString.append(")");
    	} else {
    		tableString.append(tmpTable);
    	}
	
    	// is select * or select `any_column`?
    	if (!selects.isEmpty()) {
    		/*only Java 8
    		eclCode.append(",{").append(String.join(",", selects)).append("}");**/
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
    		String layout = layouts.get(layoutName);
    		String[] columns = layout.split(";");
    		for (String column : columns) {
    			if (column.split(" ").length > 1) selects.add(column.split(" ")[1]);
    		}
    	}
    	eclCode.append(");");
    	
    	System.out.println(eclCode.toString());
    	return(eclCode.toString());
    	
    	
	}
	

}
