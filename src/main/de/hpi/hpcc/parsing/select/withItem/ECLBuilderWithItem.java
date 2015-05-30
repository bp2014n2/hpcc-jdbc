package de.hpi.hpcc.parsing.select.withItem;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.select.ECLSelectParser;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.visitor.ECLNameParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;

public class ECLBuilderWithItem extends ECLBuilderSelect {

	private WithItem withItem;
	ECLSelectParser selectParser;

	public ECLBuilderWithItem(WithItem withItem, ECLLayouts eclLayouts) {
		super(withItem.getSelectBody(), eclLayouts);
		this.withItem = withItem;
	}

	@Override
	public String generateECL() {
		StringBuilder eclCode = new StringBuilder();
		eclCode.append(withItem.getName()+"_table := ");
		selectParser = new ECLSelectParser(eclLayouts);
		eclCode.append(selectParser.parse(withItem.getSelectBody())+";\n");
		eclCode.append(generateTempIndex());
		return eclCode.toString();
	}
	/*
	 * y := INDEX(y_table, {patient_num}, {}, '~i2b2demodata::y_idx_tmp');
	 * BUILD(y, SORT ALL, OVERWRITE, EXPIRE(1));
	 */
	private String generateTempIndex() {
		List<String> parameterList = new ArrayList<String>();
		parameterList.add(withItem.getName()+"_table");
		List<String> columns = new ArrayList<String>();
		ECLSelectItemFinder finder = new ECLSelectItemFinder(eclLayouts);
    	List<SelectExpressionItem> selectItems = finder.find(select);
    	
		for (int i=0; i < selectItems.size(); i++) {
    		SelectExpressionItem selectItem = selectItems.get(i);
    		ECLNameParser namer = new ECLNameParser();
    		String name = namer.name(selectItem.getExpression());
    		if(selectItem.getAlias() != null) {
    			name = selectItem.getAlias().getName();
    		}
    		columns.add(name);
		}
		String keyedColumnList = ECLUtils.join(columns, ", ");
    	keyedColumnList = ECLUtils.encapsulateWithCurlyBrackets(keyedColumnList);
		parameterList.add(keyedColumnList); // keyed
		parameterList.add("{}"); //nonkeyed
		parameterList.add(ECLUtils.encapsulateWithSingleQuote("~"+eclLayouts.getPublicSchema()+"::"+withItem.getName()+"_idx_tmp"));
    
		String index = ECLUtils.join(parameterList, ", ");
    	index = ECLUtils.convertToIndex(index);
    	
    	
    	List<String> buildParameters = new ArrayList<String>();
    	buildParameters.add(withItem.getName());
    	//TODO: make accessible
    	//buildParameters.add("CLUSTER("+layouts.getTargetCluster()+")");
    	buildParameters.add("SORT ALL");
    	buildParameters.add("OVERWRITE");
    	buildParameters.add("EXPIRE(1)");
    	String build = ECLUtils.join(buildParameters, ", ");
    	build = ECLUtils.convertToBuild(build);
    	outputCount++;
    	
    	return withItem.getName() + " := " + index + ";\n" + build + ";\n";
	}

}
