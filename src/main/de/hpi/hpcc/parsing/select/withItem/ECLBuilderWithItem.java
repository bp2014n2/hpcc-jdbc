package de.hpi.hpcc.parsing.select.withItem;

import net.sf.jsqlparser.statement.select.WithItem;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.select.ECLSelectParser;

public class ECLBuilderWithItem extends ECLBuilderSelect {

	private WithItem withItem;

	public ECLBuilderWithItem(WithItem withItem, ECLLayouts eclLayouts) {
		super(withItem.getSelectBody(), eclLayouts);
		this.withItem = withItem;
	}

	@Override
	public String generateECL() {
		StringBuilder eclCode = new StringBuilder();
		eclCode.append(withItem.getName()+" := ");
		ECLSelectParser selectParser = new ECLSelectParser(eclLayouts);
		eclCode.append(selectParser.parse(withItem.getSelectBody())+";\n");
		return eclCode.toString();
	}

}
