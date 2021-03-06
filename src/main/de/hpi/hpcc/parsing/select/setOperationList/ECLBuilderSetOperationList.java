package de.hpi.hpcc.parsing.select.setOperationList;

import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.select.ECLSelectParser;
import de.hpi.hpcc.parsing.visitor.ECLSetOperationParser;

public class ECLBuilderSetOperationList extends ECLBuilderSelect {

	private SetOperationList setOperationList;

	public ECLBuilderSetOperationList(SetOperationList setOperationList, ECLLayouts eclLayouts) {
		super(setOperationList, eclLayouts);
		this.setOperationList = setOperationList;
	}

	@Override
	public String generateECL() {
		StringBuilder eclCode = new StringBuilder();
		ECLSelectParser selectParser = new ECLSelectParser(eclLayouts);
		ECLSetOperationParser operationParser = new ECLSetOperationParser();
		for (int i = 0; i<setOperationList.getSelects().size(); i++) {
			SelectBody sb = setOperationList.getSelects().get(i);
			
			if (i != 0) {
				//TOOD: append setOperation
				eclCode.append(operationParser.parse(setOperationList.getOperations().get(i-1)));
			}
			
			eclCode.append(selectParser.parse(sb));
		}
		setOperationList.getSelects().size();
		return eclCode.toString();
	}

}
