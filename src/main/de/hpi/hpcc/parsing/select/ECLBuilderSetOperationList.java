package de.hpi.hpcc.parsing.select;

import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import de.hpi.hpcc.parsing.ECLLayouts;

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
		for (int i = 0; i<setOperationList.getSelects().size(); i++) {
			SelectBody sb = setOperationList.getSelects().get(i);
			
			if (i != 0) {
				//TOOD: append setOperation
				//eclCode.append(operationParser.parse(setOperationList.getOperations().get(i-1)));
			}
			
			eclCode.append(selectParser.parse(sb));
		}
		setOperationList.getSelects().size();
		return null;
	}

}
