package de.hpi.hpcc.parsing.visitor;

import net.sf.jsqlparser.schema.Column;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLExpressionParserExists extends ECLExpressionParser {

	public ECLExpressionParserExists(ECLLayouts eclLayouts) {
		super(eclLayouts);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void visit(Column tableColumn) {
		parsed = "";
		if (tableColumn.getTable() != null && tableColumn.getTable().getName() != null) {
			parsed = tableColumn.getTable().getName() + "."; 
		}
		parsed += tableColumn.getColumnName();
	}
}
