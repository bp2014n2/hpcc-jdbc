package de.hpi.hpcc.parsing.select;

import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;

public class SQLParserWithItem extends SQLParserSelect {

	public SQLParserWithItem(WithItem withItem, ECLLayouts eclLayouts) {
		super(withItem, eclLayouts);
		// TODO Auto-generated constructor stub
	}

	@Override
	public LinkedHashSet<String> getAllColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SelectItem> getSelectItems() {
		// TODO Auto-generated method stub
		return null;
	}

}
