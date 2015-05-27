package de.hpi.hpcc.parsing.select.setOperationList;

import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.select.SQLParserSelect;

public class SQLParserSetOperationList extends SQLParserSelect {
	
	SetOperationList setOperationList;

	public SQLParserSetOperationList(SetOperationList list, ECLLayouts eclLayouts) {
		super(list, eclLayouts);
		this.setOperationList = list;
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

	@Override
	public Statement getStatement() {
		// TODO Auto-generated method stub
		return null;
	}

}
