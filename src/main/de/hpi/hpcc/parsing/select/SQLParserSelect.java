package de.hpi.hpcc.parsing.select;

import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;

public abstract class SQLParserSelect extends SQLParser {

	protected Select select;
	
	public SQLParserSelect(SelectBody sql, ECLLayouts eclLayouts) {
		super(eclLayouts);
		// TODO Auto-generated constructor stub
	}

	public Statement getStatement() {
		return select;
	}
	abstract public LinkedHashSet<String> getAllColumns();
	abstract public List<SelectItem> getSelectItems();

}
