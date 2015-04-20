package de.hpi.hpcc.parsing.drop;

import java.io.StringReader;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	public SQLParserDrop(String sql, ECLLayouts layouts) {
		super(sql, layouts);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Drop) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	public String getName() {
		return ((Drop) statement).getName();
	}
	
	public String getFullName() {
		return "i2b2demodata::"+getName();
	}

}
