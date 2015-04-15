package de.hpi.hpcc.parsing;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	protected SQLParserDrop(String sql, ECLLayouts layouts) {
		super(sql, layouts);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Drop) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected String getName() {
		return ((Drop) statement).getName();
	}
	
	protected String getFullName() {
		return "i2b2demodata::"+getName();
	}

}
