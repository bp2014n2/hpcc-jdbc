package de.hpi.hpcc.parsing;

import java.io.StringReader;

import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.drop.Drop;

public class SQLParserDrop extends SQLParser {

	protected SQLParserDrop(String sql) {
		super(sql);
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
