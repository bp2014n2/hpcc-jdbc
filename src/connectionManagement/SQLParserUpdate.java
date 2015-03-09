package connectionManagement;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParserUpdate extends SQLParser {

	protected SQLParserUpdate(String sql) {
		super(sql);
		try {
			if (parserManager.parse(new StringReader(sql)) instanceof Update) {
				statement = parserManager.parse(new StringReader(sql));
			} 
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
	
	protected String getName() {
		return ((Update) statement).getTable().getName();
	}
	
	protected String getFullName() {
		return ((Update) statement).getTable().getFullyQualifiedName();
	}

}
