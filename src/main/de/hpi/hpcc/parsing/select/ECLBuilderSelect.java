package de.hpi.hpcc.parsing.select;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;

abstract public class ECLBuilderSelect extends ECLBuilder {
	
	protected SQLParserSelect sqlParser;
	protected Select select;
	
	
	public ECLBuilderSelect(Select select, ECLLayouts eclLayouts) {
		super(select, eclLayouts);
		this.select = select;
	}

	public ECLBuilderSelect(SelectBody selectBody, ECLLayouts eclLayouts) {
		super(null, eclLayouts);
		try {
			Statement statement = SQLParser.parse(selectBody.toString());
			if(statement instanceof Select) {
				this.select = (Select) statement;
			}
		} catch (HPCCException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * This method generates ECL code from a given SQL code. 
	 * Therefore it delegates the generation to the appropriate method, 
	 * depending on the type of the given SQL (e.g. Select, Insert or Update) 
	 * @param sql
	 * @return returns ECL code as String, including layout definitions and imports 
	 */
	abstract public String generateECL();

	@Override
	protected Select getStatement() {
		return select;
	}
}
