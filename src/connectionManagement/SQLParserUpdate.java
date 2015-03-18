package connectionManagement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;

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
		return ((Update) statement).getTables().get(0).getName();
	}
	
	protected String getFullName() {
		return "i2b2demodata::"+getName();
	}
	
	protected Expression getWhere() {
		return ((Update) statement).getWhere();
	}
	
	protected LinkedHashSet<String> getAllCoumns() {
		String table = ((Update) statement).getTables().get(0).getName();
		return ECLLayouts.getAllColumns(table);
	}
	
	protected ArrayList<String> getColumns() {
		List<Column> columns = ((Update) statement).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columns) {
			columnNames.add(column.getColumnName());
		}
		return (ArrayList<String>) columnNames;
	}
	
	protected ArrayList<Expression> getExpressions() {
		return (ArrayList<Expression>) ((Update) statement).getExpressions();
	}

	protected Expression containsJoinCondition(Expression expression) {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}