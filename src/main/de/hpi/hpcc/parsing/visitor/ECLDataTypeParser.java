package de.hpi.hpcc.parsing.visitor;

import java.util.List;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class ECLDataTypeParser extends FullVisitorAdapter {

	private String dataType = "STRING50";
	private ECLLayouts layouts;
	private SQLParser sqlParser;

	public ECLDataTypeParser(ECLLayouts layouts, SQLParser sqlParser) {
		this.layouts = layouts;
		this.sqlParser = sqlParser;
	}
	
	public String parse(Expression expression) {
		dataType = "STRING50";
		expression.accept(this);
		return dataType;
	}
	
	@Override
	public void visit(Function function) {
		String functionName = function.getName().toLowerCase();
		switch (functionName) {
		case "substring": dataType = "STRING50"; break;
		case "sum":
		case "count":
		default: dataType = "INTEGER8"; break;
		}
	}

	@Override
	public void visit(LongValue longValue) {
		dataType = "INTEGER5";
	}

	@Override
	public void visit(StringValue stringValue) {
		dataType = "STRING50";
	}

	@Override
	public void visit(Column tableColumn) {
		ECLFromItemsCollector collector = new ECLFromItemsCollector();
		List<FromItem> fromItems = collector.collect(sqlParser.getStatement());
		for(FromItem fromItem : fromItems) {
			ECLFromItemColumnFinder finder = new ECLFromItemColumnFinder(fromItem, layouts);	
			if(finder.contains(tableColumn)) {
				Table table = (Table) finder.fromItem;
				Expression expression = finder.expression;
				if (expression instanceof Column) {
					dataType = layouts.getECLDataType(table.getName(), expression.toString());
					break;
				}
				expression.accept(this);
			} 
		}
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		switch (aexpr.getName().toLowerCase()) {
		case "row_number": dataType = "INTEGER5"; break;
		default: dataType = "INTEGER5"; break;
		}
	}
}
