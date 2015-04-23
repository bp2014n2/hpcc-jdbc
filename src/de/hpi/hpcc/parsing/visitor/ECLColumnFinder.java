package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;

public class ECLColumnFinder extends FullVisitorAdapter {

	private List<String> columns = new ArrayList<String>();
	private List<String> tableNameAndAlias;
	private Statement statement;
	private ECLLayouts layouts;

	public List<String> find(Statement statement) {
		this.statement = statement;
		statement.accept(this);
		return columns;
	}
	
	public ECLColumnFinder(ECLLayouts layouts, List<String> tableNameAndAlias) {
		this.layouts = layouts;
		this.tableNameAndAlias = tableNameAndAlias;
	}

	@Override
	public void visit(Column tableColumn) {
		String columnName = tableColumn.getColumnName();
		String tableName = tableColumn.getTable().getName();
		if (tableName != null) {
			if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase()) && !columns.contains(columnName)) {
				columns.add(columnName);
			}
		} else {
			Pattern selectPattern = Pattern.compile("select\\s*(distinct\\s*)?((((count|sum|avg)\\(\\w*\\))|\\w*)\\s*,\\s*)*("+ columnName +"\\s*|(count|sum|avg)\\(\\s*"+ columnName +"\\s*\\))\\s*(as\\s*\\w*\\s*)?(,\\s*((count|sum|avg)\\(\\w*\\)|\\w*)\\s*(as\\s*\\w*\\s*)?)*from\\s*(\\w*\\.)?(\\w*)",Pattern.CASE_INSENSITIVE);
			Pattern wherePattern = Pattern.compile("from\\s*(\\w*\\.)?(\\w*)(\\s*\\w*)?\\s*where\\s*(\\(?(\\w*\\.)?\\w*\\s*((=|<=|>=)\\s*'?\\w*'?|in\\s*\\([\\w\\s\\\\'%\\.\\-]*\\))\\s*\\)?\\s*(and|or)\\s*)*\\(?" + columnName,Pattern.CASE_INSENSITIVE);
			Matcher selectMatcher = selectPattern.matcher(statement.toString());
			Matcher whereMatcher = wherePattern.matcher(statement.toString());
			if (selectMatcher.find()) {
				tableName = selectMatcher.group(14);
			} else if (whereMatcher.find()) {
				tableName = whereMatcher.group(2);
			}
			if (tableNameAndAlias.contains(tableName==null ? "" : tableName.toLowerCase())) {
				columns.add(columnName);
			}
		}
	}

}
