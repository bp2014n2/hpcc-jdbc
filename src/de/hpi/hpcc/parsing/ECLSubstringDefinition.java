package de.hpi.hpcc.parsing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECLSubstringDefinition {
	String column;
	String alias;
	int start;
	int count;
	String context = "";
	
	public ECLSubstringDefinition(String column, String alias, int start, int count) {
		this.column = column;
		this.alias = alias;
		this.start = start;
		this.count = count;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getEnd() {
		return start + count - 1;
	}
	public void setContext(String context) {
		Pattern pattern = Pattern.compile("\\s*(=|<|>|<=|>=)\\s*('?\\w+'?)",Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(context);
		if (matcher.find()) {
			String value = matcher.group(2);
			if (!(value.startsWith("'") && value.endsWith("'"))) context = context.replace(value, "'" + value + "'");
		}
		this.context = context;
	}
	public String getContext() {
		return this.context;
	}
	public String toString() {
		return getContext() == "" ? getAlias() + " := " + getColumn() + "[" + getStart() + ".." + getEnd() + "]" : getColumn() + "[" + getStart() + ".." + getEnd() + "]" + getContext();
	}
	public String toReplaceString() {
		return getContext() == "" ? getAlias() + " := " + getColumn() : getColumn() + getContext();
	}
	public String toSql() {
		return (getAlias() == null ? getColumn() : getColumn() + " as " + getAlias()) + getContext();
	}
}
