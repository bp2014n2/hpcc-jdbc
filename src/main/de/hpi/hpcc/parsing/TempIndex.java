package de.hpi.hpcc.parsing;

import java.util.ArrayList;
import java.util.List;

public class TempIndex {
	
	private List<Tuple<String, String>> keyedColumns;
	private List<Tuple<String, String>> nonKeyedColumns;
	
	
	public List<Tuple<String, String>> getKeyedColumns() {
		return keyedColumns;
	}

	public void setKeyedColumns(List<Tuple<String, String>> keyedColumns) {
		this.keyedColumns = keyedColumns;
	}

	public List<Tuple<String, String>> getNonKeyedColumns() {
		return nonKeyedColumns;
	}

	public void setNonKeyedColumns(List<Tuple<String, String>> nonKeyedColumns) {
		this.nonKeyedColumns = nonKeyedColumns;
	}

	public List<String> getKeyedColumnNames() {
		List<String> names = new ArrayList<String>();
		if (keyedColumns == null) return null;
		for (Tuple<String, String> column : keyedColumns) {
			names.add(column.left);
		}
		return names;
	}
	
	public List<String> getNonKeyedColumnNames() {
		List<String> names = new ArrayList<String>();
		if (nonKeyedColumns == null) return null;
		for (Tuple<String, String> column : nonKeyedColumns) {
			names.add(column.left);
		}
		return names;
	}

	public List<String> getAllIndexColumns() {
		List<String> columnNames = new ArrayList<String>();
		for (Tuple<String, String> column : keyedColumns) {
			columnNames.add(column.left);
		}
		for (Tuple<String, String> column : nonKeyedColumns) {
			columnNames.add(column.left);
		}
		return columnNames;
	}
	
	public String getRecordDefinition() {
		String record = "RECORD ";
		for (Tuple<String, String> column : keyedColumns) {
			record += column.left + " " + column.right + "; ";
		}
		for (Tuple<String, String> column : nonKeyedColumns) {
			record += column.left + " " + column.right + "; ";
		}
		
		record += "END;";
		return record;
	}
}
