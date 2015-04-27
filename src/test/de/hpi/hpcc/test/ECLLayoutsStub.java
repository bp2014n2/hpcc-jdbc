package de.hpi.hpcc.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCDatabaseMetaData;
import de.hpi.hpcc.parsing.ECLLayouts;

public class ECLLayoutsStub extends ECLLayouts {

	public ECLLayoutsStub(HPCCDatabaseMetaData dbMetadata) {
		super(dbMetadata);
		// TODO Auto-generated constructor stub
	}

	Map<String, String> layouts = new HashMap<String, String>();
	
	public void setLayout(String table, String layout) {
		Matcher matcher = Pattern.compile("record\\s+(.*)\\s+end;", Pattern.CASE_INSENSITIVE).matcher(layout);
		if (matcher.find()) {
			layout = matcher.group(1);
			layouts.put(table.toLowerCase(), layout);
		}
		
	}
	
	@Override
	public String getECLDataType(String table, String columnName){
		String layout = getStubbedLayout(table);
		if(layout != null) {
			String[] columns = layout.split(";\\s+");
			for (String column : columns) {
				Matcher matcher = Pattern.compile("(\\w+)\\s+(\\w+)").matcher(column);
				if (matcher.find()) {
					String type = matcher.group(1);
					String name = matcher.group(2);
					if (name.equals(columnName)) {
						return type;
					}
				}
			}
		}
		return null;
	}
	
	private String getStubbedLayout(String table) {
		return layouts.get(table.toLowerCase());
	}
	
	@Override
	public LinkedHashSet<String> getAllColumns(String table) {	
		String layout = getStubbedLayout(table);
		
//		"RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;"
		if(layout != null) {
			String[] columns = layout.split(";\\s+");
			List<String> list = new ArrayList<String>();
			for (String column : columns) {
//				String name = Pattern.compile("\\w+\\s+(\\w+)").matcher(column).group(1);
				Matcher matcher = Pattern.compile("\\w+\\s+(\\w+)", Pattern.CASE_INSENSITIVE).matcher(column);
				if (matcher.find()) {
					String name = matcher.group(1);
					list.add(name);
				}
				
			}
			
			return new LinkedHashSet<String>(list);
		} else {
			//throw new Exception("DFUFile not found: "+table);
			return null;
		}
		
	}
	
	private boolean isInt(String type) {
		return type.toLowerCase().matches("(unsigned.*|integer.*)");
	}

	
	@Override
	public boolean isColumnOfIntInAnyTable(Set<String> tables, String column) {
		for (String table : tables) {
			String type = getECLDataType(table, column);
			if (type == null) {
				continue;
			}
			
			return isInt(type);
		}
		return false;
	}
	
	@Override
	public int getSqlTypeOfColumn (Set<String> tables, String column) {
		for (String table : tables) {
			String type = getECLDataType(table, column);
			if (type != null) {
				return getSqlType(type);		
			}
		}
		return java.sql.Types.OTHER;
	}

}