package de.hpi.hpcc.parsing;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.main.HPCCColumnMetaData;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

public class ECLParser {

	private ECLLayouts layouts;
	private ECLEngine engine;

	public ECLParser(ECLLayouts layouts) {
		this.layouts = layouts;
	}
	
	public List<String> parse(String sql) throws SQLException {
		List<String> eclCodes = new ArrayList<String>();
		for(String query : convertToAppropriateSQL(sql)) {
			eclCodes.add(primitiveParse(query));
		}
		return eclCodes;
	}
    
    private List<String> convertToAppropriateSQL(String sql) throws SQLException {
    	List<String> queries = new ArrayList<String>();
    	Matcher matcher = Pattern.compile("select\\s+nextval\\(\\s*'(\\w+)'\\s*\\)", Pattern.CASE_INSENSITIVE).matcher(sql);
    	if(matcher.find()){
			String sequence = matcher.group(1);
			queries.add("update sequences set value = value + 1 where name = '"+sequence+"'");
			queries.add("select value as nextval from sequences where name = '"+sequence+"'");
    		//TODO: implement in ONE Query
		} else {
			queries.add(sql);
		}
		return queries;
	}

	private String primitiveParse(String sql) throws SQLException {
		StringBuilder eclCode = new StringBuilder();
		
		eclCode.append("&eclText=\n");
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sql)+"');\n");
		ECLStatementParser typeParser = new ECLStatementParser(layouts);
		engine = typeParser.getEngine(sql);
		String generatedECL = engine.generateECL();
		if (generatedECL == null) {
			return null;
		}
		eclCode.append(generatedECL);
		eclCode.append("\n\n//"+eclMetaEscape(sql));
		return eclCode.toString();
	}
    
	private String eclMetaEscape(String sqlQuery) {
		sqlQuery = sqlQuery.replace("\n", " ");
		sqlQuery = sqlQuery.replace("\\", "\\\\");
		sqlQuery = sqlQuery.replace("'", "\\'");
		return sqlQuery;
	}

	public List<HPCCColumnMetaData> getExpectedRetCols() {
		return engine.getExpectedRetCols();
	}

	public int getOutputCount() {
		return engine.getOutputCount();
	}
	
}
