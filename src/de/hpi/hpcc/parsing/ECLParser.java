package de.hpi.hpcc.parsing;

import java.sql.SQLException;
import java.util.List;

import de.hpi.hpcc.main.HPCCColumnMetaData;

public class ECLParser {

	private ECLLayouts layouts;
	private ECLEngine engine;

	public ECLParser(ECLLayouts layouts) {
		this.layouts = layouts;
	}

	public String parse(String sql) throws SQLException {
		sql = convertToAppropriateSQL(sql);
		StringBuilder eclCode = new StringBuilder();
		
		eclCode.append("&eclText=\n");
		eclCode.append("#WORKUNIT('name', 'i2b2: "+eclMetaEscape(sql)+"');\n");
		ECLStatementParser typeParser = new ECLStatementParser(layouts);
		engine = typeParser.getEngine(sql);
		eclCode.append(engine.generateECL());
		eclCode.append("\n\n//"+eclMetaEscape(sql));
		return eclCode.toString();
	}
    
    private String convertToAppropriateSQL(String sql) throws SQLException {
    	if(sql.toLowerCase().contains("nextval")){
//			String sequence = sql.substring(sql.indexOf('(')+2, sql.indexOf(')')-1);
//			ECLEngine updateEngine = new ECLEngineUpdate(conn, dbMetadata);
//			conn.sendRequest(updateEngine.parseEclCode("update sequences set value = value + 1 where name = '"+sequence+"'"));
//			sql = "select value as nextval from sequences where name = '"+sequence+"'";
    		//TODO: implement in ONE Query
		}
		return sql;
	}
    
	private String eclMetaEscape(String sqlQuery) {
		sqlQuery = sqlQuery.replace("'", "\\'");
		sqlQuery = sqlQuery.replace("\n", " ");
		return sqlQuery;
	}

	public List<HPCCColumnMetaData> getExpectedRetCols() {
		return engine.getExpectedRetCols();
	}
	
}
