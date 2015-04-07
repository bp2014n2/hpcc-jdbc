package de.hpi.hpcc.main;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.logging.Level;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;


public class HPCCPreparedStatement extends HPCCStatement implements PreparedStatement{
    private String sqlStatement;
    private HashMap<Integer, Object> parameters    = new HashMap<Integer, Object>();

    public HPCCPreparedStatement(HPCCConnection connection, String sqlStatement, String name) throws SQLException {
        super(connection, name);
        CCJSqlParser parser = new CCJSqlParser(new StringReader(sqlStatement));
        try {
			parser.Statement();
		} catch (ParseException parseException) {
			log(Level.SEVERE, "Unable to parse PreparedStatement (not valid)!");
			throw new SQLException();
		}
        this.sqlStatement = sqlStatement;
    }

	public ResultSet executeQuery() throws SQLException {
    	execute();
        return result;
    }

    public boolean execute() throws SQLException {
    	replaceJdbcParameters();
    	return execute(sqlStatement);
	}
    
    private void replaceJdbcParameters() {
    	int parameterCount = getParameterCount();
    	if(parameterCount > parameters.size()) {
    		log(Level.WARNING, "You have not provided enough parameters. (Needed: "+parameterCount+", given: "+parameters.size()+")");
    	}
    		
    	for (int i = sqlStatement.length()-1; i >= 0; i--) {
    		if (!sqlStatement.substring(0,i+1).contains("?")) {
    			break;
    		}
        	if (sqlStatement.charAt(i)=='?') {
        		if (parameters.containsKey(parameterCount)) {
        			Object param = parameters.get(parameterCount--);
           			if (param != null) {
           				if (param instanceof String) {
           					param = ((String) param).replace("\n", "");
           					param = ((String) param).replace("\\\'", "\"");
           					param = ((String) param).replace("\\", "\\\\");
           				}
                   		if (param instanceof Timestamp) {
                   			param = "'"+param.toString()+"'";
                   		}
           				sqlStatement = new StringBuilder(sqlStatement).replace(i, i+1, param.toString()).toString();
                   	} else {
                   		sqlStatement = new StringBuilder(sqlStatement).replace(i, i+1, "NULL").toString();
                   	}
        		} else {
        			parameterCount--;
       				sqlStatement = new StringBuilder(sqlStatement).replace(i, i+1, "NULL").toString();
           		}	
       		}
       	}  	
	}
    
//    private String ensureCorrectFormat(String string) {
//    	string = replaceEscapingQuotes(string);
//		return '\'' + string + '\'';
//	}
//
//    private String replaceEscapingQuotes(String string) {
//		return string.replace("\'\'", "\\\'");
//	}
//
//	private final static Pattern QUOTEDSTRPATTERN = Pattern.compile(
//            "\\s*(\"|\')(.*?)(\"|\')\\s*",Pattern.DOTALL);
//
//    private static String handleQuotedString(String quotedString) {
//        if (quotedString == null || quotedString.length() <= 0)
//            return "";
//
//        Matcher matcher = QUOTEDSTRPATTERN.matcher(quotedString);
//
//        if(matcher.matches() )
//            return matcher.group(2).trim();
//        else
//            return quotedString;
//    }
//    
////    private static String ensureECLString(String instr) {
////        return '\'' + replaceSQLwithECLEscapeChar(handleQuotedString(instr)) + '\'';
////    }
//    
//    private final static  String eclescaped = "\\\'";
//    private final static Pattern SQLESCAPEDPATTERN = Pattern.compile(
//            "(.*)(\'\')(.*)(\'\')(.*)",Pattern.DOTALL);
//    
//    private static String replaceSQLwithECLEscapeChar(String quotedString) {
//        if (quotedString == null)
//            return "";
//
//        Matcher m = SQLESCAPEDPATTERN.matcher(quotedString);
//
//        String replaced;
//        if (m.matches())
//        {
//            replaced = m.group(1) + eclescaped + m.group(3) + eclescaped + m.group(5);
//        }
//        else
//        {
//            replaced = quotedString.replace("\'", "\\\'");
//        }
//
//        return replaced;
//    }
    
    private int getParameterCount() {
    	return (sqlStatement.length() - sqlStatement.replace("?", "").length());
	}
    
	public void setObject(int parameterIndex, Object x) {
		if(parameterIndex > getParameterCount()-1){
			log(Level.WARNING, "The given index ("+parameterIndex+") is too high. Value will be ignored.");
		} else {
			parameters.put(parameterIndex, x);
		}
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) {
        setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
    	setObject(parameterIndex, x);
    }
    
    public void setString(int parameterIndex, String x) {
//        setObject(parameterIndex, ensureCorrectFormat(x));
    	setObject(parameterIndex, x);
    }
    
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
 		setString(parameterIndex, new String(x));
    }
    
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
    	setObject(parameterIndex, null);
    }
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }
    
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) {
        setObject(parameterIndex, x);
    }

	public void setTimestamp(int parameterIndex, Timestamp x)throws SQLException {
		setObject(parameterIndex, x);
	}

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }
    
    public void clearParameters() throws SQLException {
        parameters.clear();
    }
    
    public ResultSetMetaData getMetaData() throws SQLException {
        return result != null ? result.getMetaData() : null;
    }
    
    public int executeUpdate() throws SQLException{
		return executeUpdate(sqlStatement);
    }
    
    //Override super method
    protected static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCPreparedStatement.class.getSimpleName()+": "+infoMessage);
	}
    
    //Unsuppported methods!!
    public void addBatch() throws SQLException {
        handleUnsupportedMethod("addBatch()");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader, int length)");
    }
    
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x, int length)");
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setUnicodeStream(int parameterIndex, InputStream x, int length)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x, int length)");
    }
    
    public void setDate(int parameterIndex, Date x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setDate(int parameterIndex, Date x, Calendar calendar)");
    }

    public void setTime(int parameterIndex, Time x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setTime(int parameterIndex, Time x, Calendar calendar)");
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setTimestamp(int parameterIndex, Timestamp x, Calendar calendar)");
    }
    
    public ParameterMetaData getParameterMetaData() throws SQLException {
        handleUnsupportedMethod("getParameterMetaData()");
		return null;
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        handleUnsupportedMethod("setRowId(int parameterIndex, RowId x)");
    }
    
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        handleUnsupportedMethod("setNCharacterStream(int parameterIndex, Reader value, long length)");
    }
    
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setClob(int parameterIndex, Reader reader, long length)");
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        handleUnsupportedMethod("setBlob(int parameterIndex, InputStream inputStream, long length)");
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setNClob(int parameterIndex, Reader reader, long length)");
    }
    
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x, long length)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x, long length)");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader, long length)");
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x)");
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader)");
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        handleUnsupportedMethod("setNCharacterStream(int parameterIndex, Reader value)");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setClob Not supported yet.");
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        handleUnsupportedMethod("setBlob Not supported yet.");
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setNClob Not supported yet.");
    }
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        handleUnsupportedMethod("setRef(int parameterIndex, Ref x)");
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        handleUnsupportedMethod("setBlob(int parameterIndex, Blob x)");
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        handleUnsupportedMethod("setClob(int parameterIndex, Clob x)");
    }
    
    public void setArray(int parameterIndex, Array x) throws SQLException {
        handleUnsupportedMethod("setArray(int parameterIndex, Array x)");
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        handleUnsupportedMethod("setURL(int parameterIndex, URL x)");
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
    	handleUnsupportedMethod("setNString(int parameterIndex, String value)");
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
    	handleUnsupportedMethod("setNClob(int parameterIndex, NClob value)");
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    	handleUnsupportedMethod("setSQLXML(int parameterIndex, SQLXML xmlObject)");
    }
}
