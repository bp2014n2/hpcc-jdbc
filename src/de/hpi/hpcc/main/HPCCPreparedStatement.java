package de.hpi.hpcc.main;

import java.io.InputStream;
import java.io.Reader;
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
import java.util.logging.Level;

public class HPCCPreparedStatement extends HPCCStatement implements PreparedStatement{
    private String sqlStatement;

    public HPCCPreparedStatement(HPCCConnection connection, String sqlStatement) {
        super(connection);
        this.sqlStatement = sqlStatement;
    }

    public ResultSet executeQuery() {
    	execute();
        return result;
    }

    public boolean execute() {
    	replaceJdbcParameters();
    	return execute(sqlStatement);
	}
    
    private void replaceJdbcParameters() {
    	int parameterCount = sqlStatement.length() - sqlStatement.replace("?", "").length();
    	for (int i = 1; i <= parameterCount; i++) {
    		if (parameters.containsKey(i)) {
            	Object param = parameters.get(i);
            	sqlStatement = sqlStatement.replaceFirst("\\?", param.toString());
    		} else {
            	sqlStatement = sqlStatement.replaceFirst("\\?", "NULL");
    		}
    	}
	}

    public void setString(int parameterIndex, String x) {
        parameters.put(parameterIndex, HPCCJDBCUtils.ensureECLString(x));
    }
    
	public void setObject(int parameterIndex, Object x) throws SQLException {
		parameters.put(parameterIndex, x);
	}
    
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException{
        setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException{
    	setObject(parameterIndex, x);
    }
    
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
    	parameters.put(parameterIndex, null);
    }
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }
    
    //Standard setter doing all the same stuff
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.put(parameterIndex, x);
    }
    
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) {
        parameters.put(parameterIndex, x);
    }

	public void setTimestamp(int parameterIndex, Timestamp x)throws SQLException {
		parameters.put(parameterIndex, x);
	}

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        parameters.put(parameterIndex, x);
    }
    
    public void setArray(int parameterIndex, Array x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        parameters.put(parameterIndex, value);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        parameters.put(parameterIndex, value);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        parameters.put(parameterIndex, xmlObject);
    }
    
    public void clearParameters() throws SQLException {
        parameters.clear();
    }
    
    public ResultSetMetaData getMetaData() throws SQLException {
        return result != null ? result.getMetaData() : null;
    }
    
    //Unsuppported methods!!
    public void addBatch() throws SQLException {
        handleUnsupportedMethod("addBatch()");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader, int length)");
    }
    
    public int executeUpdate() throws SQLException{
		ResultSet rs = executeQuery();
		rs.last();
		return rs.getRow();
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
    
    //Override super method
    protected static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCPreparedStatement.class.getSimpleName()+": "+infoMessage);
	}
}
