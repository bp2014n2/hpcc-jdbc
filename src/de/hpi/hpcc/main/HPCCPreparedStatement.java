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

    protected static final String      className = "HPCCPreparedStatement";
    private String sqlStatement;

    public HPCCPreparedStatement(HPCCConnection connection, String sqlStatement) {
        super(connection);
        this.sqlStatement = sqlStatement;
    }

    public ResultSet executeQuery(){
    	execute();
        return result;
    }

    private void replaceJdbcParameters() {
    	for (int i = 1; i <= parameters.size(); i++) {
        	Object param = parameters.get(i);
        	sqlStatement = sqlStatement.replaceFirst("\\?", param.toString());
    	}
	}

    public boolean execute(){
    	replaceJdbcParameters();
    	return execute(sqlStatement);
	}
    
    public void setBoolean(int parameterIndex, boolean x) throws SQLException{
        log(Level.FINEST,"setBoolean(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException{
        log(Level.FINEST, "setByte(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException{
        log(Level.FINEST,"setShort(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException
    {
        log(Level.FINEST, "setInt(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException
    {
        log(Level.FINEST, "setLong(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        log(Level.FINEST, "setFloat(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        log(Level.FINEST, "setDouble(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        log(Level.FINEST, "setBigDecimal(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException
    {
        log(Level.FINEST, "setString(" + parameterIndex + ", " + x + " )");
        try
        {
        	parameters.put(parameterIndex, HPCCJDBCUtils.ensureECLString(x));
        }
        catch (Exception e)
        {
            throw new SQLException("Cannot setString: " + e.getLocalizedMessage());
        }
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        log(Level.FINEST, "setBytes(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        log(Level.FINEST, "setDate(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) {
        log(Level.FINEST, "setTime(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

	public void setTimestamp(int parameterIndex, Timestamp x)throws SQLException {
		parameters.put(parameterIndex, x);
	}

    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        log(Level.FINEST, "setNull(" + parameterIndex + ", " + sqlType + " )");

        /*if( this.eclQuery.getQueryType() == SQLType.CALL)
            parameters.put(parameterIndex, "");
        else
            throw new SQLException("NULL cannot be represented in ECL.");*/
    }
    
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x != null) {
			parameters.put(parameterIndex, x);
		} else {
			setNull(parameterIndex, java.sql.Types.OTHER);
		}
	}
    
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException{
        setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException{
    	setObject(parameterIndex, x);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        log(Level.FINEST, "setRef(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        log(Level.FINEST, "setBlob(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        log(Level.FINEST, "setClob(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        log(Level.FINEST, "setArray(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        log(Level.FINEST, "getMetaData( )");
        return result != null ? result.getMetaData() : null;
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        log(Level.FINEST, "setNull(" + parameterIndex + ", " + sqlType + " )");
        setNull(parameterIndex, sqlType);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        log(Level.FINEST, "setURL(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        log(Level.FINEST, "setNString(" + parameterIndex + ", " + value + " )");
        parameters.put(parameterIndex, value);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        log(Level.FINEST, "setNClob(" + parameterIndex + ", " + value + " )");
        parameters.put(parameterIndex, value);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        log(Level.FINEST, "setSQLXML(" + parameterIndex + ", " + xmlObject + " )");
        parameters.put(parameterIndex, xmlObject);
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
    
    public int executeUpdate() throws SQLException{
        handleUnsupportedMethod("executeUpdate()");
		return 0;
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
}
