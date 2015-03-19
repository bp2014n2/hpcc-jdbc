package connectionManagement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.NodeList;

public class HPCCStatement implements Statement{
	private static final Logger logger = HPCCLogger.getLogger();
	
    protected boolean                  closed        = false;
    protected String                   sqlQuery;
    protected HPCCConnection           connection;
    protected SQLWarning               warnings;
    protected HPCCResultSet            result        = null;
    protected HPCCResultSetMetadata    resultMetadata = null;

    protected HPCCDatabaseMetaData     databaseMetadata;
    protected ECLEngine                eclEngine;
    public static final String         resultSetName = "HPCC Result";

    public HPCCStatement(HPCCConnection connection){
        this.connection = (HPCCConnection) connection;
        this.databaseMetadata = connection.getDatabaseMetaData();
        log(Level.FINE, "Statement and DatabaseMetaData created");
    }

    protected ResultSet executeHPCCQuery(HashMap<Integer, Object> params){
        log("Attempting to process sql query: " + sqlQuery);
        result = null;
        try{
        	if (!this.closed){
                NodeList rowList = eclEngine.execute(params);
                if (rowList != null){
                    result = new HPCCResultSet(this, rowList, resultMetadata);
                }
            } else {
            	log(Level.SEVERE, "Statement is closed, cannot execute query");
            	throw new SQLException();
            }
        } catch (Exception e){
            convertToSQLExceptionAndAddWarn(e);
        }
        return result;
    }

    private void convertToSQLExceptionAndAddWarn(Exception e){
        SQLException sqlexcept = new SQLException(e.getLocalizedMessage());
        sqlexcept.setStackTrace(e.getStackTrace());

        if (warnings == null)
            warnings = new SQLWarning();

        warnings.setNextException(sqlexcept);
    }

    public ResultSet executeQuery(String query){
    	try{
        	log("Attempting to process sql query: " + query);
            if (!this.closed){
                eclEngine = new ECLEngine(databaseMetadata, connection.getProperties(), query);
                eclEngine.generateECL();
                resultMetadata = new HPCCResultSetMetadata(eclEngine.getExpectedRetCols(), resultSetName);
            } else {
                throw new SQLException("HPCCPreparedStatement closed, cannot execute query");
            }
        }
        catch (SQLException e){
            HPCCJDBCUtils.traceoutln(Level.SEVERE, e.getLocalizedMessage()+"BLA");
            convertToSQLExceptionAndAddWarn(e);
            eclEngine = null;
        }
    	execute();
        return result;
    }

	public void close() throws SQLException{
        if (!closed){
            closed = true;
            connection = null;
            result = null;
            sqlQuery = null;
            databaseMetadata = null;
            eclEngine = null;
        }
        log(Level.FINE, "Statement closed");
    }

    public int getMaxRows() throws SQLException{
        try{
            log("getMaxRows()");
            return Integer.parseInt(this.connection.getProperty("EclLimit"));
        }catch (Exception e){
            throw new SQLException("Could not determine MaxRows");
        }
    }

    public SQLWarning getWarnings() throws SQLException{
        log("getWarnings()");
        return warnings;
    }

    public void clearWarnings() throws SQLException{
        log("clearWarnings()");
        warnings = null;
    }

    public boolean execute(String sql) throws SQLException{
        executeQuery(sql);
        return execute();
    }

    public boolean execute(){
        result = null;
        log("execute()");
        log("Attempting to process sql query: " + sqlQuery);
        try{
            result = (HPCCResultSet) executeHPCCQuery(null);
        }catch (Exception e){
            convertToSQLExceptionAndAddWarn(e);
        }
        return result != null;
    }

    public ResultSet getResultSet() throws SQLException{
        return result;
    }

 
    public int getFetchDirection() throws SQLException{
        return ResultSet.FETCH_FORWARD;
    }
    
    public Connection getConnection() throws SQLException{
        return connection;
    }
    
    public int getQueryTimeout() throws SQLException{
    	handleUnsupportedMethod("getQueryTimeout()");
    	return 0;
    }
    
    public void cancel() throws SQLException{
    	handleUnsupportedMethod("cancel()");
    }
    
    public void setQueryTimeout(int seconds) throws SQLException{
    	handleUnsupportedMethod("setQueryTimeout(int seconds)");
    }
    
    public int getFetchSize() throws SQLException{
    	log("getFetchSize: -1");
        return -1;
    }
    
    public int getUpdateCount() throws SQLException{
        log("getUpdateCount: -1");
        return -1;
    }
    
    public int executeUpdate(String sql) throws SQLException{
    	handleUnsupportedMethod("executeUpdate(String sql)");
    	return 0;
    }
    
    public int getMaxFieldSize() throws SQLException{
    	handleUnsupportedMethod("getMaxFieldSize()");
    	return 0;
    }

    public void setMaxFieldSize(int max) throws SQLException{
    	handleUnsupportedMethod("setMaxFieldSize(int max)");
    }
    
    public void setMaxRows(int max) throws SQLException{
		handleUnsupportedMethod("setMaxRows(int max)");
    }

    public void setEscapeProcessing(boolean enable) throws SQLException{
    	handleUnsupportedMethod("setEscapeProcessing(boolean enable)");
    }
    
    public void setCursorName(String name) throws SQLException{
    	handleUnsupportedMethod("setCursorName(String name)");
    }
    
    public boolean getMoreResults() throws SQLException{
		handleUnsupportedMethod("getMoreResults()");
		return false;
    }

    public void setFetchDirection(int direction) throws SQLException{
    	handleUnsupportedMethod("setFetchDirection(int direction)");
    }

    public void setFetchSize(int rows) throws SQLException{
    	handleUnsupportedMethod("setFetchSize(int rows)");
    }
    
    public int getResultSetConcurrency() throws SQLException{
    	handleUnsupportedMethod("getResultSetConcurrency()");
    	return 0;
    }

    public int getResultSetType() throws SQLException{
    	handleUnsupportedMethod("getResultSetType()");
    	return 0;
    }

    public void addBatch(String sql) throws SQLException{
    	handleUnsupportedMethod("addBatch(String sql)");
    }

    public void clearBatch() throws SQLException{
    	handleUnsupportedMethod("clearBatch()");
    }

    public int[] executeBatch() throws SQLException{
    	handleUnsupportedMethod("executeBatch()");
    	return null;
    }
    
    public boolean getMoreResults(int current) throws SQLException{
    	handleUnsupportedMethod("getMoreResults(int current)");
    	return false;
    }

    public ResultSet getGeneratedKeys() throws SQLException{
    	handleUnsupportedMethod("getGeneratedKeys()");
    	return null;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
    	handleUnsupportedMethod("executeUpdate(String sql, int autoGeneratedKeys)");
    	return 0;
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
    	handleUnsupportedMethod("executeUpdate(String sql, int[] columnIndexes)");
    	return 0;
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException{
    	handleUnsupportedMethod("executeUpdate(String sql, String[] columnNames)");
    	return 0;
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
    	handleUnsupportedMethod("execute(String sql, int autoGeneratedKeys)");
    	return false;
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException{
    	handleUnsupportedMethod("execute(String sql, int[] columnIndexes)");
    	return false;
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException{
    	handleUnsupportedMethod("execute(String sql, String[] columnNames)");
    	return false;
    }

    public int getResultSetHoldability() throws SQLException{
    	handleUnsupportedMethod("getResultSetHoldability()");
    	return 0;
    }

    public boolean isClosed() throws SQLException{
    	handleUnsupportedMethod("isClosed()");
    	return false;
    }

    public void setPoolable(boolean poolable) throws SQLException{
    	handleUnsupportedMethod("setPoolable(boolean poolable)");
    }

    public boolean isPoolable() throws SQLException{
    	handleUnsupportedMethod("isPoolable()");
    	return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException{
    	handleUnsupportedMethod("unwrap(Class<T> iface)");
    	return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException{
    	handleUnsupportedMethod("isWrapperFor(Class<?> iface)");
    	return false;
    }

	@Override
	public void closeOnCompletion() throws SQLException {
		handleUnsupportedMethod("closeOnCompletion()");
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		handleUnsupportedMethod("isCloseOnCompletion()");
		return false;
	}
	
	private static void log(String infoMessage){
		log(Level.INFO, infoMessage);
	}
	
	private static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCStatement.class.getSimpleName()+": "+infoMessage);
	}
	
	private void handleUnsupportedMethod(String methodSignature) throws SQLException {
		logger.log(Level.SEVERE, methodSignature+" is not supported yet.");
        throw new UnsupportedOperationException();
	}
}
