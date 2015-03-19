package connectionManagement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;

import org.w3c.dom.NodeList;

public class HPCCStatement implements Statement
{
    protected boolean                  closed        = false;
    protected String                   sqlQuery;
    protected HPCCConnection           connection;
    protected SQLWarning               warnings;
    protected HPCCResultSet            result        = null;
    protected HPCCResultSetMetadata    resultMetadata = null;

    protected HPCCDatabaseMetaData     databaseMetadata;
    protected ECLEngine                eclQuery = null;
    public static final String         resultSetName = "HPCC Result";

    public HPCCStatement(HPCCConnection connection){
        traceOutLine("Creating Statement");
        this.connection = (HPCCConnection)connection;
        this.databaseMetadata = connection.getDatabaseMetaData();
    }

    protected void processQuery(String query){
        try{
        	traceOutLine("Attempting to process sql query: " + query);
            if (!this.closed){
                eclQuery = new ECLEngine(databaseMetadata, connection.getProperties(), query);
                eclQuery.generateECL();
                resultMetadata = new HPCCResultSetMetadata(eclQuery.getExpectedRetCols(), resultSetName);
            } else {
                throw new SQLException("HPCCPreparedStatement closed, cannot execute query");
            }
        }
        catch (SQLException e){
            HPCCJDBCUtils.traceoutln(Level.SEVERE, e.getLocalizedMessage());
            if (warnings == null){
                warnings = new SQLWarning();
            }
            warnings.setNextException(e);
            eclQuery = null;
        }
    }

    protected ResultSet executeHPCCQuery(HashMap<Integer, Object> params) throws SQLException{
        traceOutLine("executeQuery()");
        traceOutLine("\tAttempting to process sql query: " + sqlQuery);
        result = null;
        try{
        	if (!this.closed){
                NodeList rowList = eclQuery.execute(params);
                if (rowList != null){
                    result = new HPCCResultSet(this, rowList, resultMetadata);
                }
            } else {
            	traceOutLine(Level.SEVERE, "Statement is closed, cannot execute query");
            	throw new SQLException();
            }
        } catch (Exception e){
            throw convertToSQLExceptionAndAddWarn(e);
        }
        return result;
    }

    private SQLException convertToSQLExceptionAndAddWarn(Exception e)
    {
        SQLException sqlexcept = new SQLException(e.getLocalizedMessage());
        sqlexcept.setStackTrace(e.getStackTrace());

        if (warnings == null)
            warnings = new SQLWarning();

        warnings.setNextException(sqlexcept);

        return sqlexcept;
    }

    public ResultSet executeQuery(String sql) throws SQLException{
        traceOutLine("executeQuery(" + sql + ")");
        processQuery(sql);
        return executeHPCCQuery(null);
    }

	public void close() throws SQLException{
        traceOutLine("close( )");
        if (!closed){
            closed = true;
            connection = null;
            result = null;
            sqlQuery = null;
            databaseMetadata = null;
            eclQuery = null;
        }
    }

    public int getMaxRows() throws SQLException{
        try{
            traceOutLine("getMaxRows()");
            return Integer.parseInt(this.connection.getProperty("EclLimit"));
        }catch (Exception e){
            throw new SQLException("Could not determine MaxRows");
        }
    }

    public SQLWarning getWarnings() throws SQLException{
        traceOutLine("getWarnings()");
        return warnings;
    }

    public void clearWarnings() throws SQLException{
        traceOutLine("clearWarnings()");
        warnings = null;
    }

    public boolean execute(String sql) throws SQLException{
        sqlQuery = sql;
        processQuery(sql);
        return execute();
    }

    public boolean execute() throws SQLException{
        result = null;
        traceOutLine(": execute()");
        traceOutLine("\tAttempting to process sql query: " + sqlQuery);
        try{
            result = (HPCCResultSet) executeHPCCQuery(null);
        }catch (Exception e){
            //Unlikely to occur, but if it does, should report in sqlwarnings
            throw convertToSQLExceptionAndAddWarn(e);
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
    	traceOutLine("getFetchSize: -1");
        return -1;
    }
    
    public int getUpdateCount() throws SQLException{
        traceOutLine("getUpdateCount: -1");
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
	
	private static void traceOutLine(String infoMessage){
		traceOutLine(Level.INFO, infoMessage);
	}
	
	private static void traceOutLine(Level loggingLevel, String infoMessage){
		HPCCJDBCUtils.traceoutln(loggingLevel, HPCCStatement.class.getSimpleName()+": "+infoMessage);
	}
	
	private void handleUnsupportedMethod(String methodSignature) throws SQLException {
    	traceOutLine(Level.SEVERE, methodSignature+" is not supported yet.");
        throw new UnsupportedOperationException();
	}
}
