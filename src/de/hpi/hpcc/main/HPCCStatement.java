package de.hpi.hpcc.main;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.NodeList;

import de.hpi.hpcc.logging.HPCCLogger;
import de.hpi.hpcc.parsing.ECLEngine;

public class HPCCStatement implements Statement{
	protected static final Logger	logger = HPCCLogger.getLogger();
	protected HPCCConnection connection;
	protected ECLEngine eclEngine;
	
    protected boolean                  closed        = false;
    
    protected SQLWarning               warnings;
    protected ResultSet            result        = null;
    
    public static final String         resultSetName = "HPCC Result";

    public HPCCStatement(HPCCConnection connection){
        this.connection = (HPCCConnection) connection;
        this.eclEngine = new ECLEngine(connection, connection.getDatabaseMetaData());
        log("Statement created");
    }

	public boolean execute(String sqlStatement) throws SQLException {
		if (this.closed){
			log(Level.SEVERE, "Statement is closed! Cannot execute query!");
			if (warnings == null){
                warnings = new SQLWarning();
			}
            warnings.setNextException(new SQLException());
		}
		
		ArrayList<String> whiteList = new ArrayList<String>();
		whiteList.add("query_global_temp");
		whiteList.add("observation_fact");
		whiteList.add("provider_dimension");
		whiteList.add("dx");
		whiteList.add("master_query_global_temp");
		
		result = null;
		
		for (String table : whiteList) {
			if (sqlStatement.toLowerCase().contains(table)) {
				try {
					this.eclEngine = new ECLEngine(connection, connection.getDatabaseMetaData());
					long timeBefore = System.currentTimeMillis();
					String eclCode = eclEngine.parseEclCode(sqlStatement);
					long timeAfter = System.currentTimeMillis();
					long timeDifference = timeAfter-timeBefore;
					HPCCJDBCUtils.traceoutln(Level.INFO, "Time for parsing SQL to ECL: "+timeDifference+ " - "+sqlStatement);
					connection.sendRequest(eclCode);
					NodeList rowList = connection.parseDataset(connection.getInputStream(), System.currentTimeMillis());
					if (rowList != null) {
						result = new HPCCResultSet(this, rowList, new HPCCResultSetMetadata(eclEngine.getExpectedRetCols(),	resultSetName));
					}
					return result != null;
				} catch (Exception exception) {
					exception.printStackTrace();
					this.close();
				}
			}
		}
		
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = (Connection) DriverManager.getConnection("jdbc:postgresql://54.93.194.65/i2b2",	"i2b2demodata", "demouser");
			/*
			 * create HPCCStatement object for single use SQL query
			 * execution
			 */
			
			HPCCJDBCUtils.traceoutln(Level.INFO, "Query sent to PostgreSQL");
			Statement stmt = connection.createStatement();
			result = stmt.executeQuery(sqlStatement);
			return result != null;
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} catch (ClassNotFoundException classNotFoundException) {
			classNotFoundException.printStackTrace();
		}	
		return false;
	}    

	public ResultSet executeQuery(String sqlQuery) throws SQLException {      
    	execute(sqlQuery);
        return result;
	}
	
	public void close() throws SQLException {
        if (!closed){
//        	result.close();
            closed = true;
            connection = null;
            result = null;
            eclEngine = null;
        }
        log("Statement closed");
    }
	
	public ResultSet getResultSet() throws SQLException{
	    return this.result;
	}

	 
	public int getFetchDirection() throws SQLException{
		return ResultSet.FETCH_FORWARD;
	}
	    
	public Connection getConnection() throws SQLException{
		return this.connection;
	}
	
    public int getMaxRows() {
		return Integer.parseInt(this.connection.getProperty("EclLimit"));
    }

    public SQLWarning getWarnings() {
        return warnings;
    }

    public void clearWarnings() {
        warnings = null;
        log(Level.FINEST, "Warnings cleared");
    }
    
    //Methods for subclasses
	protected static void log(String infoMessage){
		log(Level.INFO, infoMessage);
	}
	
	protected static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCStatement.class.getSimpleName()+": "+infoMessage);
	}
	
	protected void handleUnsupportedMethod(String methodSignature) throws SQLException {
		logger.log(Level.SEVERE, methodSignature+" is not supported yet.");
        throw new UnsupportedOperationException();
	}    
	
	//Unsupported methods!!
	public int getFetchSize() throws SQLException{
    	log("getFetchSize: -1");
        return -1;
    }
    
    public int getUpdateCount() throws SQLException{
        log("getUpdateCount: -1");
        return -1;
    }
    
    public int getQueryTimeout() throws SQLException{
//    	handleUnsupportedMethod("getQueryTimeout()");
    	return 0;
    }
    
    public void cancel() throws SQLException{
    	handleUnsupportedMethod("cancel()");
    }
    
    public void setQueryTimeout(int seconds) throws SQLException{
    	/*
    	 * TODO: t.b.a.
    	 */
    }
    
    public int executeUpdate(String sql) throws SQLException{
    	executeQuery(sql);
    	if (result == null) {
    		return 0;
    	}
    	result.last();
    	return result.getRow();
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
//    	handleUnsupportedMethod("setCursorName(String name)");
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
}
