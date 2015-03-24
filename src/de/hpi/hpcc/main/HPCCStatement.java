package de.hpi.hpcc.main;

import java.sql.Connection;
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
	private static final Logger	logger = HPCCLogger.getLogger();
	private HPCCConnection connection;
	private ECLEngine eclEngine;
	
    protected boolean                  closed        = false;
    
    protected SQLWarning               warnings;
    protected HPCCResultSet            result        = null;
    protected HPCCResultSetMetadata    resultMetadata = null;

    
    public static final String         resultSetName = "HPCC Result";

    public HPCCStatement(HPCCConnection connection){
        this.connection = (HPCCConnection) connection;
        this.eclEngine = new ECLEngine(connection, connection.getDatabaseMetaData());
        log(Level.FINE, "Statement created");
    }
	
    public boolean execute(){
    	result = (HPCCResultSet) executeHPCCQuery(null);
	    return result != null;
	}

	public boolean execute(String sqlStatement){
		try{
        	log("Attempting to process sql query: " + sqlStatement);
            if (!this.closed){
            	eclEngine.generateECL(sqlStatement);
            } else {
                throw new SQLException("HPCCPreparedStatement closed, cannot execute query");
            }
        }
        catch (SQLException e){
            convertToSQLExceptionAndAddWarn(e);
            eclEngine = null;
        }
		return execute();
	}    
	
    public ResultSet executeQuery(String sql){
    	String query = sql.toLowerCase();

		ArrayList<String> blacklist = new ArrayList<String>();
		blacklist.add("qt_query_master");
		blacklist.add("qt_query_result_type");
		blacklist.add("qt_query_status_type");
		blacklist.add("qt_patient_set_collection");

		/*
		 * Could be dangerous if there is a query that contains the table names
		 * in a string etc
		 */
		for (String psqlQuery : blacklist) {
			if (query.contains(psqlQuery)) {
				try {
					Class.forName("org.postgresql.Driver");
					Connection connection = (Connection) DriverManager.getConnection("jdbc:postgresql://54.93.194.65/i2b2",	"i2b2demodata", "demouser");
					/*
					 * create HPCCStatement object for single use SQL query
					 * execution
					 */
					Statement stmt = connection.createStatement();
					return stmt.executeQuery(sql);
				} catch (SQLException sqlException) {
					sqlException.printStackTrace();
				} catch (ClassNotFoundException classNotFoundException) {
					classNotFoundException.printStackTrace();
				}
				break;
			}
		}
    	        
    	execute(query);
        return result;
    }
	
    protected ResultSet executeHPCCQuery(HashMap<Integer, Object> params){
        result = null;
        try{
        	if (!this.closed){
                String eclCode = eclEngine.parseEclCode(params);
                connection.sendRequest(eclCode);
                NodeList rowList = eclEngine.parseDataset(connection.getInputStream(), System.currentTimeMillis());
                if (rowList != null){
                    result = new HPCCResultSet(this, rowList, new HPCCResultSetMetadata(eclEngine.getExpectedRetCols(), resultSetName));
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

	public void close() throws SQLException{
        if (!closed){
            closed = true;
            connection = null;
            result = null;
            eclEngine = null;
        }
        log(Level.FINE, "Statement closed");
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

    private void convertToSQLExceptionAndAddWarn(Exception e){
        SQLException sqlexcept = new SQLException(e.getLocalizedMessage());
        sqlexcept.setStackTrace(e.getStackTrace());

        if (warnings == null)
            warnings = new SQLWarning();

        warnings.setNextException(sqlexcept);
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
    	log(Level.FINE, "Returning warnings");
        return warnings;
    }

    public void clearWarnings() throws SQLException{
    	log(Level.FINE, "Clearing warnings");
        warnings = null;
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
    
	//Unsupported Methods!!
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
}
