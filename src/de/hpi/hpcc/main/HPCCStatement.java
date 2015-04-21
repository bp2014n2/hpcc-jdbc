package de.hpi.hpcc.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.NodeList;

import de.hpi.hpcc.logging.HPCCLogger;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;

public class HPCCStatement implements Statement{
	protected static final Logger logger = HPCCLogger.getLogger();
	protected HPCCConnection connection;
	protected ECLEngine eclEngine;
    protected boolean closed = false;
    protected SQLWarning warnings;
    protected ResultSet result = null;
    protected String name;
    protected String sqlStatement;
    private static final List<String> whiteList = new ArrayList<String>() {
		private static final long serialVersionUID = 1L;
	{
    	add("query_global_temp");
		add("dx");
		add("master_query_global_temp");
		add("observation_fact");
		add("provider_dimension");
		add("visit_dimension");
		add("patient_dimension");
		add("modifier_dimension");
		add("concept_dimension");
		add("qt_patient_set_collection");
		add("qt_patient_env_collection");
		add("avk_fdb_t_leistungskosten");
    	
    }};
    
    private boolean federatedDatabase = false;

    public HPCCStatement(HPCCConnection connection, String name){
    	this.name = name;
        this.connection = (HPCCConnection) connection;
        this.connection.addToQueue(this);
        log("Statement created");
    }
    
	public boolean execute(String sqlStatement) throws SQLException {
		this.sqlStatement = sqlStatement;
		if (this.closed){
			log(Level.SEVERE, "Statement is closed! Cannot execute query!");
			if (warnings == null){
                warnings = new SQLWarning();
			}
            warnings.setNextException(new SQLException());
		}
		
		result = null;
		log("currentQuery: "+sqlStatement);
		
		String sqlStatementTemp = ECLEngine.escapeToAppropriateSQL(sqlStatement);
		ECLLayouts eclLayouts = new ECLLayouts(connection.getDatabaseMetaData());
		SQLParser sqlParser = SQLParser.getInstance(sqlStatementTemp, eclLayouts);
		List<String> tables = sqlParser.getAllTables();
		
		if (!federatedDatabase || whiteList.containsAll(tables)) {
			return sendQueryToHPCC(sqlStatement);
		}
		return sendQueryToPostgreSQL(sqlStatement);
	}
	
	private boolean sendQueryToHPCC(String sqlStatement) throws SQLException {
		try {
			this.eclEngine = ECLEngine.getInstance(connection, connection.getDatabaseMetaData(), sqlStatement);
			String eclCode = eclEngine.parseEclCode(sqlStatement);
			connection.sendRequest(eclCode);
			NodeList rowList;
			rowList = connection.parseDataset(connection.getInputStream(), System.currentTimeMillis());
			if (rowList != null) {
				result = new HPCCResultSet(this, rowList, new HPCCResultSetMetadata(eclEngine.getExpectedRetCols(),	"HPCC Result"));
			}
			return result != null;
		} catch (HPCCException exception) {
			exception.printStackTrace();
			this.close();
			throw exception;
		}
	}
	
	private boolean sendQueryToPostgreSQL(String sqlStatement) throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = (Connection) DriverManager.getConnection("jdbc:postgresql://54.93.194.65/i2b2",	"i2b2demodata", "demouser");
			log("Query sent to PostgreSQL");
			Statement stmt = connection.createStatement();
			result = stmt.executeQuery(sqlStatement);
			return result != null;
		} catch (ClassNotFoundException classNotFoundException) {
			throw new HPCCException("PostgreSQL driver not found");
		}
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
	
	protected String getSqlStatement() {
		return this.sqlStatement;
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
		return Integer.parseInt(this.connection.getClientInfo("EclLimit"));
    }

    public SQLWarning getWarnings() {
        return warnings;
    }

    public void clearWarnings() {
        warnings = null;
        log(Level.FINEST, "Warnings cleared");
    }
    
    public int getQueryTimeout() throws SQLException{
    	return this.connection.getNetworkTimeout();
    }
    
    public void setQueryTimeout(int seconds) throws SQLException{
    	this.connection.setNetworkTimeout(null, seconds);
    }
    
    public int executeUpdate(String sqlUpdate) throws SQLException{
    	if (execute(sqlUpdate)) {
    		result.last();
        	return result.getRow();
    	}
    	return 0;
    }
    
    public void setCursorName(String name) throws SQLException{
    	if (!this.connection.isUniqueCursorName(name)) {
    		log(Level.SEVERE, "Cursor name not unique!");
    		throw new HPCCException();
    	} else {
    		this.name = name;
    	}
    }
    
    public String getCursorName() {
    	return this.name;
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
    
    public void cancel() throws SQLException{
    	handleUnsupportedMethod("cancel()");
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
