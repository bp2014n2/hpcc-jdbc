package de.hpi.hpcc.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.w3c.dom.NodeList;

import de.hpi.hpcc.logging.HPCCLogger;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLParser;
import de.hpi.hpcc.parsing.SQLParser;

public class HPCCStatement implements Statement{
	protected static final Logger logger = HPCCLogger.getLogger();
	protected HPCCConnection connection;
	protected ECLParser parser;
    protected boolean closed = false;
    protected SQLWarning warnings;
    protected ResultSet result = null;
    protected String name;
    protected String sqlStatement;
    private boolean federatedDatabase = true;
    private Statement postgreSQLStatement;
    
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

    public HPCCStatement(HPCCConnection connection, String name){
    	this.name = name;
        this.connection = (HPCCConnection) connection;
        this.connection.addName(name);
        log("Statement created");
        Long difference = System.nanoTime()-HPCCDriver.beginTime;
		HPCCJDBCUtils.traceoutln(Level.INFO, "created statement at: "+difference/1000000);
    }
    
	public boolean execute(String sqlStatement) throws SQLException {
		HPCCJDBCUtils.traceoutln(Level.INFO, "currentQuery: "+sqlStatement);
		Long difference = System.nanoTime()-HPCCDriver.beginTime;
		HPCCJDBCUtils.traceoutln(Level.INFO, "started query at: "+difference/1000000);
		this.sqlStatement = sqlStatement;
		if (this.closed){
			log(Level.SEVERE, "Statement is closed! Cannot execute query!");
			if (warnings == null){
                warnings = new SQLWarning();
			}
            warnings.setNextException(new SQLException());
		}
		result = null;
		if (checkFederatedDatabase(sqlStatement)) {
			boolean result = executeQueryOnPostgreSQL(sqlStatement); 
			difference = System.nanoTime()-HPCCDriver.beginTime;
			HPCCJDBCUtils.traceoutln(Level.INFO, "finished Postgresql query at: "+difference/1000000);
			return result;
		} else {
			boolean result = executeQueryOnHPCC(sqlStatement);
			difference = System.nanoTime()-HPCCDriver.beginTime;
			HPCCJDBCUtils.traceoutln(Level.INFO, "finished HPCC query at: "+difference/1000000);
			return result;
		}
	}
	
	protected Statement createPostgreSQLStatement() throws SQLException {
		if(postgreSQLStatement == null) {
			postgreSQLStatement = this.connection.getPostgreSQLConnection().createStatement();
		}
		return postgreSQLStatement;
	}
	
	private boolean checkFederatedDatabase(String sqlStatement) throws SQLException {
		ECLLayouts eclLayouts = new ECLLayouts(connection.getDatabaseMetaData());
		SQLParser sqlParser = SQLParser.getInstance(sqlStatement, eclLayouts);
		Set<String> tables = sqlParser.getAllTables();
		
		return HPCCDriver.isPostgreSQLAvailable() && federatedDatabase && !whiteList.containsAll(tables);
	}
	

	
	private boolean executeQueryOnHPCC(String sqlStatement) throws SQLException {
		try {
			HPCCJDBCUtils.traceoutln(Level.INFO, "Query sent to HPCC: "+sqlStatement);
			ECLLayouts layouts = new ECLLayouts(connection.getDatabaseMetaData());
			this.parser = new ECLParser(layouts);
			NodeList rowList = null;
			for(String query : parser.parse(sqlStatement)) {
				HPCCJDBCUtils.traceoutln(Level.INFO, "sent query to hpcc at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
				connection.sendRequest(query);
				HPCCJDBCUtils.traceoutln(Level.INFO, "finished query at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
				rowList = connection.parseDataset(connection.getInputStream(), System.currentTimeMillis());
				HPCCJDBCUtils.traceoutln(Level.INFO, "finished parsing dataset at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
				
			}
			
			if (rowList != null) {
				HPCCJDBCUtils.traceoutln(Level.INFO, "started creating resultset at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
				result = new HPCCResultSet(this, rowList, new HPCCResultSetMetadata(parser.getExpectedRetCols(),	"HPCC Result"));
				HPCCJDBCUtils.traceoutln(Level.INFO, "finished creating resultset at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
			}
			return result != null;
		} catch (HPCCException exception) {
			exception.printStackTrace();
			this.close();
			throw exception;
		}
	}
	
	private boolean executeQueryOnPostgreSQL(String sqlStatement) throws SQLException {
		HPCCJDBCUtils.traceoutln(Level.INFO, "Query sent to PostgreSQL: "+sqlStatement);
		HPCCJDBCUtils.traceoutln(Level.INFO, "started query at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		Statement stmt = createPostgreSQLStatement();
		HPCCJDBCUtils.traceoutln(Level.INFO, "created statement at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		result = stmt.executeQuery(sqlStatement);
		HPCCJDBCUtils.traceoutln(Level.INFO, "finished query at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		return result != null;
	}
	
	private int executeUpdateOnPostgreSQL(String sqlStatement) throws SQLException {
		HPCCJDBCUtils.traceoutln(Level.INFO, sqlStatement);
		HPCCJDBCUtils.traceoutln(Level.INFO, "started update at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		Statement stmt = createPostgreSQLStatement();
		HPCCJDBCUtils.traceoutln(Level.INFO, "created statement at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		int result = stmt.executeUpdate(sqlStatement);
		HPCCJDBCUtils.traceoutln(Level.INFO, "finished update at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		return result;
	}

	public ResultSet executeQuery(String sqlQuery) throws SQLException { 
		HPCCJDBCUtils.traceoutln(Level.INFO, "started query at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
    	execute(sqlQuery);
    	HPCCJDBCUtils.traceoutln(Level.INFO, "finished query at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
        return result;
	}
	
	public void close() throws SQLException {
        if (!closed){
//        	result.close();
            closed = true;
            connection = null;
            result = null;
            parser = null;
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
    	HPCCJDBCUtils.traceoutln(Level.INFO, "started update at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
    	if (checkFederatedDatabase(sqlUpdate)) {
    		int i = executeUpdateOnPostgreSQL(sqlUpdate);
    		HPCCJDBCUtils.traceoutln(Level.INFO, "finished update at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
			return i;
		} else {
			if(executeQueryOnHPCC(sqlUpdate)) {
	    		result.last();
	    		int i = result.getRow();
	    		HPCCJDBCUtils.traceoutln(Level.INFO, "finished update at: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
	        	return i;
	    	}
		}
    	return 0;
    }
    
    public void setCursorName(String cursorName) throws SQLException{
    	if (!this.connection.isUniqueCursorName(name)) {
    		log(Level.SEVERE, "Cursor name not unique!");
    		throw new HPCCException();
    	} else {
    		this.name = cursorName;
    		this.connection.addName(cursorName);
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
	
	//TODO: Unsupported methods!!
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
