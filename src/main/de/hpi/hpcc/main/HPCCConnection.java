package de.hpi.hpcc.main;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.hpi.hpcc.logging.HPCCLogger;

public class HPCCConnection implements Connection{
	private Connection postgreSQLConnection;
    private boolean closed;
    private HPCCDatabaseMetaData metadata;
    private HPCCDriverProperties driverProperties;
    private SQLWarning warnings;
    private String catalog = HPCCJDBCUtils.HPCCCATALOGNAME;
    private HttpURLConnection httpConnection;
    private boolean autoCommit = true;
    private HashSet<String> statementNames = new HashSet<String>();
	private UUID sessionID;
	private final static String POSTGRESQL_USER = "i2b2demodata";
	private final static String POSTGRESQL_PASS = "demouser";
	private final static String POSTGRESQL_URL = "jdbc:postgresql://54.93.194.65/i2b2";
    
    protected static final Logger logger = HPCCLogger.getLogger();

    public HPCCConnection(HPCCDriverProperties driverProperties){

    	this.sessionID = UUID.randomUUID();
    	this.driverProperties = driverProperties;

        metadata = new HPCCDatabaseMetaData(driverProperties, this);        

        if (metadata != null && metadata.hasHPCCTargetBeenReached())
        {
            closed = false;
            HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection initialized - server: " + driverProperties.getProperty("ServerAddress"));
        }
        else
            HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection not initialized - server: " + driverProperties.getProperty("ServerAddress"));
    }
    
    public String getSessionID() {
    	return this.sessionID.toString().replace("-", "");
    }
    
    public Statement createStatement() throws SQLException {
        return new HPCCStatement(this, getUniqueName());
    }
    
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }
    
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    	return createStatement();
    }
    
    public PreparedStatement prepareStatement(String sqlStatement) throws SQLException {
    	return new HPCCPreparedStatement(this, sqlStatement, getUniqueName());
    }
    
    public PreparedStatement prepareStatement(String sqlStatement, int[] columnIndexes) throws SQLException {
    	return prepareStatement(sqlStatement);
    }
    
    public PreparedStatement prepareStatement(String sqlStatement, String[] columnNames) throws SQLException {
    	return prepareStatement(sqlStatement);
    }

    public PreparedStatement prepareStatement(String sqlStatement, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sqlStatement);
    }

    public PreparedStatement prepareStatement(String sqlStatement, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    	return prepareStatement(sqlStatement);
    }

    public void addName(String statementName) { 
    	this.statementNames.add(statementName);
    }
    
    private String getUniqueName() {
		int index = this.statementNames.size()+1;
		while(!isUniqueCursorName("Statement "+index)){
			index++;
		}
		return "Statement "+index;
	}

	public boolean isUniqueCursorName(String name) {
		for(String statementName : this.statementNames) {
			if(name.equals(statementName)) {
				return false;
			}
		}
		return true;
	}

	public URL generateUrl(){
    	String urlString = driverProperties.getProperty("Protocol")+(driverProperties.getProperty("WsECLDirectAddress") + ":"
                + driverProperties.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit")+"&cluster=" + driverProperties.getProperty("TargetCluster");

        URL hpccRequestUrl = HPCCJDBCUtils.makeURL(urlString);
        HPCCJDBCUtils.traceoutln(Level.INFO, "url for connection: "+hpccRequestUrl);		
        return hpccRequestUrl;
    }
    
    public void sendRequest(String eclCode){
    	int responseCode = -1;
//      TODO: make better: replace "+" in http request body since it is a reserved character representing a space character
    	String body = eclCode.replace("+", "%2B");
		try {
			httpConnection = createHPCCESPConnection(generateUrl());
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
			HPCCJDBCUtils.traceoutln(Level.INFO, "write to outputstream: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
			wr.write(body);
	        wr.flush();
	        wr.close();
	        HPCCJDBCUtils.traceoutln(Level.INFO, "close outputstream: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
			
	        responseCode = httpConnection.getResponseCode();
	        HPCCJDBCUtils.traceoutln(Level.INFO, "after response code: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
			
		} catch (IOException e) {
			System.out.println(e.getMessage());
			if (responseCode != 200){
	                throw new RuntimeException("HTTP Connection Response code: " + responseCode
	                        + "\nVerify access to WsECLDirect: " + httpConnection, e);
			} else if(e.getMessage().contains("Error in response:")) {
	            		System.out.println("Server response: "+e.getMessage().substring(e.getMessage().indexOf("'")+1,e.getMessage().length() - 1));
	        } else {
	        	throw new RuntimeException(e);	
	        }
		}
		HPCCJDBCUtils.traceoutln(Level.INFO, "end of sendRequest: "+((System.nanoTime()-HPCCDriver.beginTime)/1000000));
		
    }

    public NodeList parseDataset(InputStream xml, long startTime) throws HPCCException {
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	String expectedDSName = null;
        NodeList rowList = null;

        DocumentBuilder db;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new HPCCException("Failed to create DocumentBuilder");
		}
        Document dom;
		try {
			dom = db.parse(xml);
		} catch (SAXException | IOException e) {
			throw new HPCCException("Failed to parse dataset");
		}

        long elapsedTime = System.currentTimeMillis() - startTime;

        HPCCJDBCUtils.traceoutln(Level.INFO, "Total elapsed http request/response time in milliseconds: " + elapsedTime);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("Dataset");

        HPCCJDBCUtils.traceoutln(Level.INFO, "Parsing results...");

        HPCCNodeListAdapter nodes = new HPCCNodeListAdapter(dsList);
        
        int dsCount = 0;
        if (dsList != null && nodes.iterator().hasNext()){
            for (Node node : nodes) {
            	Element ds = (Element) node;
                String currentdatsetname = ds.getAttribute("name");
                if (expectedDSName == null || expectedDSName.length() == 0
                        || currentdatsetname.equalsIgnoreCase(expectedDSName))
                {
                    rowList = ds.getElementsByTagName("Row");
                    break;
                }
            }
        }
        else if (docElement.getElementsByTagName("Exception").getLength() > 0)
        {
            NodeList exceptionlist = docElement.getElementsByTagName("Exception");

            if (exceptionlist.getLength() > 0)
            {
                HPCCException resexception = null;
                NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

                for (int j = 0; j < currexceptionelements.getLength(); j++)
                {
                    Node exceptionelement = currexceptionelements.item(j);
                    if (exceptionelement.getNodeName().equals("Message"))
                    {
                        resexception = new HPCCException("HPCCJDBC: Error in response: \'"
                                + exceptionelement.getTextContent() + "\'");
                    }
                }
                if (dsList == null || dsList.getLength() <= 0)
                    throw resexception;
            }
        }
        else
        {
            // The root element is itself the Dataset element
            if (dsCount == 0)
            {
                rowList = docElement.getElementsByTagName("Row");
            }
        }
        HPCCJDBCUtils.traceoutln(Level.INFO,  "Finished Parsing results.");

        return rowList;
    }
    
    public HttpURLConnection createHPCCESPConnection(URL theurl) throws IOException {
    	return createHPCCESPConnection(theurl, Integer.parseInt(driverProperties.getProperty("ConnectTimeoutMilli")));
    }
    
    protected HttpURLConnection createHPCCESPConnection(URL url, int connectionTimeout) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestProperty("Authorization", this.createBasicAuth());
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(connectionTimeout);
        conn.setReadTimeout(Integer.parseInt(driverProperties.getProperty("ReadTimeoutMilli")));

        return conn;
    }

    public String createBasicAuth() {
        return "Basic " + HPCCJDBCUtils.Base64Encode((driverProperties.getProperty("username") + ":" + driverProperties.getProperty("password")).getBytes(), false);
    }

    public HPCCDatabaseMetaData getDatabaseMetaData() {
        return metadata;
    }
    
    public PreparedStatement prepareStatement(String query, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(query);
    }
    
	public InputStream getInputStream() {
		try {
			return httpConnection.getInputStream();
		} catch (IOException ioException) {
			log(Level.SEVERE, "Unable to get InputStream!\n"+ioException.getMessage());
		}
		return null;
	} 
	
	public Connection getPostgreSQLConnection() throws SQLException {
		if(this.postgreSQLConnection == null) {
			this.postgreSQLConnection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASS);
		}
		return this.postgreSQLConnection;
	}
	
//	ResultSet executePostgreSQLStatement(String sqlStatement) throws SQLException {
//		if(this.postgreSQLConnection == null) {
//			this.postgreSQLConnection = DriverManager.getConnection("jdbc:postgresql://54.93.194.65/i2b2",	"i2b2demodata", "demouser");
//		}
//		Statement stmt = postgreSQLConnection.createStatement();
//		return stmt.executeQuery(sqlStatement);
//	}

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public void close() throws SQLException {
        if(this.postgreSQLConnection != null) {
        	this.postgreSQLConnection.close();
        }
        this.closed = true;
//        httpConnection.disconnect();
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return metadata;
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    public boolean isValid(int timeout) throws SQLException {
        boolean success = false;
        if (!closed) {
            success = metadata.isTargetHPCCReachable(timeout);
        } else {
        	log("Connection already closed! Therefore it's not valid anymore.");
        }
        return success;
    }

    public String getClientInfo(String property) {
    	return driverProperties.getProperty(property);
    }

    public Properties getClientInfo() throws SQLException {
        return driverProperties;
    }
    
    public String getCatalog() throws SQLException{
        return catalog;
    }

    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    public void clearWarnings() throws SQLException {
    	warnings = null;
    }
    
	public int getNetworkTimeout() throws SQLException {
		return Integer.parseInt(driverProperties.getProperty("ConnectTimeoutMilli"));
	}
    
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		//TODO: refactor DatabaseMetaData -> set ConnectTimeoutMilli in connection
	}
    
    public int getTransactionIsolation() throws SQLException {
    	return TRANSACTION_NONE;
    }
    
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    	//TODO: use transactions
    	this.autoCommit = autoCommit;
    }
	
    //Logger methods
	private static void log(String infoMessage){
		log(Level.INFO, infoMessage);
	}
	
	private static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCConnection.class.getSimpleName()+": "+infoMessage);
	}
	
	private void handleUnsupportedMethod(String methodSignature) throws SQLException {
		logger.log(Level.SEVERE, methodSignature+" is not supported yet.");
        throw new UnsupportedOperationException();
	}
    
	//Unsupported Methods!!
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    	logger.log(Level.SEVERE, "setClientInfo(String name, String value) is not supported yet.");
    	throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    	logger.log(Level.SEVERE, "setClientInfo(Properties properties) is not supported yet.");
    	throw new SQLClientInfoException();
    }
    
    public void setReadOnly(boolean readOnly) throws SQLException {
    	handleUnsupportedMethod("setReadOnly(boolean readOnly)");
    }

    public void setTransactionIsolation(int level) throws SQLException  {
    	handleUnsupportedMethod("setTransactionIsolation(int level)");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    	handleUnsupportedMethod("prepareCall(String sql, int resultSetType, int resultSetConcurrency)");
		return null;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
    	handleUnsupportedMethod("getTypeMap()");
		return null;
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    	handleUnsupportedMethod("setTypeMap(Map<String, Class<?>> map)");
    }

    public void setHoldability(int holdability) throws SQLException {
    	handleUnsupportedMethod("setHoldability(int holdability)");
    }

    public int getHoldability() throws SQLException {
    	handleUnsupportedMethod("getHoldability()");
		return 0;
    }

    public Savepoint setSavepoint() throws SQLException {
    	handleUnsupportedMethod("setSavepoint()");
    	return null;
    }

    public Savepoint setSavepoint(String name) throws SQLException {
    	handleUnsupportedMethod("setSavepoint(String name)");
		return null;
    }

    public void rollback(Savepoint savepoint) throws SQLException {
    	handleUnsupportedMethod("rollback(Savepoint savepoint)");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    	handleUnsupportedMethod("releaseSavepoint(Savepoint savepoint)");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        handleUnsupportedMethod("prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
		return null;
    }

    public void commit() throws SQLException {
    	handleUnsupportedMethod("commit()");
    }

    public Clob createClob() throws SQLException {
    	handleUnsupportedMethod("createClob()");
		return null;
    }

    public Blob createBlob() throws SQLException {
    	handleUnsupportedMethod("createBlob()");
		return null;
    }

    public NClob createNClob() throws SQLException {
    	handleUnsupportedMethod("createNClob()");
    	return null;
    }

    public SQLXML createSQLXML() throws SQLException {
    	handleUnsupportedMethod("createSQLXML()");
    	return null;
    }
    
    public CallableStatement prepareCall(String sql) throws SQLException {
    	handleUnsupportedMethod("prepareCall(String sql)");
    	return null;
    }

    public String nativeSQL(String sql) throws SQLException {
    	handleUnsupportedMethod("nativeSQL(String sql)");
        return null;
    }

    public void rollback() throws SQLException {
        handleUnsupportedMethod("rollback()");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
       handleUnsupportedMethod("createArrayOf(String typeName, Object[] elements)");
       return null;
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
       handleUnsupportedMethod("createStruct(String typeName, Object[] attributes)");
       return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        handleUnsupportedMethod("unwrap(Class<T> iface)");
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        handleUnsupportedMethod("isWrapperFor(Class<?> iface)");
        return false;
    }

	public void abort(Executor arg0) throws SQLException {
		handleUnsupportedMethod("abort(Executor arg0)");
	}

	public String getSchema() throws SQLException {
		handleUnsupportedMethod("getSchema()");
		return null;
	}

	public void setSchema(String arg0) throws SQLException {
		handleUnsupportedMethod("setSchema(String arg0)");
	}
}
