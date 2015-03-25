/*##############################################################################

Copyright (C) 2011 HPCC Systems.

All rights reserved. This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

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
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HPCCConnection implements Connection
{
    private boolean              closed;
    private HPCCDatabaseMetaData metadata;
    private Properties           driverProperties;
    private Properties           clientInfo;
    private SQLWarning           warnings = null;
    private String               catalog = HPCCJDBCUtils.HPCCCATALOGNAME;
    private HttpURLConnection httpConnection;

    public HPCCConnection(HPCCDriverProperties driverProperties){
        this.driverProperties = driverProperties;
        metadata = new HPCCDatabaseMetaData(driverProperties, this);

        // TODO not doing anything w/ this yet, just exposing it to comply w/ API definition...
        clientInfo = new Properties();

        if (metadata != null && metadata.hasHPCCTargetBeenReached())
        {
            closed = false;
            HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection initialized - server: " + this.driverProperties.getProperty("ServerAddress"));
        }
        else
            HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection not initialized - server: " + this.driverProperties.getProperty("ServerAddress"));
    }
    
    public URL generateUrl(){
    	String urlString = driverProperties.getProperty("Protocol")+(driverProperties.getProperty("WsECLDirectAddress") + ":"
                + driverProperties.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit")+"&cluster=" + driverProperties.getProperty("TargetCluster");

        URL hpccRequestUrl = HPCCJDBCUtils.makeURL(urlString);
        
        return hpccRequestUrl;
    }
    
    public void sendRequest(String eclCode){
    	int responseCode = -1;
//      replace "+" and "?" in http request body since it is a reserved character representing a space character
    	String body = eclCode.replace("+", "%2B");
		try {
			httpConnection = createHPCCESPConnection(generateUrl());
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
			wr.write(body);
	        wr.flush();
	        wr.close();
	        responseCode = httpConnection.getResponseCode();
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
    }

    public NodeList parseDataset(InputStream xml, long startTime) throws Exception {
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	String expectedDSName = null;
        NodeList rowList = null;

        DocumentBuilder db = dbf.newDocumentBuilder();
        Document dom = db.parse(xml);

        long elapsedTime = System.currentTimeMillis() - startTime;

        HPCCJDBCUtils.traceoutln(Level.INFO, "Total elapsed http request/response time in milliseconds: " + elapsedTime);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("Dataset");

        HPCCJDBCUtils.traceoutln(Level.INFO, "Parsing results...");

        int dsCount = 0;
        if (dsList != null && (dsCount = dsList.getLength()) > 0){
            HPCCJDBCUtils.traceoutln(Level.INFO, "Results datsets found: " + dsList.getLength());

            // The dataset element is encapsulated within a Result element
            // need to fetch appropriate resulst dataset

            for (int i = 0; i < dsCount; i++)
            {
                Element ds = (Element) dsList.item(i);
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
                Exception resexception = null;
                NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

                for (int j = 0; j < currexceptionelements.getLength(); j++)
                {
                    Node exceptionelement = currexceptionelements.item(j);
                    if (exceptionelement.getNodeName().equals("Message"))
                    {
                        resexception = new Exception("HPCCJDBC: Error in response: \'"
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
    
    public HttpURLConnection createHPCCESPConnection(URL theurl) throws IOException{
    	return createHPCCESPConnection(theurl, Integer.parseInt(driverProperties.getProperty("ConnectTimeoutMilli")));
    }
    
    protected HttpURLConnection createHPCCESPConnection(URL url, int connectionTimeout) throws IOException{
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestProperty("Authorization", createBasicAuth(driverProperties.getProperty("username"), driverProperties.getProperty("password")));
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(connectionTimeout);
        conn.setReadTimeout(Integer.parseInt(driverProperties.getProperty("ReadTimeoutMilli")));

        return conn;
    }

    public static String createBasicAuth(String username, String passwd){
        return "Basic " + HPCCJDBCUtils.Base64Encode((username + ":" + passwd).getBytes(), false);
    }

    public Properties getProperties()
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: getProperties(  )");
        return driverProperties;
    }

    public String getProperty(String propname)
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: getProperty( " + propname + " )");
        return driverProperties.getProperty(propname, "");
    }

    public HPCCDatabaseMetaData getDatabaseMetaData()
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: getDatabaseMetaData(  )");
        return metadata;
    }

    public Statement createStatement() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection: createStatement(  )");
        return new HPCCStatement(this);
    }

    public PreparedStatement prepareStatement(String query) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCCConnection: prepareStatement( " + query + " )");
        HPCCPreparedStatement p = new HPCCPreparedStatement(this, query);
        SQLWarning prepstmtexcp = p.getWarnings();
        if (prepstmtexcp != null)
            throw (SQLException)prepstmtexcp.getNextException();

        return p;
    }

    public CallableStatement prepareCall(String sql) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,"HPCCConnection: prepareCall(string sql) Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: prepareCall(string sql) Not supported yet.");
    }

    public String nativeSQL(String sql) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: nativeSQL(string sql) Not supported yet.");
        return sql;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: setAutoCommit(boolean autoCommit) Not supported yet.");
    }

    public boolean getAutoCommit() throws SQLException
    {
        return true;
    }

    public void commit() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: commit Not supported yet.");
    }

    public void rollback() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: rollback Not supported yet.");
    }

    public void close() throws SQLException
    {
        closed = true;
    }

    public boolean isClosed() throws SQLException
    {
        return closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return metadata;
    }

    public void setReadOnly(boolean readOnly) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: setReadOnly Not supported yet.");
    }

    public boolean isReadOnly() throws SQLException
    {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST, "HPCCConnection: setCatalog.");
        this.catalog = catalog;
    }

    public String getCatalog() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST, "HPCCConnection: getCatalog()");
        return catalog;
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: settransactionisolation Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: settransactionisolation Not supported yet.");
    }

    public int getTransactionIsolation() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: getTransactionIsolation Not supported yet.");
        return 0;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: getWarnings");
        return warnings;
    }

    public void clearWarnings() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: clearWarnings.");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createStatement(resulttype, resultsetcon)##");
        return new HPCCPreparedStatement(this, null);
    }

    public PreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareStatement(" + query + ", resultsetype, resultsetcon)##");
        return new HPCCPreparedStatement(this, query);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException(
                "HPCCConnection: prepareCall(String sql, int resultSetType, int resultSetConcurrency) Not supported yet.");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: getTypeMap Not supported yet.");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: setTypeMap Not supported yet.");
    }

    public void setHoldability(int holdability) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: setHoldability Not supported yet.");
    }

    public int getHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: getHoldability Not supported yet.");
    }

    public Savepoint setSavepoint() throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: setSavepoint Not supported yet.");
    }

    public Savepoint setSavepoint(String name) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: setSavepoint Not supported yet.");
    }

    public void rollback(Savepoint savepoint) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: rollback Not supported yet.");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "releaseSavepoint Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: releaseSavepoint Not supported yet.");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: createStatement Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
        throw new UnsupportedOperationException(
                "HPCCConnection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareCall Not supported yet.");
        throw new UnsupportedOperationException(
                "HPCCConnection: prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareStatement(String sql, int autoGeneratedKeys)  Not supported yet.");
        throw new UnsupportedOperationException(
                "HPCCConnection: prepareStatement(String sql, int autoGeneratedKeys) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareStatement(String sql, int[] columnIndexes) Not supported yet.");
        throw new UnsupportedOperationException(
                "HPCCConnection: prepareStatement(String sql, int[] columnIndexes) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: prepareStatement(String sql, String[] columnNames) Not supported yet.");
        throw new UnsupportedOperationException(
                "HPCCConnection:  prepareStatement(String sql, String[] columnNames) Not supported yet.");
    }

    public Clob createClob() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createClob Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: createClob Not supported yet.");
    }

    public Blob createBlob() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createBlob Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: createBlob Not supported yet.");
    }

    public NClob createNClob() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createNClob Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: createNClob Not supported yet.");
    }

    public SQLXML createSQLXML() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: createSQLXML Not supported yet.");
        throw new UnsupportedOperationException("HPCCConnection: createSQLXML Not supported yet.");
    }

    public boolean isValid(int timeout) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCConnection: isValid");
        boolean success = false;
        if (!closed)
        {
            if (metadata != null)
                success = metadata.isTargetHPCCReachable(timeout);
        }
        return success;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCCONNECTION SETCLIENTINFO(" + name + ", " + value + ")");
        clientInfo.put(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCCONNECTION SETCLIENTINFO (properties)");
        clientInfo = properties;
    }

    public String getClientInfo(String name) throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCCONNECTION GETCLIENTINFO");
        return clientInfo.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException
    {
        HPCCJDBCUtils.traceoutln(Level.FINEST,  "HPCCCONNECTION GETCLIENTINFO");
        return clientInfo;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        throw new UnsupportedOperationException(
                "HPCCConnection: createArrayOf(String typeName, Object[] elements) Not supported yet.");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "HPCCConnection: createStruct(String typeName, Object[] attributes)Not supported yet.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: unwrap(Class<T> iface) Not supported yet.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException("HPCCConnection: isWrapperFor(Class<?> iface) sNot supported yet.");
    }

	@Override
	public void abort(Executor arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSchema(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public InputStream getInputStream() {
		try {
			return httpConnection.getInputStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
