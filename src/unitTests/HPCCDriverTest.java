package unitTests;

import connectionManagement.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;

import org.junit.*;
import org.junit.Test;

import static org.junit.Assert.*;


public class HPCCDriverTest {
	
	private static final String acceptedURL = "jdbc:hpcc://test.de";
	
	@Test @Before
	public void testDriverRegistration() throws ClassNotFoundException, SQLException {
		Class.forName("connectionManagement.HPCCDriver");
	}
	
	@Test
	public void testDriverUnacceptedURLs() {
		boolean allStatementsThrowExceptions = true;
		try {
			DriverManager.getDriver("jdbpcc");
			allStatementsThrowExceptions = false;
		} catch (SQLException e) {}
		try {
			DriverManager.getDriver("jdbc:hpcc");
			allStatementsThrowExceptions = false;
		} catch (SQLException e) {}
		try {
			DriverManager.getDriver("http://localhost");
			allStatementsThrowExceptions = false;
		} catch (SQLException e) {}
		try {
			DriverManager.getDriver("localhost");
			allStatementsThrowExceptions = false;
		} catch (SQLException e) {}
		try {
			DriverManager.getDriver("jdbc:hpcc;http://localhost");
			allStatementsThrowExceptions = false;
		} catch (SQLException e) {}
		
		assertTrue(allStatementsThrowExceptions);
	}
	
	@Test
	public void testURLAccepting() throws SQLException{
		assertNotNull(DriverManager.getDriver(acceptedURL));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://localhost:8010"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://92.232.93.92"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://google.com"));	
	}
	
	@Test
	public void testGetConnection() throws SQLException {
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("", null);
		assertNotNull(connection);
		connection.close();
	}
	
	@Test
	public void testDefaultProperties() throws SQLException{
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("", null);
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("username").equals("hpccdemo"));
		assertTrue(connection.getProperties().getProperty("password").equals("hpccdemo"));
		assertTrue(connection.getProperties().getProperty("ConnectTimeoutMilli").equals("5000"));
		assertTrue(connection.getProperties().getProperty("ReadTimeoutMilli").equals("15000"));
		assertTrue(connection.getProperties().getProperty("LazyLoad").equals("true"));
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("100"));
		assertTrue(connection.getProperties().getProperty("TraceLevel").equals(Level.INFO.getName()));
		assertTrue(connection.getProperties().getProperty("TraceToFile").equals("false"));
		assertTrue(connection.getProperties().getProperty("TargetCluster").equals("hthor"));
		assertTrue(connection.getProperties().getProperty("QuerySet").equals("hthor"));
		assertTrue(connection.getProperties().getProperty("PageSize").equals("100"));
		assertTrue(connection.getProperties().getProperty("PageOffset").equals("0"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectPort").equals("8010"));
		assertTrue(connection.getProperties().getProperty("WsECLPort").equals("8002"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchPort").equals("8010"));
		assertTrue(connection.getProperties().getProperty("Basic Auth").equals(HPCCConnection.createBasicAuth("hpccdemo", "hpccdemo")));
		connection.close();
	}
	
	@Test
	public void testSettingPropertiesFailures() throws SQLException{
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		Properties connectionProperties = new Properties();
		connectionProperties.put("WsECLWatchAddress", "asdas");
		connectionProperties.put("WsECLAddress", "asddas");
		connectionProperties.put("WsECLDirectAddress", "asdasd");
		connectionProperties.put("PageSize", "aasda");
		connectionProperties.put("PageOffset", "asdsa");
		connectionProperties.put("ConnectTimeoutMilli", "asdasa");
		connectionProperties.put("ReadTimeoutMilli", "asdasda");
		connectionProperties.put("EclResultLimit", "asdadsa");
		
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("", connectionProperties);
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("jdbc:hpcc://localhost"));
		assertTrue(connection.getProperties().getProperty("PageSize").equals("100"));
		assertTrue(connection.getProperties().getProperty("PageOffset").equals("0"));
		assertTrue(connection.getProperties().getProperty("ConnectTimeoutMilli").equals("5000"));
		assertTrue(connection.getProperties().getProperty("ReadTimeoutMilli").equals("15000"));
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("100"));
		
		connection.close();
	}
	
	@Test 
	public void testGetConnectionWithCredentials() throws SQLException {
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://192.168.56.101", "test", "test");
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("jdbc:hpcc://192.168.56.101"));
		assertTrue(connection.getProperties().getProperty("username").equals("test"));
		assertTrue(connection.getProperties().getProperty("password").equals("test"));
		connection.close();
	}
	
	@Test 
	public void testECLResultLimitSpecialValue() throws SQLException {
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://192.168.56.101", "test", "test");
		Properties connectionProperties = new Properties();
		connectionProperties.put("EclResultLimit", "ALL");
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc://mytest.de").connect("jdbc:hpcc://mytest.de", connectionProperties);
		
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("ALL"));
	}
	
	@Test 
	public void testAllProperties() throws SQLException {
		
		((HPCCDriver) DriverManager.getDriver("jdbc:hpcc://test.de")).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://192.168.56.101", "test", "test");
		
		Properties connectionProperties = new Properties();
		connectionProperties.put("ServerAddress", "jdbc:hpcc://mytest.de");
		connectionProperties.put("WsECLWatchAddress", "jdbc:hpcc://mytest.de");
		connectionProperties.put("WsECLAddress", "jdbc:hpcc://mytest.de");
		connectionProperties.put("WsECLDirectAddress", "jdbc:hpcc://mytest.de");
		connectionProperties.put("username", "test");
		connectionProperties.put("password", "test");
		connectionProperties.put("ConnectTimeoutMilli", "50");
		connectionProperties.put("ReadTimeoutMilli", "1000");
		connectionProperties.put("LazyLoad", "false");
		connectionProperties.put("EclResultLimit", "1000");
		connectionProperties.put("TraceLevel", Level.WARNING.getName());
		connectionProperties.put("TraceToFile", "true");
		connectionProperties.put("TargetCluster", "hthor2");
		connectionProperties.put("QuerySet", "hthor2");
		connectionProperties.put("PageSize", "1000");
		connectionProperties.put("PageOffset", "1000");
		connectionProperties.put("WsECLDirectPort", "8011");
		connectionProperties.put("WsECLPort", "8012");
		connectionProperties.put("WsECLWatchPort", "8013");
		connectionProperties.put("Basic Auth", HPCCConnection.createBasicAuth("test", "test"));
		
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc://mytest.de").connect("jdbc:hpcc://mytest.de", connectionProperties);
		
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("jdbc:hpcc://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("jdbc:hpcc://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("jdbc:hpcc://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("jdbc:hpcc://mytest.de"));
		assertTrue(connection.getProperties().getProperty("username").equals("test"));
		assertTrue(connection.getProperties().getProperty("password").equals("test"));
		assertTrue(connection.getProperties().getProperty("ConnectTimeoutMilli").equals("50"));
		assertTrue(connection.getProperties().getProperty("ReadTimeoutMilli").equals("1000"));
		assertTrue(connection.getProperties().getProperty("LazyLoad").equals("false"));
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("1000"));
		assertTrue(connection.getProperties().getProperty("TraceLevel").equals(Level.WARNING.getName()));
		assertTrue(connection.getProperties().getProperty("TraceToFile").equals("true"));
		assertTrue(connection.getProperties().getProperty("TargetCluster").equals("hthor2"));
		assertTrue(connection.getProperties().getProperty("QuerySet").equals("hthor2"));
		assertTrue(connection.getProperties().getProperty("PageSize").equals("1000"));
		assertTrue(connection.getProperties().getProperty("PageOffset").equals("1000"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectPort").equals("8011"));
		assertTrue(connection.getProperties().getProperty("WsECLPort").equals("8012"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchPort").equals("8013"));
		assertTrue(connection.getProperties().getProperty("Basic Auth").equals(HPCCConnection.createBasicAuth("test", "test")));
		
		connection.close();
	}
}
