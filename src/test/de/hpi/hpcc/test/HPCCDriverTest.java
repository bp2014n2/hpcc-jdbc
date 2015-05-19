package de.hpi.hpcc.test;

import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCConnection;
import de.hpi.hpcc.main.HPCCDriver;

public class HPCCDriverTest {
	
	private static final String acceptedURL = "jdbc:hpcc://test.de";
	
	@Test @Before
	public void testDriverRegistration() throws ClassNotFoundException, SQLException {
		Class.forName("de.hpi.hpcc.main.HPCCDriver");
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
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://localhost:8010/"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://92.232.93.92"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://92.232.93.92:8010"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://92.232.93.92:8010/"));
		assertNotNull(DriverManager.getDriver("jdbc:hpcc://google.com"));	
	}
	
	@Test
	public void testGetConnection() throws SQLException {
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection(acceptedURL, null);
		assertNotNull(connection);
		connection.close();
	}
	
	@Test
	public void testDefaultProperties() throws SQLException{
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://localhost", null);
		assertEquals("//localhost", connection.getClientInfo("ServerAddress"));
		assertEquals("//localhost", connection.getClientInfo("WsECLWatchAddress"));
		assertEquals("//localhost", connection.getClientInfo("WsECLAddress"));
		assertEquals("//localhost", connection.getClientInfo("WsECLDirectAddress"));
		assertEquals("", connection.getClientInfo("username"));
		assertEquals("", connection.getClientInfo("password"));
		assertEquals("5000000", connection.getClientInfo("ConnectTimeoutMilli"));
		assertEquals("5000000", connection.getClientInfo("ReadTimeoutMilli"));
		assertEquals("true", connection.getClientInfo("LazyLoad"));
		assertEquals("100", connection.getClientInfo("EclResultLimit"));
		assertEquals(Level.INFO.getName(), connection.getClientInfo("TraceLevel"));
		assertEquals("false", connection.getClientInfo("TraceToFile"));
		assertEquals("thor", connection.getClientInfo("TargetCluster"));
		assertEquals("hthor", connection.getClientInfo("QuerySet"));
		assertEquals("100", connection.getClientInfo("PageSize"));
		assertEquals("0", connection.getClientInfo("PageOffset"));
		assertEquals("8010", connection.getClientInfo("WsECLDirectPort"));
		assertEquals("8002", connection.getClientInfo("WsECLPort"));
		assertEquals("8010", connection.getClientInfo("WsECLWatchPort"));
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
		
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://localhost", connectionProperties);
		assertEquals("//localhost", connection.getClientInfo("WsECLWatchAddress"));
		assertEquals("//localhost", connection.getClientInfo("WsECLAddress"));
		assertEquals("//localhost", connection.getClientInfo("WsECLDirectAddress"));
		assertEquals("100", connection.getClientInfo("PageSize"));
		assertEquals("0", connection.getClientInfo("PageOffset"));
		assertEquals("5000000", connection.getClientInfo("ConnectTimeoutMilli"));
		assertEquals("5000000", connection.getClientInfo("ReadTimeoutMilli"));
		assertEquals("100", connection.getClientInfo("EclResultLimit"));
		
		connection.close();
	}
	
	@Test 
	public void testGetConnectionWithCredentials() throws SQLException {
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://192.168.56.101", "test", "test");
		assertEquals("//192.168.56.101", connection.getClientInfo("ServerAddress"));
		assertEquals("test", connection.getClientInfo("username"));
		assertEquals("test", connection.getClientInfo("password"));
		connection.close();
	}
	
	@Test 
	public void testECLResultLimitSpecialValue() throws SQLException {
		((HPCCDriver) DriverManager.getDriver(acceptedURL)).resetProperties();
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("jdbc:hpcc://192.168.56.101", "test", "test");
		Properties connectionProperties = new Properties();
		connectionProperties.put("EclResultLimit", "ALL");
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc://mytest.de").connect("jdbc:hpcc://mytest.de", connectionProperties);
		
		assertEquals("ALL", connection.getClientInfo("EclResultLimit"));
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
		
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc://mytest.de").connect("jdbc:hpcc://mytest.de", connectionProperties);
		
		assertEquals("//mytest.de", connection.getClientInfo("ServerAddress"));
		assertEquals("//mytest.de", connection.getClientInfo("WsECLWatchAddress"));
		assertEquals("//mytest.de", connection.getClientInfo("WsECLAddress"));
		assertEquals("//mytest.de", connection.getClientInfo("WsECLDirectAddress"));
		assertEquals("test", connection.getClientInfo("username"));
		assertEquals("test", connection.getClientInfo("password"));
		assertEquals("50", connection.getClientInfo("ConnectTimeoutMilli"));
		assertEquals("1000", connection.getClientInfo("ReadTimeoutMilli"));
		assertEquals("false", connection.getClientInfo("LazyLoad"));
		assertEquals("1000", connection.getClientInfo("EclResultLimit"));
		assertEquals(Level.WARNING.getName(), connection.getClientInfo("TraceLevel"));
		assertEquals("true", connection.getClientInfo("TraceToFile"));
		assertEquals("hthor2", connection.getClientInfo("TargetCluster"));
		assertEquals("hthor2", connection.getClientInfo("QuerySet"));
		assertEquals("1000", connection.getClientInfo("PageSize"));
		assertEquals("1000", connection.getClientInfo("PageOffset"));
		assertEquals("8011", connection.getClientInfo("WsECLDirectPort"));
		assertEquals("8012", connection.getClientInfo("WsECLPort"));
		assertEquals("8013", connection.getClientInfo("WsECLWatchPort"));
		
		connection.close();
	}
	
}
