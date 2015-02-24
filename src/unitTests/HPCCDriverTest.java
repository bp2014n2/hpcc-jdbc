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
	
	@Test @Before
	public void testDriverRegistration() throws ClassNotFoundException, SQLException {
		Class.forName("connectionManagement.HPCCDriver");
		assertNotNull(DriverManager.getDriver("jdbc:hpcc"));
	}
	
	@Test(expected=SQLException.class)
	public void testDriverRegistrationFailure() throws SQLException {
		DriverManager.getDriver("jdbpcc");
	}
	
	@Test
	public void testURLAcception() throws SQLException{
		assertTrue(DriverManager.getDriver("jdbc:hpcc").acceptsURL("jdbc:hpcc"));
		assertFalse(DriverManager.getDriver("jdbc:hpcc").acceptsURL("http://localhost"));
		assertFalse(DriverManager.getDriver("jdbc:hpcc").acceptsURL("localhost"));
		assertFalse(DriverManager.getDriver("jdbc:hpcc").acceptsURL("jdbc:hpcc;http://localhost"));
	}
	
	@Test
	public void testConnectionAndProperties() throws SQLException {
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("", null);
		assertNotNull(connection);
		
		testDefaultPropertiesAndFailures();
		testAllProperties();
	}
	
	private void testDefaultPropertiesAndFailures() throws SQLException{
		
		trySettingPropertiesFailures();
		
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("", null);
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("http://localhost"));
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
	
	private void trySettingPropertiesFailures() throws SQLException{
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
		
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("http://localhost"));
		assertTrue(connection.getProperties().getProperty("PageSize").equals("100"));
		assertTrue(connection.getProperties().getProperty("PageOffset").equals("0"));
		assertTrue(connection.getProperties().getProperty("ConnectTimeoutMilli").equals("5000"));
		assertTrue(connection.getProperties().getProperty("ReadTimeoutMilli").equals("15000"));
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("100"));
	}
	
	
	private void testAllProperties() throws SQLException {
		HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("http://192.168.56.101", "test", "test");
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("http://192.168.56.101"));
		assertTrue(connection.getProperties().getProperty("username").equals("test"));
		assertTrue(connection.getProperties().getProperty("password").equals("test"));
		
		connection.close();
		
		Properties connectionProperties = new Properties();
		connectionProperties.put("ServerAddress", "http://mytest.de");
		connectionProperties.put("WsECLWatchAddress", "http://mytest.de");
		connectionProperties.put("WsECLAddress", "http://mytest.de");
		connectionProperties.put("WsECLDirectAddress", "http://mytest.de");
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
		
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc").connect("http://mytest.de", connectionProperties);
		
		assertTrue(connection.getProperties().getProperty("ServerAddress").equals("http://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLWatchAddress").equals("http://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLAddress").equals("http://mytest.de"));
		assertTrue(connection.getProperties().getProperty("WsECLDirectAddress").equals("http://mytest.de"));
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
		
		connectionProperties.put("EclResultLimit", "ALL");
		
		connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc").connect("http://mytest.de", connectionProperties);
		
		assertTrue(connection.getProperties().getProperty("EclResultLimit").equals("ALL"));
	}
}
