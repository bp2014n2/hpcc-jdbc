package unitTests;

import static org.junit.Assert.*;

import org.junit.Test;

import connectionManagement.HPCCUrlParser;

public class HPCCUrlParserTest {

	@Test
	public void testGetPort() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92"), null);
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92:"), null);
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92:/"), null);
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92:asdasa/"), null);
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92/"), null);
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92:8010"), "8010");
		assertEquals(parser.getPort("jdbc:hpcc://92.232.93.92:8010/"), "8010");
	}
	
	@Test
	public void testGetUrlWithOutProtocol() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92"), "//92.232.93.92");
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92:8010"), "//92.232.93.92");
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92:8010/"), "//92.232.93.92");
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92:"), "//92.232.93.92");
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92:/"), "//92.232.93.92");
		assertEquals(parser.getFileLocation("jdbc:hpcc://92.232.93.92/"), "//92.232.93.92");
	}
	
	@Test
	public void testIsValidUrl() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertTrue(parser.isValidUrl("http://www.test.de"));
		assertTrue(parser.isValidUrl("https://www.test.de"));
		assertTrue(parser.isValidUrl("jdbc:hpcc://www.test.de"));
		assertTrue(parser.isValidUrl("http://92.10.11.20"));
		assertTrue(parser.isValidUrl("http://92.10.11.20:8000"));
		assertTrue(parser.isValidUrl("http://92.10.11.20/"));
		assertTrue(parser.isValidUrl("http://92.10.11.20:8000/"));
		assertTrue(parser.isValidUrl("http://92.10.11.20/asdas/"));
		assertTrue(parser.isValidUrl("http://92.10.11.20:8000/ysxasda"));
		assertFalse(parser.isValidUrl("http:/www.test.de"));
		assertFalse(parser.isValidUrl(null));
	}
}
