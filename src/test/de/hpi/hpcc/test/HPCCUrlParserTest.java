package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCUrlParser;

public class HPCCUrlParserTest {

	@Test
	public void testGetPort() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertEquals(null, parser.getPort("jdbc:hpcc://92.232.93.92"));
		assertEquals(null, parser.getPort("jdbc:hpcc://92.232.93.92:"));
		assertEquals(null, parser.getPort("jdbc:hpcc://92.232.93.92:/"));
		assertEquals(null, parser.getPort("jdbc:hpcc://92.232.93.92:asdasa/"));
		assertEquals(null, parser.getPort("jdbc:hpcc://92.232.93.92/"));
		assertEquals("8010", parser.getPort("jdbc:hpcc://92.232.93.92:8010"));
		assertEquals("8010", parser.getPort("jdbc:hpcc://92.232.93.92:8010/"));
	}
	
	@Test
	public void testGetUrlWithOutProtocol() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertEquals("//92.232.93.92", parser.getFileLocation("jdbc:hpcc://92.232.93.92"));
		assertEquals("//92.232.93.92", parser.getFileLocation("jdbc:hpcc://92.232.93.92:8010"));
		assertEquals("//92.232.93.92", parser.getFileLocation("jdbc:hpcc://92.232.93.92:8010/"));
		assertNull(parser.getFileLocation("jdbc:hpcc://92.232.93.92:"));
		assertNull(parser.getFileLocation("jdbc:hpcc://92.232.93.92:/"));
		assertEquals("//92.232.93.92", parser.getFileLocation("jdbc:hpcc://92.232.93.92/"));
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
		assertFalse(parser.isValidUrl("http://92.10.11.20/asdas/"));
		assertTrue(parser.isValidUrl("http://92.10.11.20:8000/ysxasda"));
		assertFalse(parser.isValidUrl("http:/www.test.de"));
		assertFalse(parser.isValidUrl(null));
	}
}
