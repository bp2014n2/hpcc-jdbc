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
	public void testGetUri() {
		HPCCUrlParser parser = new HPCCUrlParser();
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92"), "//92.232.93.92");
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92:8010"), "//92.232.93.92");
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92:8010/"), "//92.232.93.92");
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92:"), "//92.232.93.92");
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92:/"), "//92.232.93.92");
		assertEquals(parser.getUri("jdbc:hpcc://92.232.93.92/"), "//92.232.93.92");
	}
}
