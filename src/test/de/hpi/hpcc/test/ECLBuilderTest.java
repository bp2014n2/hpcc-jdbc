package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

abstract public class ECLBuilderTest {
	
	private static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	public static void assertStatementCanBeParsedAs(String expected, String sql) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		ECLBuilder builder;
		builder = typeParser.getBuilder(sql);
		assertEquals(expected, builder.generateECL());
    }
	
	@BeforeClass
	public static void initialize() {
//		eclBuilder = new ECLBuilder();
		eclLayouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		eclLayouts.setLayout("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytemptable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
	}
}
