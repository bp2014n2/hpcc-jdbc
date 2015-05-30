package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

abstract public class ECLBuilderTest {
	
	protected static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	public static void assertStatementCanBeParsedAs(String expected, String sql) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		ECLBuilder builder = typeParser.getBuilder(sql);
		assertEquals(expected, builder.generateECL());
    }
	
	@BeforeClass
	public static void initialize() {
		eclLayouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		eclLayouts.setLayout("anothertable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		
		eclLayouts.setLayout("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytemptable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		
		List<String> keyedColumns = new ArrayList<String>();
		List<String> nonKeyedColumns = new ArrayList<String>();
		keyedColumns.add("myColumn");
		keyedColumns.add("myColumnA");
		keyedColumns.add("myColumnB");
		eclLayouts.setIndex("myTable", "myTable_idx", keyedColumns, nonKeyedColumns);
		eclLayouts.setIndex("myTableA", "myTableA_idx", keyedColumns, nonKeyedColumns);
		eclLayouts.setIndex("myTableB", "myTableB_idx", keyedColumns, nonKeyedColumns);
		eclLayouts.setIndex("myTempTable", "myTempTable_idx", keyedColumns, nonKeyedColumns);
	}
}
