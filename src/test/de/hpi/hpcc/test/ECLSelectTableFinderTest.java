package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.main.HPCCJDBCUtils;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectTableFinder;

public class ECLSelectTableFinderTest {

	private static void assertAllExpressionsAreFound(List<String> expected, String sql) throws HPCCException {
		ECLSelectTableFinder finder = new ECLSelectTableFinder();
		Statement statement = SQLParser.parse(sql);
		List<Table> found = finder.findTables(statement);
		assertEquals(expected.size(), found.size());
		for(Table table : found) {
			assertTrue(HPCCJDBCUtils.containsStringCaseInsensitive(expected, table.getName()));
		}
	}
	
	@Test
	public void shouldFindTableInSimpleSelect() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myTable");
		assertAllExpressionsAreFound(expected , "SELECT myColumn, myColumnA FROM myTable");
	}
	
	@Test
	public void shouldFindTableInSubSelect() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myTable");
		assertAllExpressionsAreFound(expected , "SELECT myColumn from (select myColumn FROM myTable)");
	}
	
	@Test
	public void shouldNotFindTableInUpdate() throws HPCCException {
		List<String> expected = new ArrayList<String>();
//		expected.add("myTable");
		assertAllExpressionsAreFound(expected , "update myTable set myColumn = 'myValue'");
	}
	
	@Test
	public void shouldFindTableInSelectOfUpdate() throws HPCCException {
		List<String> expected = new ArrayList<String>();
//		expected.add("myTable");
		expected.add("myTableA");
		assertAllExpressionsAreFound(expected , "update myTable set myColumnA = 'myValue' where exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
	}
}
