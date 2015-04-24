package de.hpi.hpcc.test;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLColumnFinder;

public class ECLColumnFinderTest {
	
	private static ECLLayoutsStub layouts = new ECLLayoutsStub(null);
	
	public static void assertAllColumnsAreFound(List<String> expectedTables, List<String> tableNameAndAlias, String sql) throws HPCCException {
		ECLColumnFinder finder = new ECLColumnFinder(layouts, tableNameAndAlias);
		Statement statement = SQLParser.parse(sql);
		List<String> foundTables = finder.find(statement);
		assertEquals(foundTables, expectedTables);
    }
	
	@Test
	public void shouldFindColumns() throws HPCCException {
		List<String> tableNameAndAlias = new ArrayList<String>();
		tableNameAndAlias.add("mytable");
		List<String> expectedColumns = new ArrayList<String>();
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableNameAndAlias, "SELECT myColumn from myTable");
	}
	
}
