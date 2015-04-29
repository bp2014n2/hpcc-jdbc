package de.hpi.hpcc.test;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.statement.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLTableFinder;

public class ECLTableFinderTest {
	
	public static void assertAllTablesAreFound(Set<String> expectedTables, String sql) throws HPCCException {
		ECLTableFinder finder = new ECLTableFinder();
		Statement statement = SQLParser.parse(sql);
		Set<String> foundTables = finder.find(statement);
		assertEquals(expectedTables, foundTables);
    }
	
	@Test
	public void shouldFindFromItems() throws HPCCException {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "SELECT * FROM myTable");
	}
	
	@Test
	public void shouldNotFindWithAliases() throws HPCCException {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "WITH t AS (SELECT * FROM myTable) SELECT * FROM t");
	}
	
	@Test
	public void shouldNotFindAliases() throws HPCCException {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "SELECT * FROM myTable t");
		assertAllTablesAreFound(expectedTables, "SELECT t.myColumn FROM myTable t");
	}
	
	@Test
	public void shouldFindWhereIns() throws HPCCException {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		expectedTables.add("anothertable");
		assertAllTablesAreFound(expectedTables, "SELECT * FROM myTable WHERE myColumn IN (SELECT anotherColumn FROM anotherTable)");
	}
	
}
