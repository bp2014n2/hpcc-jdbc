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
	
	private static ECLTableFinder finder = new ECLTableFinder();
	
	public static void assertAllTablesAreFound(Set<String> expectedTables, String sql) {
		try {
			Statement statement = SQLParser.parse(sql);
			Set<String> foundTables = finder.find(statement);
			assertEquals(foundTables, expectedTables);
		} catch (HPCCException e) {

		}
    }
	
	@Test
	public void shouldFindFromItems() {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "SELECT * from myTable");
	}
	
	@Test
	public void shouldNotFindWithAliases() {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "WITH t AS(SELECT * FROM myTable) SELECT * from t");
	}
	
	@Test
	public void shouldNotFindAliases() {
		Set<String> expectedTables = new HashSet<String>();
		expectedTables.add("mytable");
		assertAllTablesAreFound(expectedTables, "SELECT * from myTable t");
		assertAllTablesAreFound(expectedTables, "SELECT t.myColumn from myTable t");
	}
	
}
