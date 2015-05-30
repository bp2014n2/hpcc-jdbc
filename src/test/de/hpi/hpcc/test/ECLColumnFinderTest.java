package de.hpi.hpcc.test;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.statement.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLColumnFinder;

public class ECLColumnFinderTest {

	private static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	@BeforeClass
	public static void initialize() {
		eclLayouts.setLayout("myTable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		eclLayouts.setLayout("myTableA", "RECORD STRING50 columnFromA; END;");
		eclLayouts.setLayout("myTableB", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	public static void assertAllColumnsAreFound(Set<String> expectedColumns, String tableName, String sql) throws HPCCException {
		ECLColumnFinder finder = new ECLColumnFinder(eclLayouts, tableName);
		Statement statement = SQLParser.parse(sql);
		Set<String> foundColumns = finder.find(statement);
		assertEquals(expectedColumns, foundColumns);
    }
	
	@Test
	public void shouldFindColumns() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTable");
		expectedColumns.add("myColumnA");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn, myColumnA from myTable");
		expectedColumns.add("myColumnB");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT * from myTable");
	}
	
	@Test
	public void shouldFindJoinColumns() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTable, myTableA");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTableA, myTable");
		expectedColumns.add("myColumnA");
		assertAllColumnsAreFound(expectedColumns, tableName, "WITH t AS (SELECT myColumn, myColumnA FROM myTable) SELECT myColumn from myTableA, t");
	}
	
	@Test
	public void shouldFindColumnsInWhere() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumn");
		expectedColumns.add("myColumnA");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTable WHERE myColumnA = 'myValue'");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTable WHERE myTable.myColumnA = 'myValue'");
		assertAllColumnsAreFound(expectedColumns, tableName, "UPDATE myTable SET myColumnA = 'updated' WHERE myColumn = 'myValue'");
		expectedColumns.add("myColumnB");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from myTable WHERE myColumnA = myColumnB");
	}
	
	@Test
	public void shouldFindColumnsInSubSelect() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from (SELECT myColumn FROM myTable)");
		expectedColumns.add("myColumnA");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from (SELECT myColumn, myColumnA FROM myTable)");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from (SELECT myColumn FROM myTable WHERE myColumnA = 'myValue')");
		assertAllColumnsAreFound(expectedColumns, tableName, "INSERT INTO myTable (SELECT myColumn from myTable WHERE myColumnA = 'myValue')");
		expectedColumns.add("myColumnB");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT myColumn from (SELECT * FROM myTable)");
		assertAllColumnsAreFound(expectedColumns, tableName, "UPDATE myTable SET myColumnA = 'updated' WHERE myColumn IN (SELECT myColumn from myTable WHERE myColumnB = 'myValue')");
	}
	
	@Test
	public void shouldFindColumnsInWith() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumnA");
		expectedColumns.add("myColumnB");
		assertAllColumnsAreFound(expectedColumns, tableName, "INSERT INTO myTable WITH t AS (SELECT myColumnA from myTable WHERE myColumnB = 'myValue') SELECT myColumnA FROM t");
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableName, "INSERT INTO myTable WITH t AS (SELECT * from myTable WHERE myColumnB = 'myValue') SELECT myColumnA FROM t");
	}
	
	@Test
	public void shouldFindColumnsWithTableAlias() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumnA");
		assertAllColumnsAreFound(expectedColumns, tableName, "SELECT t.myColumnA FROM myTable t");
	}	
	
	@Test
	public void shouldFindColumnsInFunction() throws HPCCException {
		String tableName = "myTable";
		Set<String> expectedColumns = new HashSet<String>();
		expectedColumns.add("myColumnA");
		expectedColumns.add("myColumnB");
		assertAllColumnsAreFound(expectedColumns, tableName, "INSERT INTO myTable WITH t AS (SELECT myColumnA, COUNT(myColumnB) as count_b FROM myTable GROUP BY myColumnA ORDER BY count_b) SELECT myColumnA FROM t");
		expectedColumns.add("myColumn");
		assertAllColumnsAreFound(expectedColumns, tableName, "UPDATE myTable SET myColumnA = 'A' WHERE myColumnB IN (SELECT myColumnB, SUM(myColumn) as myCount FROM myTable GROUP BY myColumnB ORDER BY myCount LIMIT 10)");
		assertAllColumnsAreFound(expectedColumns, tableName, "UPDATE myTable SET myColumnA = 'A' WHERE myColumnB IN (SELECT myColumnB FROM myTable WHERE SUBSTRING(myColumn FROM 1 FOR 3) = 'abc')");
	}

}
