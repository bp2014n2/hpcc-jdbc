package de.hpi.hpcc.test;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;

public class ECLSelectItemFinderTest {

	private static ECLLayoutsStub layouts = new ECLLayoutsStub(null);
	
	@BeforeClass
	public static void initialize() {
		layouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		layouts.setLayout("mytablea", "RECORD STRING50 columnFromA; END;");
		layouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	private static void assertAllExpressionsAreFound(List<String> expected, String sql) throws HPCCException {
		ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
		Statement statement = SQLParser.parse(sql);
		List<SelectExpressionItem> found = finder.find(statement);
		assertEquals(expected.size(), found.size());
		for(SelectExpressionItem expression : found) {
			assertTrue(expected.contains(expression.getExpression().toString()));
		}
	}
	
	@Test
	public void shouldFindColumns() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		assertAllExpressionsAreFound(expected , "SELECT myColumn, myColumnA FROM myTable");
	}
	
	@Test
	public void shouldFindColumnsWitAlias() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		assertAllExpressionsAreFound(expected , "SELECT myColumn, myColumnA as foo FROM myTable");
	}
	
	@Test
	public void shouldFindAllColumns() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		expected.add("myColumnB");
		assertAllExpressionsAreFound(expected , "SELECT * FROM myTable");		
	}
	
	@Test
	public void shouldFindFunctions() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("COUNT(*)");
		assertAllExpressionsAreFound(expected , "SELECT myColumn, COUNT(*) FROM myTable");
	}
	
	@Test
	public void shouldFindJoinColumnsOnce() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		expected.add("myColumnB");
		assertAllExpressionsAreFound(expected , "SELECT * FROM myTable, myTableB");
	}
	
	@Test
	public void shouldFindSubSelects() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		assertAllExpressionsAreFound(expected , "SELECT myColumn FROM (SELECT * from myTableA)");
		assertAllExpressionsAreFound(expected , "SELECT myColumn FROM (SELECT * from myTableB)");
		expected.add("myColumnA");
		assertAllExpressionsAreFound(expected , "SELECT * FROM (SELECT myColumn, myColumnA from myTable)");
		expected.add("myColumnB");
		assertAllExpressionsAreFound(expected , "SELECT * FROM (SELECT * from myTable)");		
	}
	
	@Test
	public void shouldFindFunctionsWithSubSelects() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("COUNT(*)");
		assertAllExpressionsAreFound(expected , "SELECT * FROM (SELECT COUNT(*) from myTable)");
	}
	
	@Test
	public void shouldFindAllColumnsForUpdates() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		expected.add("myColumnB");
		assertAllExpressionsAreFound(expected , "UPDATE myTable SET myColumn = 'value'");
	}
	
	@Test
	public void shouldFindAllColumnsForInserts() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("myColumn");
		expected.add("myColumnA");
		expected.add("myColumnB");
		assertAllExpressionsAreFound(expected , "INSERT INTO myTable VALUES ('1', '2', '3')");
	}
	
	@Test
	public void shouldFindColumnsWithWith() throws HPCCException {
		List<String> expected = new ArrayList<String>();
		expected.add("t.myColumnA");
		assertAllExpressionsAreFound(expected , "with t as (select myColumnA from myTableB) select t.myColumnA from t");
	}
	
	@Ignore @Test 
	public void shouldFindAllColumnsWithWith() throws HPCCException {
		//TODO: implement test
		List<String> expected = new ArrayList<String>();
		expected.add("myColumnA");
		assertAllExpressionsAreFound(expected , "with t as (select myColumnA from myTableB) select * from t");
	}
	
}
