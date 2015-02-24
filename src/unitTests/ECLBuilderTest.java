package unitTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.sql.SQLException;
import org.junit.Test;

import connectionManagement.ECLBuilder;

public class ECLBuilderTest {

	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		assertEquals(new ECLBuilder().generateECL("select * from mySchema.myTable"), "myTable");
		assertEquals(new ECLBuilder().generateECL("select myColumn from mySchema.myTable"), "myTable, {myColumn}");
		assertEquals(new ECLBuilder().generateECL("select distinct myColumn from mySchema.myTable"), "");
		assertEquals(new ECLBuilder().generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"), "myTable, {myColumnA, myNewColumnB := myColumnB}");
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertEquals(new ECLBuilder().generateECL("select * from mySchema.myTable where myColumn = 'foo'"), "myTable(myColumn = 'foo')");
		assertEquals(new ECLBuilder().generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"), "myTable(myColumn = 'foo'), {myColumn}");
		assertEquals(new ECLBuilder().generateECL("select myColumn from mySchema.myTable where myColumn is NULL"), "myTable(myColumn = ''), {myColumn}");
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertEquals(new ECLBuilder().generateECL("select * from mySchema.myTable where myColumn like 'foo%'"), "myTable(myColumn[1..3] = 'foo')");
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertEquals(new ECLBuilder().generateECL("select * from mySchema.myTable order by myColumn"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithGroupBy() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select * from mySchema.myTable group by myColumn"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithCountStar() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select count(*) from mySchema.myTable"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select count(myColumn) from mySchema.myTable"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithLimit() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select myColumn from mySchema.myTable limit 1"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithJoin() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select myColumnA from (select myColumnA, myColumnB from myTable)"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)"), "SORT(myTable, myColumn)");
	}
	
	@Test
	public void shouldTranslateSelectWithFunction() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("select nextval('mySequence')"), "");
	}
	
	@Test
	public void shouldTranslateInsertInto() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("insert into myTable values (valueA)"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable values (valueA, valueB)"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable (columnA) values (valueA)"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable (columnA, columnB) values (valueA, valueB)"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable (columnA, columnB) values (valueA, valueB) returning *"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable (myColumnA) with x as (select myColumnC from anotherTable) select x.myColumnB from x"), "");
		assertEquals(new ECLBuilder().generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"), "");
	}
	
	@Test
	public void shouldTranslateUpdate() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("update myTable set myColumn = 'myValue'"), "");
		assertEquals(new ECLBuilder().generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"), "");
		
	}
	
	@Test
	public void shouldTranslateDropTable() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("drop myTable"), "");		
	}
	
	@Test
	public void shouldCreateTable() {
		fail("not yet implemented");
		assertEquals(new ECLBuilder().generateECL("create table newTable (myColumnA myTypeA, myColumnB myTypeB"), "");
		assertEquals(new ECLBuilder().generateECL("create temp table newTable (myColumnA myTypeA, myColumnB myTypeB"), "");
		
	}
	
}
