package unitTests;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import connectionManagement.ECLBuilder;

public class ECLBuilderTest {
	
	private static ECLBuilder eclBuilder;
	
	@BeforeClass
	public static void initialize() {
		eclBuilder = new ECLBuilder();
	}
	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		assertEquals(eclBuilder.generateECL("select * from mySchema.myTable"), "myTable");
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable"), "Table(myTable, {myColumn})");
		assertEquals(eclBuilder.generateECL("select distinct * from mySchema.myTable"), "DEDUP(myTable, All)");
		assertEquals(eclBuilder.generateECL("select distinct myColumn from mySchema.myTable"), "DEDUP(Table(myTable, {myColumn}), All)");
		assertEquals(eclBuilder.generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"), "Table(myTable, {myColumnA, myNewColumnB := myColumnB})");
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertEquals(eclBuilder.generateECL("select * from mySchema.myTable where myColumn = 'foo'"), "myTable(myColumn = 'foo')");
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"), "Table(myTable(myColumn = 'foo'), {myColumn})");
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn is NULL"), "Table(myTable(myColumn = ''), {myColumn})");
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertEquals(eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo%'"), "myTable(myColumn[1..3] = 'foo')");
		assertEquals(eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo'"), "myTable(myColumn[1..3] = 'foo')");
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertEquals(eclBuilder.generateECL("select * from mySchema.myTable order by myColumn"), "SORT(myTable, myColumn)");
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable order by myColumn"), "SORT(Table(myTable, {myColumn}), myColumn)");
		assertEquals(eclBuilder.generateECL("select distinct myColumn from mySchema.myTable order by myColumn"), "SORT(DEDUP(Table(myTable, {myColumn}), All), myColumn)");
//		following query is currently not supported because of an unknown equivalent in ECL
//		assertEquals(eclBuilder.generateECL("select myColumn from myTable group by myColumn order by count(*)"), "");
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn"), "Table(myTable, {myColumn}, myColumn)");
		assertEquals(eclBuilder.generateECL("select count(myColumn) from mySchema.myTable group by myColumn"), "Table(myTable, {count_myColumn := count(group)}, myColumn)");
		assertEquals(eclBuilder.generateECL("select myColumn, count(myColumn) from mySchema.myTable group by myColumn order by count(*)"), "SORT(Table(myTable, {myColumn, count_myColumn := count(group)}, myColumn), count(group))");
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		assertEquals(eclBuilder.generateECL("select count(*) from mySchema.myTable"), "Table(myTable, {count_ := count(group)})");
		assertEquals(eclBuilder.generateECL("select count(myColumn) from mySchema.myTable"), "Table(myTable, {count_myColumn := count(group)})");
	}
		
	@Test @Ignore
	public void shouldTranslateSelectWithLimit() {
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTable limit 1"), "");
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
		assertEquals(eclBuilder.generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"), "");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		assertEquals(eclBuilder.generateECL("select myColumnA from (select myColumnA, myColumnB from myTable)"), "Table((Table(myTable, {myColumnA, myColumnB})), {myColumnA})");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
//		not implemented yet
//		assertEquals(eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')"), "Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})");
		assertEquals(eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)"), "Table(myTableA(myColumnB in set(Table(myTableB, {myColumnC}))), {myColumnA})");
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithFunction() {
		assertEquals(eclBuilder.generateECL("select nextval('mySequence')"), "");
	}
	
	@Test @Ignore
	public void shouldTranslateInsertInto() {
		assertEquals(eclBuilder.generateECL("insert into myTable values (valueA)"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable values (valueA, valueB)"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable (columnA) values (valueA)"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable (columnA, columnB) values (valueA, valueB)"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable (columnA, columnB) values (valueA, valueB) returning *"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable (myColumnA) with x as (select myColumnC from anotherTable) select x.myColumnB from x"), "");
		assertEquals(eclBuilder.generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"), "");
	}
	
	@Test @Ignore
	public void shouldTranslateUpdate() {
		assertEquals(eclBuilder.generateECL("update myTable set myColumn = 'myValue'"), "");
		assertEquals(eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"), "");
		
	}
	
	@Test @Ignore
	public void shouldTranslateDropTable() {
		assertEquals(eclBuilder.generateECL("drop myTable"), "");		
	}
	
	@Test @Ignore
	public void shouldCreateTable() {
		assertEquals(eclBuilder.generateECL("create table newTable (myColumnA myTypeA, myColumnB myTypeB"), "");
		assertEquals(eclBuilder.generateECL("create temp table newTable (myColumnA myTypeA, myColumnB myTypeB"), "");
		
	}
	
}
