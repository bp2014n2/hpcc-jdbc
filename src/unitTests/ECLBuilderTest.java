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
		assertEquals("myTable", eclBuilder.generateECL("select * from mySchema.myTable"));
		assertEquals("Table(myTable, {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable"));
		assertEquals("DEDUP(myTable, All)", eclBuilder.generateECL("select distinct * from mySchema.myTable"));
		assertEquals("DEDUP(Table(myTable, {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable"));
		assertEquals("Table(myTable, {myColumnA, myNewColumnB := myColumnB})", eclBuilder.generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"));
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertEquals("myTable(myColumn = 'foo')", eclBuilder.generateECL("select * from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("Table(myTable(myColumn = 'foo'), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("Table(myTable(myColumn = ''), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn is NULL"));
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertEquals("myTable(myColumn[1..3] = 'foo')", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo%'"));
		assertEquals("myTable(myColumn[1..3] = 'foo')", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo'"));
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertEquals("SORT(myTable, myColumn)", eclBuilder.generateECL("select * from mySchema.myTable order by myColumn"));
		assertEquals("SORT(Table(myTable, {myColumn}), myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable order by myColumn"));
		assertEquals("SORT(DEDUP(Table(myTable, {myColumn}), All), myColumn)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable order by myColumn"));
//		following query is currently not supported because of an unknown equivalent in ECL
//		assertEquals("", eclBuilder.generateECL("select myColumn from myTable group by myColumn order by count(*)"));
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		assertEquals("Table(myTable, {myColumn}, myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn"));
		assertEquals("Table(myTable, {count_myColumn := count(group)}, myColumn)", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable group by myColumn"));
		assertEquals("SORT(Table(myTable, {myColumn, count_myColumn := count(group)}, myColumn), count(group))", eclBuilder.generateECL("select myColumn, count(myColumn) from mySchema.myTable group by myColumn order by count(*)"));
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		assertEquals("Table(myTable, {count_ := count(group)})", eclBuilder.generateECL("select count(*) from mySchema.myTable"));
		assertEquals("Table(myTable, {count_myColumn := count(group)})", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable"));
	}
		
	@Test @Ignore
	public void shouldTranslateSelectWithLimit() {
//		not implemented yet
		assertEquals("", eclBuilder.generateECL("select myColumn from mySchema.myTable limit 1"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
//		not implemented yet
		assertEquals("", eclBuilder.generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		assertEquals("Table((Table(myTable, {myColumnA, myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from (select myColumnA, myColumnB from myTable)"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
//		not implemented yet
//		assertEquals("Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')"));
		assertEquals("Table(myTableA(myColumnB in set(Table(myTableB, {myColumnC}))), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithFunction() {
//		not implemented yet
		assertEquals("", eclBuilder.generateECL("select nextval('mySequence')"));
		assertEquals("", eclBuilder.generateECL("select nextval('mySequence')"));
	}
	
	@Test @Ignore
	public void shouldTranslateInsertInto() {
		assertEquals("", eclBuilder.generateECL("insert into myTable values (valueA)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable values (valueA, valueB)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (columnA) values (valueA)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (columnA, columnB) values (valueA, valueB)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (columnA, columnB) values (valueA, valueB) returning *"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA) with x as (select myColumnC from anotherTable) select x.myColumnB from x"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"));
	}
	
	@Test @Ignore
	public void shouldTranslateUpdate() {
		assertEquals("", eclBuilder.generateECL("update myTable set myColumn = 'myValue'"));
		assertEquals("", eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"));
		
	}
	
	@Test @Ignore
	public void shouldTranslateDropTable() {
		assertEquals("", eclBuilder.generateECL("drop myTable"));		
	}
	
	@Test @Ignore
	public void shouldCreateTable() {
		assertEquals("", eclBuilder.generateECL("create table newTable (myColumnA myTypeA, myColumnB myTypeB"));
		assertEquals("", eclBuilder.generateECL("create temp table newTable (myColumnA myTypeA, myColumnB myTypeB"));
		
	}
	
}
