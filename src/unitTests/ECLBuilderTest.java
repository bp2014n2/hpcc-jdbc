package unitTests;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import connectionManagement.ECLBuilder;
import connectionManagement.ECLLayouts;

public class ECLBuilderTest {
	
	private static ECLBuilder eclBuilder;
	
	@BeforeClass
	public static void initialize() {
		eclBuilder = new ECLBuilder();
		ECLLayouts.setLayouts("myTable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		ECLLayouts.setLayouts("myTableA", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		ECLLayouts.setLayouts("myTableB", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		assertEquals("Table(myTable, {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable"));
		assertEquals("Table(myTable, {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable"));
		assertEquals("DEDUP(Table(myTable, {myColumn, myColumnA, myColumnB}), All)", eclBuilder.generateECL("select distinct * from mySchema.myTable"));
		assertEquals("DEDUP(Table(myTable, {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable"));
		assertEquals("Table(myTable, {myColumnA, myNewColumnB := myColumnB})", eclBuilder.generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"));
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertEquals("Table(myTable(myColumn = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("Table(myTable(myColumn = 'foo'), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("Table(myTable(myColumn = ''), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn is NULL"));
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertEquals("Table(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo%'"));
		assertEquals("Table(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo'"));
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertEquals("Table(SORT(Table(myTable, {myColumn, myColumnA, myColumnB}), myColumn), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable order by myColumn"));
		assertEquals("Table(SORT(Table(myTable, {myColumn}), myColumn), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable order by myColumn"));
		assertEquals("DEDUP(Table(SORT(Table(myTable, {myColumn}), myColumn), {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable order by myColumn"));
		assertEquals("Table(SORT(Table(myTable, {count_ := count(group), myColumn}, myColumn), count_), {myColumn})", eclBuilder.generateECL("select myColumn from myTable group by myColumn order by count(*)"));
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		assertEquals("Table(myTable, {myColumn}, myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn"));
		assertEquals("Table(myTable, {myColumn}, myColumnA, myColumnB)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumnA, myColumnB"));
		assertEquals("Table(myTable, {count_myColumn := count(group)}, myColumn)", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable group by myColumn"));
		assertEquals("Table(SORT(Table(myTable, {count_ := count(group), myColumn}, myColumn), count_), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn order by count(*)"));
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		assertEquals("Table(myTable, {count_ := count(group)})", eclBuilder.generateECL("select count(*) from mySchema.myTable"));
		assertEquals("Table(myTable, {count_myColumn := count(group)})", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable"));
	}
		
	@Test
	public void shouldTranslateSelectWithLimit() {
		assertEquals("CHOOSEN(Table(myTable, {myColumn}), 1)", eclBuilder.generateECL("select myColumn from mySchema.myTable limit 1"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
//		not implemented yet
		assertEquals("Join(myTableA, myTableB, left.myColumnA = right.myColumnB), {myColumn}", eclBuilder.generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"));
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
	}
	
	@Test @Ignore
	public void shouldTranslateInsertInto() {
		assertEquals("", eclBuilder.generateECL("insert into myTable values (valueA, valueB, valueC)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA) values (valueA)"));
		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB)"));
//		currently not supported
//		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *"));
//		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA) with x as (select myColumnC from anotherTable) select x.myColumnB from x"));
//		assertEquals("", eclBuilder.generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"));
	}
	
	@Test @Ignore
	public void shouldTranslateUpdate() {
		assertEquals("", eclBuilder.generateECL("update myTable set myColumn = 'myValue'"));
		assertEquals("", eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"));	
	}
	
	@Test @Ignore
	public void shouldTranslateDropTable() {
		assertEquals("Std.File.DeleteLogicalFile('~mySchema::myTable')", eclBuilder.generateECL("drop mySchema.myTable"));		
	}
	
	@Test @Ignore
	public void shouldCreateTable() {
		assertEquals("", eclBuilder.generateECL("create table newTable (myColumnA myTypeA, myColumnB myTypeB"));
		assert(ECLLayouts.getLayouts().containsKey("myTable"));
		assertEquals("", eclBuilder.generateECL("create temp table newTable (myColumnA myTypeA, myColumnB myTypeB"));
	}
	
}
