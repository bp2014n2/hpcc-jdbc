package unitTests;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.regex.Pattern;

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
		ECLLayouts.setLayouts("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		ECLLayouts.setLayouts("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		ECLLayouts.setLayouts("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		assertEquals("TABLE(myTable, {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable"));
		assertEquals("TABLE(myTable, {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable"));
		assertEquals("DEDUP(TABLE(myTable, {myColumn, myColumnA, myColumnB}), All)", eclBuilder.generateECL("select distinct * from mySchema.myTable"));
		assertEquals("DEDUP(TABLE(myTable, {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable"));
		assertEquals("TABLE(myTable, {myColumnA, myNewColumnB := myColumnB})", eclBuilder.generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"));
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertEquals("TABLE(myTable(myColumn = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("TABLE(myTable(myColumn = 'foo'), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("TABLE(myTable(myColumn = ''), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn is NULL"));
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertEquals("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo%'"));
		assertEquals("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo'"));
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertEquals("TABLE(SORT(TABLE(myTable, {myColumn, myColumnA, myColumnB}), myColumn), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable order by myColumn"));
		assertEquals("TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable order by myColumn"));
		assertEquals("DEDUP(TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable order by myColumn"));
		assertEquals("TABLE(SORT(TABLE(myTable, {count_ := count(group), myColumn}, myColumn), count_), {myColumn})", eclBuilder.generateECL("select myColumn from myTable group by myColumn order by count(*)"));
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		assertEquals("TABLE(myTable, {myColumn}, myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn"));
		assertEquals("TABLE(myTable, {myColumn}, myColumnA, myColumnB)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumnA, myColumnB"));
		assertEquals("TABLE(myTable, {count_myColumn := count(group)}, myColumn)", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable group by myColumn"));
		assertEquals("TABLE(SORT(TABLE(myTable, {count_ := count(group), myColumn}, myColumn), count_), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn order by count(*)"));
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		assertEquals("TABLE(myTable, {count_ := count(group)})", eclBuilder.generateECL("select count(*) from mySchema.myTable"));
		assertEquals("TABLE(myTable, {count_myColumn := count(group)})", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable"));
	}
		
	@Test
	public void shouldTranslateSelectWithLimit() {
		assertEquals("CHOOSEN(TABLE(myTable, {myColumn}), 1)", eclBuilder.generateECL("select myColumn from mySchema.myTable limit 1"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
//		not implemented yet
		assertEquals("JOIN(myTableA, myTableB, left.myColumnA = right.myColumnB), {myColumn}", eclBuilder.generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		assertEquals("TABLE((TABLE(myTable, {myColumnA, myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from (select myColumnA, myColumnB from myTable)"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
		assertEquals("TABLE(myTableA(myColumnB IN SET(TABLE(myTableB, {myColumnC}),myColumnB)), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)"));
//		assertEquals("Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithFunction() {
//		not implemented yet
		assertEquals("", eclBuilder.generateECL("select nextval('mySequence')"));
	}
	
	@Test
	public void shouldTranslateInsertInto() {
		assertEquals("OUTPUT(DATASET([{valueA, valueB, valueC}], myTable_record),,'~%NEWTABLE%', overwrite);\n",eclBuilder.generateECL("insert into myTable values (valueA, valueB, valueC)"));
		assertEquals("OUTPUT(TABLE((DATASET([{valueA}], {STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) values (valueA)"));
		assertEquals("OUTPUT(TABLE((DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING50 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB)"));
		assertEquals("OUTPUT(TABLE((DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING50 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *"));
		assertEquals("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {myColumnB}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x"));
//		assertEquals("OUTPUT(TABLE(TABLE((TABLE(anotherTable, {myColumnA})), {myColumnA}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"));
	}
	
	@Test
	public void shouldTranslateUpdate() {
		assertEquals("updates := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(updates,, '~%NEWTABLE%', overwrite);\n",eclBuilder.generateECL("update myTable set myColumn = 'myValue'"));
		assertEquals("updates := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+updates,, '~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"));	
	} 
	
	@Test
	public void shouldTranslateDropTable() {
		assertEquals("Std.File.DeleteLogicalFile('~i2b2demodata::myTable', true)", eclBuilder.generateECL("drop table myTable"));		
	}
	
	@Test
	public void shouldCreateTable() {
		assertEquals("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING37 myColumnB, STRING25 myColumnC}),,'~%NEWTABLE%',OVERWRITE);", eclBuilder.generateECL("create table newTable (myColumnA int, myColumnB varchar(37), myColumnC timestamp)"));
		assert(ECLLayouts.getLayouts().containsKey("myTable"));
		assertEquals("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING12 myColumnB}),,'~%NEWTABLE%',OVERWRITE);", eclBuilder.generateECL("create temp table newTable (myColumnA int, myColumnB varchar(12))"));
	}
	
}
