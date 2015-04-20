package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import de.hpi.hpcc.parsing.create.ECLBuilderCreate;
import de.hpi.hpcc.parsing.drop.ECLBuilderDrop;
import de.hpi.hpcc.parsing.insert.ECLBuilderInsert;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.update.ECLBuilderUpdate;

public class ECLBuilderTest {
	
	private static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	@BeforeClass
	public static void initialize() {
//		eclBuilder = new ECLBuilder();
		eclLayouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE(myTable, {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable"));
		assertEquals("TABLE(myTable, {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable"));
		assertEquals("DEDUP(TABLE(myTable, {myColumn, myColumnA, myColumnB}), All)", eclBuilder.generateECL("select distinct * from mySchema.myTable"));
		assertEquals("DEDUP(TABLE(myTable, {myColumn}), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable"));
		assertEquals("TABLE(myTable, {myColumnA, myNewColumnB := myColumnB})", eclBuilder.generateECL("select myColumnA, myColumnB as myNewColumnB from mySchema.myTable"));
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE(myTable(myColumn = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("TABLE(myTable(myColumn = 'foo'), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn = 'foo'"));
		assertEquals("TABLE(myTable(myColumn = ''), {myColumn})", eclBuilder.generateECL("select myColumn from mySchema.myTable where myColumn is NULL"));
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo%'"));
		assertEquals("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", eclBuilder.generateECL("select * from mySchema.myTable where myColumn like 'foo'"));
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("SORT(TABLE(myTable, {myColumn, myColumnA, myColumnB}), myColumn)", eclBuilder.generateECL("select * from mySchema.myTable order by myColumn"));
		assertEquals("SORT(TABLE(myTable, {myColumn}), myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable order by myColumn"));
		assertEquals("DEDUP(SORT(TABLE(myTable, {myColumn}), myColumn), All)", eclBuilder.generateECL("select distinct myColumn from mySchema.myTable order by myColumn"));
		assertEquals("SORT(TABLE(myTable, {count_ := COUNT(GROUP), myColumn}, myColumn), count_)", eclBuilder.generateECL("select myColumn from myTable group by myColumn order by count(*)"));
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE(myTable, {myColumn}, myColumn)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn"));
		assertEquals("TABLE(myTable, {myColumn}, myColumnA, myColumnB)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumnA, myColumnB"));
		assertEquals("TABLE(myTable, {count_myColumn := COUNT(GROUP)}, myColumn)", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable group by myColumn"));
		assertEquals("SORT(TABLE(myTable, {count_ := COUNT(GROUP), myColumn}, myColumn), count_)", eclBuilder.generateECL("select myColumn from mySchema.myTable group by myColumn order by count(*)"));
	}
	
	@Test
	public void shouldTranslateSelectWithCount() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("COUNT(myTable)", eclBuilder.generateECL("select count(*) from mySchema.myTable"));
		assertEquals("COUNT(myTable)", eclBuilder.generateECL("select count(myColumn) from mySchema.myTable"));
	}
		
	@Test
	public void shouldTranslateSelectWithLimit() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("CHOOSEN(TABLE(myTable, {myColumn}), 1)", eclBuilder.generateECL("select myColumn from mySchema.myTable limit 1"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
//		not implemented yet
		assertEquals("JOIN(myTableA, myTableB, left.myColumnA = right.myColumnB), {myColumn}", eclBuilder.generateECL("select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE((TABLE(myTable, {myColumnA, myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from (select myColumnA, myColumnB from myTable)"));
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
		assertEquals("TABLE(myTableA(myColumnB IN SET(TABLE(myTableB, {myColumnC}),myColumnB)), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)"));
//		assertEquals("Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})", eclBuilder.generateECL("select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')"));
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithFunction() {
		ECLBuilderSelect eclBuilder = new ECLBuilderSelect(eclLayouts);
//		not implemented yet
		assertEquals("", eclBuilder.generateECL("select nextval('mySequence')"));
	}
	
	@Test
	public void shouldTranslateInsertInto() {
		ECLBuilderInsert eclBuilder = new ECLBuilderInsert(eclLayouts);
		assertEquals("OUTPUT(DATASET([{valueA, valueB, valueC}], myTable_record),,'~%NEWTABLE%', overwrite);\n",eclBuilder.generateECL("insert into myTable values (valueA, valueB, valueC)"));
		assertEquals("OUTPUT(TABLE(DATASET([{valueA}], {STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) values (valueA)"));
		assertEquals("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING50 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB)"));
		assertEquals("OUTPUT(TABLE(DATASET([{valueB, valueA}], {STRING50 myColumnB, STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnB, myColumnA) values (valueB, valueA)"));
		assertEquals("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING50 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *"));
		assertEquals("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {myColumnB}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x"));
//		assertEquals("OUTPUT(TABLE(TABLE((TABLE(anotherTable, {myColumnA})), {myColumnA}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)"));
	}
	
	@Test
	public void shouldTranslateUpdate() {
		ECLBuilderUpdate eclBuilder = new ECLBuilderUpdate(eclLayouts);
		assertEquals("toUpdate := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite);\n",eclBuilder.generateECL("update myTable set myColumn = 'myValue'"));
		assertEquals("toUpdate := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite);\n", eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'"));	
		assertEquals("join_record := RECORD STRING50 myColumnB; STRING50 myColumnA; END;\nmytable_record update(mytable_record l, join_record r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(mytable(myColumnA = 'anotherValue'), TABLE(, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, update(LEFT, RIGHT), LEFT OUTER) + mytable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);", eclBuilder.generateECL("update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)"));
	} 
	
	@Test
	public void shouldTranslateDropTable() {
		ECLBuilderDrop eclBuilder = new ECLBuilderDrop(eclLayouts);
		assertEquals("Std.File.DeleteLogicalFile('~i2b2demodata::myTable', true)", eclBuilder.generateECL("drop table myTable"));		
	}
	
	@Test
	public void shouldCreateTable() {
		ECLBuilderCreate eclBuilder = new ECLBuilderCreate(eclLayouts);
		assertEquals("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING37 myColumnB, STRING25 myColumnC}),,'~%NEWTABLE%',OVERWRITE);", eclBuilder.generateECL("create table newTable (myColumnA int, myColumnB varchar(37), myColumnC timestamp)"));
		assertEquals("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING12 myColumnB}),,'~%NEWTABLE%',OVERWRITE);", eclBuilder.generateECL("create temp table newTable (myColumnA int, myColumnB varchar(12))"));
	}
	
}
