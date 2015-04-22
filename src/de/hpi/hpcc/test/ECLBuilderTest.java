package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLStatementParser;

public class ECLBuilderTest {
	
	private static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	public static void assertStatementCanBeParsedAs(String expected, String sql) {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		ECLBuilder builder;
		try {
			builder = typeParser.getBuilder(sql);
			assertEquals(expected, builder.generateECL());
		} catch (HPCCException e) {

		}
    }
	
	@BeforeClass
	public static void initialize() {
//		eclBuilder = new ECLBuilder();
		eclLayouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		eclLayouts.setLayout("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	@Test
	public void shouldTranslateSimpleSelect() throws SQLException {
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn})", "select myColumn from mySchema.myTable");
		assertStatementCanBeParsedAs("DEDUP(TABLE(myTable, {myColumn, myColumnA, myColumnB}), All)", "select distinct * from mySchema.myTable");
		assertStatementCanBeParsedAs("DEDUP(TABLE(myTable, {myColumn}), All)", "select distinct myColumn from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumnA, STRING25 myNewColumnB := myColumnB})", "select myColumnA, myColumnB as myNewColumnB from mySchema.myTable");
	}

	@Test
	public void shouldTranslateSelectWithWhere() {
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn = 'foo'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = 'foo'), {myColumn})", "select myColumn from mySchema.myTable where myColumn = 'foo'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = ''), {myColumn})", "select myColumn from mySchema.myTable where myColumn is NULL");
	}
	
	@Test
	public void shouldTranslateSelectWithLike() {
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn like 'foo%'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn like 'foo'");
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() {
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {myColumn, myColumnA, myColumnB}), myColumn), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn})", "select myColumn from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("DEDUP(TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn}), All)", "select distinct myColumn from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP), myColumn}, myColumn), func_count), {myColumn})", "select myColumn from myTable group by myColumn order by count(*)");
		
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() {
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn}, myColumn)", "select myColumn from mySchema.myTable group by myColumn");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn}, myColumnA, myColumnB)", "select myColumn from mySchema.myTable group by myColumnA, myColumnB");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)}, myColumn)", "select count(myColumn) from mySchema.myTable group by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP), myColumn}, myColumn), func_count), {myColumn})", "select myColumn from mySchema.myTable group by myColumn order by count(*)");
	}
		
	@Test
	public void shouldTranslateSelectWithLimit() {
		assertStatementCanBeParsedAs("CHOOSEN(TABLE(myTable, {myColumn}), 1)", "select myColumn from mySchema.myTable limit 1");
	}
	
	@Test @Ignore
	public void shouldTranslateSelectWithJoin() {
//		not implemented yet
		assertStatementCanBeParsedAs("JOIN(myTableA, myTableB, left.myColumnA = right.myColumnB), {myColumn}", "select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() {
		assertStatementCanBeParsedAs("TABLE((TABLE(myTable, {myColumnA, myColumnB})), {myColumnA})", "select myColumnA from (select myColumnA, myColumnB from myTable)");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() {
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnB IN SET(TABLE(myTableB, {myColumnC}),myColumnB)), {myColumnA})", "select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)");
//		assertStatementCanBeParsedAs("Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})", "select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')");
	}
	
	@Test
	public void shouldTranslateSelectWithFunction() {
		//assertStatementCanBeParsedAs("", "select nextval('mySequence')");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)})", "select count(*) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)})", "select count(myColumn) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 anotherName := COUNT(GROUP)})", "select count(myColumn) as anotherName from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_sum := SUM(GROUP, myColumn)})", "select sum(myColumn) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 anotherName := SUM(GROUP, myColumn)})", "select sum(myColumn) as anotherName from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 substring := myColumn[1..3]})", "select substring(myColumn from 1 for 3) as substring from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 func_substring := myColumn[2..5]})", "select substring(myColumn from 2 for 4) from mySchema.myTable");
	}
	
	@Test
	public void shouldTranslateInsertInto() {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([{valueA, valueB, valueC}], myTable_record),,'~%NEWTABLE%', overwrite);\n","insert into myTable values (valueA, valueB, valueC)");
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA}], {STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) values (valueA)");
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) values (valueA, valueB)");
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueB, valueA}], {STRING25 myColumnB, STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnB, myColumnA) values (valueB, valueA)");
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *");
		assertStatementCanBeParsedAs("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {myColumnB}),{STRING50 myColumn := '', myColumnA, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x");
//		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE((TABLE(anotherTable, {myColumnA})), {myColumnA}),{STRING50 myColumn := '', myColumnA, STRING50 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)");
	}
	
	@Test
	public void shouldTranslateUpdate() {
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite);\n","update myTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite);\n", "update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
		assertStatementCanBeParsedAs("join_record := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmytable_record update(mytable_record l, join_record r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(mytable(myColumnA = 'anotherValue'), TABLE(, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, update(LEFT, RIGHT), LEFT OUTER) + mytable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);", "update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
	} 
	
	@Test
	public void shouldTranslateDropTable() {
		assertStatementCanBeParsedAs("Std.File.DeleteLogicalFile('~i2b2demodata::myTable', true)", "drop table myTable");		
	}
	
	@Test
	public void shouldCreateTable() {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING37 myColumnB, STRING25 myColumnC}),,'~%NEWTABLE%',OVERWRITE);", "create table newTable (myColumnA int, myColumnB varchar(37), myColumnC timestamp)");
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING12 myColumnB}),,'~%NEWTABLE%',OVERWRITE);", "create temp table newTable (myColumnA int, myColumnB varchar(12))");
	}
	
}
