package de.hpi.hpcc.test;

import org.junit.Ignore;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderSelectTest extends ECLBuilderTest {

	@Test
	public void shouldTranslateSimpleSelect() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn})", "select myColumn from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 string_myValue := 'myValue'})", "select 'myValue' from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 string_myValuemyValue := 'myValue:myValue'})", "select 'myValue:myValue' from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 string_myValuemyValuemyValue := 'myValue:myValue:myValue'})", "select 'myValue:myValue:myValue' from mySchema.myTable");
		assertStatementCanBeParsedAs("DEDUP(TABLE(myTable, {myColumn, myColumnA, myColumnB}), All)", "select distinct * from mySchema.myTable");
		assertStatementCanBeParsedAs("DEDUP(TABLE(myTable, {myColumn}), All)", "select distinct myColumn from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumnA, STRING25 myNewColumnB := myColumnB})", "select myColumnA, myColumnB as myNewColumnB from mySchema.myTable");
	}

	@Test
	public void shouldTranslateSelectWithWhere() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn = 'foo'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = 'foo'), {myColumn})", "select myColumn from mySchema.myTable where myColumn = 'foo'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn = ''), {myColumn})", "select myColumn from mySchema.myTable where myColumn is NULL");
	}
	
	@Test
	public void shouldTranslateSelectWithLike() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn like 'foo%'");
		assertStatementCanBeParsedAs("TABLE(myTable(myColumn[1..3] = 'foo'), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable where myColumn like 'foo'");
	}
	
	@Test
	public void shouldTranslateSelectWithOrderBy() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {myColumn, myColumnA, myColumnB}), myColumn), {myColumn, myColumnA, myColumnB})", "select * from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn})", "select myColumn from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("DEDUP(TABLE(SORT(TABLE(myTable, {myColumn}), myColumn), {myColumn}), All)", "select distinct myColumn from mySchema.myTable order by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP), myColumn}, myColumn), func_count), {myColumn})", "select myColumn from myTable group by myColumn order by count(*)");
	}
	
	@Test 
	public void shouldTranslateSelectWithGroupBy() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn}, myColumn)", "select myColumn from mySchema.myTable group by myColumn");
		assertStatementCanBeParsedAs("TABLE(myTable, {myColumn}, myColumnA, myColumnB)", "select myColumn from mySchema.myTable group by myColumnA, myColumnB");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)}, myColumn)", "select count(myColumn) from mySchema.myTable group by myColumn");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP), myColumn}, myColumn), func_count), {myColumn})", "select myColumn from mySchema.myTable group by myColumn order by count(*)");
	}
	
	@Test
	public void shouldTranslateSelectWithLimit() throws HPCCException {
		assertStatementCanBeParsedAs("CHOOSEN(TABLE(myTable, {myColumn}), 1)", "select myColumn from mySchema.myTable limit 1");
	}
	
	@Test
	public void shouldTranslateSelectWithJoin() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(JOIN(myTableA, myTableB, 1=1, ALL)(columnA = columnB), {myColumn})", "select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnB");
		assertStatementCanBeParsedAs("TABLE(JOIN(myTableA, myTableB, LEFT.columnA = RIGHT.columnA, LOOKUP)(columnA = columnA), {myColumn})", "select myColumn from mySchema.myTableA, mySchema.myTableB where myTableA.columnA = myTableB.columnA");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInFrom() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE((TABLE(myTable, {myColumnA, myColumnB})), {myColumnA})", "select myColumnA from (select myColumnA, myColumnB from myTable)");
	}
	
	@Test
	public void shouldTranslateSelectWithSubselectInWhere() throws HPCCException {
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnB IN SET(TABLE(myTableB, {myColumnC}),myColumnB)), {myColumnA})", "select myColumnA from myTableA where myColumnB in (select myColumnC from myTableB)");
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnC = 'foo'), {myColumnA})", "select myColumnA from myTableA where myColumnA in (select myColumnA from myTableA where myColumnC = 'foo')");
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnC = 'foo'), {myColumnA})", "select myColumnA from myTableA where myColumnA in (select myColumnA from mySchema.myTableA where myColumnC = 'foo')");
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnC = 'foo'), {myColumnA})", "select myColumnA from mySchema.myTableA where myColumnA in (select myColumnA from mySchema.myTableA a where myColumnC = 'foo')");
		assertStatementCanBeParsedAs("TABLE(myTableA(myColumnC = 'foo'), {myColumnA, myColumnB})", "select myColumnA, myColumnB from myTableA where myColumnA in (select myColumnA from myTableA where myColumnC = 'foo')");
//		assertStatementCanBeParsedAs("Table(myTableA(myColumnB in dictionary([{'myValue1'}, {'myValue2'}], {STRING15 myColumnB})), {myColumnA})", "select myColumnA from myTableA where myColumnB in ('myValue1', 'myValue2')");
	}
	
	@Test
	public void shouldTranslateSelectWithFunction() throws HPCCException {
		//assertStatementCanBeParsedAs("", "select nextval('mySequence')");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)})", "select count(*) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_count := COUNT(GROUP)})", "select count(myColumn) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 anotherName := COUNT(GROUP)})", "select count(myColumn) as anotherName from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 func_sum := SUM(GROUP, myColumn)})", "select sum(myColumn) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {INTEGER8 anotherName := SUM(GROUP, myColumn)})", "select sum(myColumn) as anotherName from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 substring := myColumn[1..3]})", "select substring(myColumn from 1 for 3) as substring from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(myTable, {STRING50 func_substring := myColumn[2..5]})", "select substring(myColumn from 2 for 4) from mySchema.myTable");
		assertStatementCanBeParsedAs("TABLE(SORT(TABLE(myTable(myColumnB IN [514702, 514702, 865892, 300036] AND myColumn[1..4] = '2009'), {myColumnB, myColumn, INTEGER8 func_SUM := SUM(GROUP, myColumnA)}, myColumn), myColumnB), {myColumn, func_SUM})", "SELECT myColumn, SUM(myColumnA) FROM myTable WHERE myColumnB IN (514702, 514702, 865892, 300036) AND SUBSTRING(myColumn FROM 1 FOR 4) = '2009' GROUP BY myColumn ORDER BY myColumnB");
	}
	
	@Ignore @Test
	public void shouldTranslateSelectWithUnionAll() throws HPCCException {
		//select myColumn from myTable group by myColumn union all select myColumn from myTableA group by myColumn
		assertStatementCanBeParsedAs("", "select myColumn from myTable group by myColumn union all select myColumn from myTableA group by myColumn");
	}
}
