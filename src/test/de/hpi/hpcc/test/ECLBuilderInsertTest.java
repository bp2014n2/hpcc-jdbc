package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderInsertTest extends ECLBuilderTest {

	@Test
	public void shouldTranslateSimpleInsertInto() throws HPCCException {
		// insert into myTable values (valueA, valueB, valueC)
		assertStatementCanBeParsedAs("OUTPUT(DATASET([{valueA, valueB, valueC}], myTable_record),,'~%NEWTABLE%', overwrite);\n","insert into myTable values (valueA, valueB, valueC)");
		//insert into myTable (myColumnA) values (valueA)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA}], {STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) values (valueA)");
		//insert into myTable (myColumnA, myColumnB) values (valueA, valueB)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) values (valueA, valueB)");
		//insert into myTable (myColumnB, myColumnA) values (valueB, valueA)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueB, valueA}], {STRING25 myColumnB, STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnB, myColumnA) values (valueB, valueA)");
		//insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) values (valueA, valueB) returning *");
		
		// insert into myTempTable values (valueA, valueB, valueC)
		assertStatementCanBeParsedAs("OUTPUT(DATASET([{valueA, valueB, valueC}], myTempTable_record),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n","insert into myTempTable values (valueA, valueB, valueC)");
		//insert into myTempTable (myColumnA) values (valueA)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA}], {STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnA) values (valueA)");
		//insert into myTempTable (myColumnA, myColumnB) values (valueA, valueB)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnA, myColumnB) values (valueA, valueB)");
		//insert into myTempTable (myColumnB, myColumnA) values (valueB, valueA)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueB, valueA}], {STRING25 myColumnB, STRING50 myColumnA}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnB, myColumnA) values (valueB, valueA)");
		//insert into myTempTable (myColumnA, myColumnB) values (valueA, valueB) returning *
		assertStatementCanBeParsedAs("OUTPUT(TABLE(DATASET([{valueA, valueB}], {STRING50 myColumnA, STRING25 myColumnB}),{STRING50 myColumn := '', myColumnA, myColumnB}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnA, myColumnB) values (valueA, valueB) returning *");
					
	}
	
	@Test
	public void shouldTranslateInsertWithWith() throws HPCCException {
		//insert into myTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x
		assertStatementCanBeParsedAs("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {myColumnB}),{STRING50 myColumn := '', STRING50 myColumnA := myColumnB, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x");
		//insert into myTable (myColumnA, myColumnB) with x as (select myColumnB from anotherTable) select 'myValue', x.myColumnB from x
		assertStatementCanBeParsedAs("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {STRING50 string_myValue := 'myValue', myColumnB}),{STRING50 myColumn := '', STRING50 myColumnA := string_myValue, STRING25 myColumnB := myColumnB}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) with x as (select myColumnB from anotherTable) select 'myValue', x.myColumnB from x");		
		//insert into myTempTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x
		assertStatementCanBeParsedAs("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {myColumnB}),{STRING50 myColumn := '', STRING50 myColumnA := myColumnB, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnA) with x as (select myColumnB from anotherTable) select x.myColumnB from x");
		//insert into myTempTable (myColumnA, myColumnB) with x as (select myColumnB from anotherTable) select 'myValue', x.myColumnB from 
		assertStatementCanBeParsedAs("x := TABLE(anotherTable, {myColumnB});\nOUTPUT(TABLE(TABLE(x, {STRING50 string_myValue := 'myValue', myColumnB}),{STRING50 myColumn := '', STRING50 myColumnA := string_myValue, STRING25 myColumnB := myColumnB}),,'~%NEWTABLE%', overwrite, EXPIRE(1));\n", "insert into myTempTable (myColumnA, myColumnB) with x as (select myColumnB from anotherTable) select 'myValue', x.myColumnB from x");		
		//insert into myTable (myColumn) with x as (select myColumn from myTableA), y as (select myColumn from myTableB)select myColumn from y
		//assertStatementCanBeParsedAs("x := TABLE(myTableA, {myColumn});\ny := TABLE(myTableB, {myColumn});\nOUTPUT(TABLE(TABLE(y, {myColumn}),{STRING50 myColumn := myColumn, STRING50 myColumnA := '', STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumn) with x as (select myColumn from myTableA), y as (select myColumn from myTableB)select myColumn from y");		
		
	}
	
	
	@Test
	public void shouldTranslateInsertWithSelect() throws HPCCException {
		//insert into myTable (myColumnA) select 'myValue' from anotherTable
		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE(anotherTable, {STRING50 string_myValue := 'myValue'}),{STRING50 myColumn := '', STRING50 myColumnA := string_myValue, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) select 'myValue' from anotherTable");		
		//insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)
		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE((TABLE(anotherTable, {myColumnB})), {myColumnB}),{STRING50 myColumn := '', STRING50 myColumnA := myColumnB, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA) select * from (select myColumnB from anotherTable)");
		//insert into myTable (myColumnA, myColumnB) select 'myValue', 'mySecondValue'  from myTableB
		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE(myTableB, {STRING50 string_myValue := 'myValue', STRING50 string_mySecondValue := 'mySecondValue'}),{STRING50 myColumn := '', STRING50 myColumnA := string_myValue, STRING25 myColumnB := string_mySecondValue}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnA, myColumnB) select 'myValue', 'mySecondValue'  from myTableB");
		// insert into myTable (myColumnB, myColumnA) select 'mySecondValue', 'myValue'  from myTableB
		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE(myTableB, {STRING50 string_mySecondValue := 'mySecondValue', STRING50 string_myValue := 'myValue'}),{STRING50 myColumn := '', STRING50 myColumnA := string_myValue, STRING25 myColumnB := string_mySecondValue}),,'~%NEWTABLE%', overwrite);\n", "insert into myTable (myColumnB, myColumnA) select 'mySecondValue', 'myValue'  from myTableB");
		// INSERT INTO myTable (myColumnA, myNewColumnB) SELECT 'myValue' AS myColumnA, ROW_NUMBER() OVER(ORDER BY myColumnB) AS myNewColumnB FROM (SELECT DISTINCT myColumnB FROM myTableA) t
		assertStatementCanBeParsedAs("OUTPUT(TABLE(TABLE((DEDUP(TABLE(myTableA, {myColumnB}), All)), {STRING50 myColumnA := 'myValue', INTEGER5 myNewColumnB := 0}),{STRING50 myColumn := '', STRING50 myColumnA := myColumnA, STRING25 myColumnB := ''}),,'~%NEWTABLE%', overwrite);\n", "INSERT INTO myTable (myColumnA, myNewColumnB) SELECT 'myValue' AS myColumnA, ROW_NUMBER() OVER(ORDER BY myColumnB) AS myNewColumnB FROM (SELECT DISTINCT myColumnB FROM myTableA) t");
		// INSERT INTO myTable (myColumnA, myNewColumnB, myColumnC) SELECT 'myValue' AS myColumnA, ROW_NUMBER() OVER(ORDER BY myColumnB) AS myNewColumnB, myColumnC FROM (SELECT DISTINCT myColumnB FROM myTableA) t
		assertStatementCanBeParsedAs("OUTPUT(TABLE((DEDUP(TABLE(myTableA, {myColumnB}), All)), {STRING50 myColumnA := 'myValue', INTEGER5 myNewColumnB := 0, myColumnC}),,'~%NEWTABLE%', overwrite);\n", "INSERT INTO myTable (myColumnA, myNewColumnB, myColumnC) SELECT 'myValue' AS myColumnA, ROW_NUMBER() OVER(ORDER BY myColumnB) AS myNewColumnB, myColumnC FROM (SELECT DISTINCT myColumnB FROM myTableA) t");
	}
}
