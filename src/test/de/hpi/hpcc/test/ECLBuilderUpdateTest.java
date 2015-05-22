package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderUpdateTest extends ECLBuilderTest {
	
	@Test
	public void shouldTranslateUpdate() throws HPCCException {
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite);\n","update myTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite);\n", "update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
		assertStatementCanBeParsedAs("join_record := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTable_record update(myTable_record l, join_record r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(myTable(myColumnA = 'anotherValue'), TABLE(, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, update(LEFT, RIGHT), LEFT OUTER) + myTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);\n", "update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
		
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTempTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite, EXPIRE(1));\n","update myTempTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTempTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTempTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite, EXPIRE(1));\n", "update myTempTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
		assertStatementCanBeParsedAs("join_record := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTempTable_record update(myTempTable_record l, join_record r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(myTempTable(myColumnA = 'anotherValue'), TABLE(, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, update(LEFT, RIGHT), LEFT OUTER) + myTempTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE, EXPIRE(1));\n", "update myTempTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
	}

}
