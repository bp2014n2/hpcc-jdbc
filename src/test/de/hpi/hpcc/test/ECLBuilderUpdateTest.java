package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderUpdateTest extends ECLBuilderTest {
	
	@Test
	public void shouldTranslateUpdate() throws HPCCException {
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite);\n","update myTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite);\n", "update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
		
		//Temp Tables
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTempTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite, EXPIRE(1));\n","update myTempTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTempTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTempTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite, EXPIRE(1));\n", "update myTempTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
	}
	
	@Test
	public void shouldTranslateUpdateWithExists() throws HPCCException {
		assertStatementCanBeParsedAs("exists_record1 := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTable_record exists1(myTable_record l, exists_record1 r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(myTable(myColumnA = 'anotherValue'), TABLE(myTableA, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, exists1(LEFT, RIGHT), LEFT OUTER) + myTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);\n", "update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
		//currently not supported
		//assertStatementCanBeParsedAs("exist_record1 := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTable_record exist1(myTable_record l, exist_record1 r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nexist_record2 := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTable_record exist2(myTable_record l, exist_record2 r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(JOIN(myTable(myColumnA = 'anotherValue'), TABLE(myTableA, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, exist1(LEFT, RIGHT), LEFT OUTER), TABLE(myTableB, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, exist2(LEFT, RIGHT), LEFT OUTER) + myTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);\n", "update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB) and exists (select 1 from myTableb where myTable.myColumnB = myTableb.myColumnB)");
		
		//Temp Tables
		assertStatementCanBeParsedAs("exists_record1 := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTempTable_record exists1(myTempTable_record l, exists_record1 r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(myTempTable(myColumnA = 'anotherValue'), TABLE(myTableA, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, exists1(LEFT, RIGHT), LEFT OUTER) + myTempTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE, EXPIRE(1));\n", "update myTempTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
	}
}
