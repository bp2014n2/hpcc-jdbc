package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

public class ECLBuilderTest {
	
	private static ECLLayoutsStub eclLayouts = new ECLLayoutsStub(null);
	
	public static void assertStatementCanBeParsedAs(String expected, String sql) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(eclLayouts);
		ECLBuilder builder;
		builder = typeParser.getBuilder(sql);
		assertEquals(expected, builder.generateECL());
    }
	
	@BeforeClass
	public static void initialize() {
//		eclBuilder = new ECLBuilder();
		eclLayouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		eclLayouts.setLayout("mytablea", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
		eclLayouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	@Test
	public void shouldTranslateUpdate() throws HPCCException {
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable, {myColumnA, myColumnB}), {STRING50 myColumn := 'myValue', myColumnA, myColumnB});\nOUTPUT(toUpdate,, '~%NEWTABLE%', overwrite);\n","update myTable set myColumn = 'myValue'");
		assertStatementCanBeParsedAs("toUpdate := TABLE(TABLE(myTable(myColumnB = 'anotherValue'), {myColumn, myColumnB}), {myColumn, STRING50 myColumnA := 'myValue', myColumnB});\nOUTPUT(myTable(NOT(myColumnB = 'anotherValue'))+toUpdate,, '~%NEWTABLE%', overwrite);\n", "update myTable set myColumnA = 'myValue' where myColumnB = 'anotherValue'");	
		assertStatementCanBeParsedAs("join_record := RECORD STRING25 myColumnB; STRING50 myColumnA; END;\nmyTable_record update(myTable_record l, join_record r) := TRANSFORM\n  SELF.myColumn := l.myColumn;\n  SELF.myColumnA := IF(r.myColumnA = '', l.myColumnA, r.myColumnA);\n  SELF.myColumnB := l.myColumnB;\nEND;\nOUTPUT(JOIN(myTable(myColumnA = 'anotherValue'), TABLE(, {myColumnB, STRING50 myColumnA := 'myValue'}), LEFT.myColumnB = RIGHT.myColumnB, update(LEFT, RIGHT), LEFT OUTER) + myTable(NOT myColumnA = 'anotherValue'),,'~%NEWTABLE%',OVERWRITE);", "update myTable set myColumnA = 'myValue' where myColumnA = 'anotherValue' and exists (select 1 from myTableA where myTable.myColumnB = myTableA.myColumnB)");
	} 
	
	@Test
	public void shouldTranslateDropTable() throws HPCCException {
		assertStatementCanBeParsedAs("Std.File.DeleteLogicalFile('~i2b2demodata::myTable', true)", "drop table myTable");		
	}
	
	@Test
	public void shouldTranslateDelete() throws HPCCException {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB;}),,'~%NEWTABLE%',OVERWRITE);", "delete from myTable");		
	}
	
	@Test
	public void shouldCreateTable() throws HPCCException {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING37 myColumnB, STRING25 myColumnC}),,'~%NEWTABLE%',OVERWRITE);", "create table newTable (myColumnA int, myColumnB varchar(37), myColumnC timestamp)");
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING12 myColumnB}),,'~%NEWTABLE%',OVERWRITE);", "create temp table newTable (myColumnA int, myColumnB varchar(12))");
	}
	
}
