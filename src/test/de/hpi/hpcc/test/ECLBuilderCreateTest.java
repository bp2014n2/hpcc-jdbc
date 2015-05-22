package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderCreateTest extends ECLBuilderTest {
	
	@Test
	public void shouldCreateTable() throws HPCCException {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING37 myColumnB, STRING25 myColumnC}),,'~%NEWTABLE%',OVERWRITE);", "create table newTable (myColumnA int, myColumnB varchar(37), myColumnC timestamp)");
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{INTEGER5 myColumnA, STRING12 myColumnB}),,'~%NEWTABLE%',OVERWRITE, EXPIRE(1));", "create temp table newTable (myColumnA int, myColumnB varchar(12))");
	}

}
