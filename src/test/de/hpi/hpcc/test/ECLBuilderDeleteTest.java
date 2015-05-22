package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderDeleteTest extends ECLBuilderTest {
	
	@Test
	public void shouldTranslateDelete() throws HPCCException {
		assertStatementCanBeParsedAs("OUTPUT(DATASET([],{STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB;}),,'~%NEWTABLE%',OVERWRITE);", "delete from myTable");		
	}

}
