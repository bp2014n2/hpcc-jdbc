package de.hpi.hpcc.test;

import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;

public class ECLBuilderDropTest extends ECLBuilderTest {
	
	@Test
	public void shouldTranslateDropTable() throws HPCCException {
		assertStatementCanBeParsedAs("Std.File.DeleteLogicalFile('~i2b2demodata::myTable', true)", "drop table myTable");		
	}

}
