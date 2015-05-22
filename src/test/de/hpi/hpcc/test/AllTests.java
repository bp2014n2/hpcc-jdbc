package de.hpi.hpcc.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ ECLBuilderUpdateTest.class, ECLBuilderDeleteTest.class, ECLBuilderDropTest.class, ECLBuilderInsertTest.class, 
	ECLBuilderSelectTest.class, ECLBuilderCreateTest.class, ECLColumnFinderTest.class, 
	ECLDataTypeParserTest.class, ECLEngineTest.class, ECLSelectItemFinderTest.class, ECLSelectTableFinderTest.class,
	ECLTableFinderTest.class, HPCCDriverTest.class, HPCCUrlParserTest.class })
public class AllTests {

}
