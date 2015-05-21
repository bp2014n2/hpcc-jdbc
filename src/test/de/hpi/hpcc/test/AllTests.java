package de.hpi.hpcc.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ ECLBuilderTest.class, ECLBuilderInsertTest.class, ECLBuilderSelectTest.class, ECLColumnFinderTest.class, 
	ECLDataTypeParserTest.class, ECLEngineTest.class, ECLSelectItemFinderTest.class, ECLTableFinderTest.class, 
	HPCCDriverTest.class, HPCCUrlParserTest.class })
public class AllTests {

}
