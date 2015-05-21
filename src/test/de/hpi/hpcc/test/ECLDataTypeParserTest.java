package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.junit.BeforeClass;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.visitor.ECLDataTypeParser;
import de.hpi.hpcc.parsing.visitor.ECLSelectItemFinder;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

public class ECLDataTypeParserTest {
	
private static ECLLayoutsStub layouts = new ECLLayoutsStub(null);
	
	@BeforeClass
	public static void initialize() {
		layouts.setLayout("mytable", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING25 myColumnB; END;");
		layouts.setLayout("mytablea", "RECORD INTEGER8 myColumn; INTEGER8 myColumnA; STRING50 myColumnB; END;");
		layouts.setLayout("mytableb", "RECORD STRING50 myColumn; STRING50 myColumnA; STRING50 myColumnB; END;");
	}
	
	public static void assertCorrectDataType(List<String> expectedTypes, String sql) throws HPCCException {
		ECLStatementParser typeParser = new ECLStatementParser(layouts);
		SQLParserSelect sqlParser = (SQLParserSelect) typeParser.getParser(sql);
		ECLDataTypeParser parser = new ECLDataTypeParser(layouts, sqlParser);
		ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
    	List<SelectExpressionItem> selectItems = finder.find(sqlParser.getStatement());
    	
    	List<String> foundTypes = new ArrayList<String>();
		for(int i = 0; i < selectItems.size(); i++) {
			SelectExpressionItem selectItem = selectItems.get(i);
			foundTypes.add(parser.parse(selectItem.getExpression()));
		}	
		assertEquals(expectedTypes, foundTypes);
    }

	@Test
	public void shouldFindDataType() throws HPCCException {
		List<String> expectedTypes = new ArrayList<String>();
		expectedTypes.add("STRING50");
		assertCorrectDataType(expectedTypes, "SELECT myColumn from myTable");
		expectedTypes.add("STRING25");
		assertCorrectDataType(expectedTypes, "SELECT myColumn, myColumnB from myTable");
		expectedTypes.clear();
		expectedTypes.add("INTEGER8");
		assertCorrectDataType(expectedTypes, "SELECT myColumn from myTableA");
		assertCorrectDataType(expectedTypes, "SELECT myColumn_alias from (select myColumn as myColumn_alias from myTableA)");
	}
	
	@Test
	public void shouldFindDataTypeForSubstring() throws HPCCException {
		List<String> expectedTypes = new ArrayList<String>();
		expectedTypes.add("STRING50");
		assertCorrectDataType(expectedTypes, "select substring(myColumn from 2 for 4) from myTableA");
		assertCorrectDataType(expectedTypes, "select substring(myColumn from 2 for 4) as myColumn_sub from myTableA");
		assertCorrectDataType(expectedTypes, "select myColumn_sub from (select substring(myColumn from 2 for 4) as myColumn_sub from myTable) t");
		expectedTypes.add("STRING25");
		assertCorrectDataType(expectedTypes, "select myColumn_sub, myColumnB from (select myColumnB, substring(myColumn from 2 for 4) as myColumn_sub from myTable) t");
		expectedTypes.clear();
		expectedTypes.add("STRING25");
		expectedTypes.add("STRING50");
		assertCorrectDataType(expectedTypes, "select myColumnB, myColumn_sub from (select myColumnB, substring(myColumn from 2 for 4) as myColumn_sub from myTable) t");
	}
	
	@Test
	public void shouldFindDataTypeForFunctions() throws HPCCException {
		List<String> expectedTypes = new ArrayList<String>();
		expectedTypes.add("INTEGER8");
		assertCorrectDataType(expectedTypes, "select sum(myColumn from 2 for 4) from myTable");
		assertCorrectDataType(expectedTypes, "select sum(myColumn from 2 for 4) as myColumn_sum from myTableA");
		assertCorrectDataType(expectedTypes, "select myColumn_sum from (select substring(myColumn from 2 for 4) as myColumn_sum from myTableA) t");
		expectedTypes.add("STRING25");
		assertCorrectDataType(expectedTypes, "select myColumn_sum, myColumnB from (select myColumnB, substring(myColumn from 2 for 4) as myColumn_sum from myTableA) t");
		expectedTypes.clear();
		expectedTypes.add("STRING25");
		expectedTypes.add("INTEGER8");
		assertCorrectDataType(expectedTypes, "select myColumnB, myColumn_sum from (select myColumnB, substring(myColumn from 2 for 4) as myColumn_sum from myTableA) t");
	}
}
