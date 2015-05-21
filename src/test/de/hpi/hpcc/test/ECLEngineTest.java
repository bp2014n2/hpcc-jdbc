package de.hpi.hpcc.test;

import static org.junit.Assert.assertEquals;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.visitor.ECLStatementParser;

public class ECLEngineTest {
	
	private static ECLLayoutsStub layouts = new ECLLayoutsStub(null);
	
	@BeforeClass
	public static void initialize() {
		layouts.setLayout("myTable", "RECORD STRING10 myColumnA; STRING10 myColumnB; STRING10 myColumnC; STRING10 myColumnD; STRING10 myColumnE; END;"); 
		List<Object> keyedColumns = new ArrayList<Object>();
		List<Object> nonKeyedColumns = new ArrayList<Object>();
		keyedColumns.add("myColumnA");
		nonKeyedColumns.add("myColumnC");
		layouts.setIndex("myTable", "myTable_idx_small", keyedColumns, nonKeyedColumns);
		keyedColumns = new ArrayList<Object>();
		nonKeyedColumns = new ArrayList<Object>();
		keyedColumns.add("myColumnA");
		nonKeyedColumns.add("myColumnC");
		keyedColumns.add("myColumnB");
		layouts.setIndex("myTable", "myTable_idx_mid", keyedColumns, nonKeyedColumns);
		keyedColumns = new ArrayList<Object>();
		nonKeyedColumns = new ArrayList<Object>();
		keyedColumns.add("myColumnA");
		nonKeyedColumns.add("myColumnC");
		keyedColumns.add("myColumnB");
		keyedColumns.add("myColumnD");
		nonKeyedColumns.add("myColumnE");
		layouts.setIndex("myTable", "myTable_idx_big", keyedColumns, nonKeyedColumns);
	}
	
	private static String selectIndex(ECLEngine engine, String table) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		/*
		Class[] cArg = new Class[1];
		cArg[0] = String.class;
		Method method = ECLEngine.class.getDeclaredMethod("getIndex", cArg);
		method.setAccessible(true);
		Object[] args = new Object[1];
		args[0] = table;
		return (String) method.invoke(engine, args);
		*/
		
		return engine.getIndex(table);
	}
	
	private static ECLEngine getEngine(String sql) throws HPCCException {
		ECLStatementParser parser = new ECLStatementParser(layouts);
		ECLEngine engine = parser.getEngine(sql);
		return engine;
	}
	
	@Test
	public void shouldFindIndex() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, HPCCException {
		ECLEngine engine = getEngine("SELECT myColumnA FROM myTable");
		assertEquals("myTable_idx_small", selectIndex(engine, "myTable"));
		
		engine = getEngine("SELECT myColumnA FROM myTable WHERE myColumnA IN (SELECT myColumnA FROM myTable WHERE myColumnB = myValue)");
		assertEquals("myTable_idx_mid", selectIndex(engine, "myTable"));
		
		engine = getEngine("SELECT myColumnA FROM myTable WHERE myColumnE = myValue");
		assertEquals("myTable_idx_big", selectIndex(engine, "myTable"));
		
		engine = getEngine("SELECT myColumnA, COUNT(*) FROM myTable WHERE myColumnC = myValue GROUP BY myColumnC");
		assertEquals("myTable_idx_small", selectIndex(engine, "myTable"));

//		engine = getEngine("SELECT myColumnA FROM myTable WHERE myOhterColumn = undefined");
//		assertNull(selectIndex(engine, "myTable"));
	}
	
}
