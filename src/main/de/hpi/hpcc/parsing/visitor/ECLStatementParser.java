package de.hpi.hpcc.parsing.visitor;

import de.hpi.hpcc.main.HPCCException;
import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLEngine;
import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.SQLParser;
import de.hpi.hpcc.parsing.create.ECLBuilderCreate;
import de.hpi.hpcc.parsing.create.ECLEngineCreate;
import de.hpi.hpcc.parsing.create.SQLParserCreate;
import de.hpi.hpcc.parsing.delete.ECLBuilderDelete;
import de.hpi.hpcc.parsing.delete.ECLEngineDelete;
import de.hpi.hpcc.parsing.delete.SQLParserDelete;
import de.hpi.hpcc.parsing.drop.ECLBuilderDrop;
import de.hpi.hpcc.parsing.drop.ECLEngineDrop;
import de.hpi.hpcc.parsing.drop.SQLParserDrop;
import de.hpi.hpcc.parsing.insert.ECLBuilderInsert;
import de.hpi.hpcc.parsing.insert.ECLEngineInsert;
import de.hpi.hpcc.parsing.insert.SQLParserInsert;
import de.hpi.hpcc.parsing.select.ECLBuilderSelect;
import de.hpi.hpcc.parsing.select.ECLEngineSelect;
import de.hpi.hpcc.parsing.select.SQLParserSelect;
import de.hpi.hpcc.parsing.select.SQLParserSelectVisitor;
import de.hpi.hpcc.parsing.update.ECLBuilderUpdate;
import de.hpi.hpcc.parsing.update.ECLEngineUpdate;
import de.hpi.hpcc.parsing.update.SQLParserUpdate;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class ECLStatementParser implements StatementVisitor {

	private SQLParser parser;
	private ECLEngine engine;
	private ECLBuilder builder;
	private ECLLayouts layouts;

	public ECLStatementParser(ECLLayouts layouts) {
		this.layouts = layouts;
	}

	public SQLParser getParser(String sql) throws HPCCException {
		Statement statement = SQLParser.parse(sql);
		statement.accept(this);
		return parser;
	}
	
	public ECLEngine getEngine(String sql) throws HPCCException {
		Statement statement = SQLParser.parse(sql);
		statement.accept(this);
		return engine;
	}
	
	public ECLBuilder getBuilder(String sql) throws HPCCException {
		Statement statement = SQLParser.parse(sql);
		statement.accept(this);
		return builder;
	}
	
	@Override
	public void visit(Select select) {
		SQLParserSelectVisitor selectVisitor = new SQLParserSelectVisitor(layouts);
		parser = selectVisitor.find(select.getSelectBody());
		engine = new ECLEngineSelect(select, layouts);
		builder = new ECLBuilderSelect(select, layouts);
	}

	@Override
	public void visit(Delete delete) {
		parser = new SQLParserDelete(delete, layouts);
		engine = new ECLEngineDelete(delete, layouts);
		builder = new ECLBuilderDelete(delete, layouts);
	}

	@Override
	public void visit(Update update) {
		parser = new SQLParserUpdate(update, layouts);
		engine = new ECLEngineUpdate(update, layouts);
		builder = new ECLBuilderUpdate(update, layouts);
	}

	@Override
	public void visit(Insert insert) {
		parser = new SQLParserInsert(insert, layouts);
		engine = new ECLEngineInsert(insert, layouts);
		builder = new ECLBuilderInsert(insert, layouts);
	}

	@Override
	public void visit(Replace replace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop drop) {
		parser = new SQLParserDrop(drop, layouts);
		engine = new ECLEngineDrop(drop, layouts);
		builder = new ECLBuilderDrop(drop, layouts);
	}

	@Override
	public void visit(Truncate truncate) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateIndex createIndex) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateTable createTable) {
		parser = new SQLParserCreate(createTable, layouts);
		engine = new ECLEngineCreate(createTable, layouts);
		builder = new ECLBuilderCreate(createTable, layouts);
	}

	@Override
	public void visit(CreateView createView) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Alter alter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Statements stmts) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Execute execute) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SetStatement set) {
		// TODO Auto-generated method stub
		
	}

}
