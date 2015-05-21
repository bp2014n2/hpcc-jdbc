package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;

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
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class ECLFromItemsCollector implements StatementVisitor, SelectVisitor {

	
	private List<FromItem> fromItems = new ArrayList<FromItem>();

	public List<FromItem> collect(Statement statement) {
		statement.accept(this);
		return fromItems;
	}
	
	
	@Override
	public void visit(Select select) {
		SelectBody body = select.getSelectBody();
		if (body != null) {
			body.accept(this);
		}
	}

	@Override
	public void visit(Delete delete) {
		fromItems.add(delete.getTable());
	}

	@Override
	public void visit(Update update) {
		fromItems.add(update.getFromItem());
	}

	@Override
	public void visit(Insert insert) {
		fromItems.add(insert.getTable());
	}

	@Override
	public void visit(Replace replace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop drop) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
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


	@Override
	public void visit(PlainSelect plainSelect) {
		fromItems.add(plainSelect.getFromItem());
	}


	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void visit(WithItem withItem) {
		// TODO Auto-generated method stub
		
	}
}
