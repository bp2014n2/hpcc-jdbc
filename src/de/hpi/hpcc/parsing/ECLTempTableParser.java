package de.hpi.hpcc.parsing;

import net.sf.jsqlparser.schema.Table;
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
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class ECLTempTableParser implements FromItemVisitor, SelectVisitor, StatementVisitor {

	private ECLLayouts layouts;

	public ECLTempTableParser(ECLLayouts layouts) {
		this.layouts = layouts;
	}
	
	public void replace(FromItem fromItem) {
		fromItem.accept(this);
	}
	
	public void replace(Statement statement) {
		statement.accept(this);
	}
	
	@Override
	public void visit(Table tableName) {
		String name = tableName.getName();
		if (layouts.isTempTable(name)) {
    		name = layouts.getShortTempTableName(name);
    	}
		tableName.setName(name);
	}

	@Override
	public void visit(SubSelect subSelect) {
		SelectBody sb = subSelect.getSelectBody();
		if (sb != null) {
			sb.accept(this);
		}
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesList valuesList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		FromItem fi = plainSelect.getFromItem();
		if (fi != null) {
			fi.accept(this);
		}
	}

	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem withItem) {
		SelectBody sb = withItem.getSelectBody();
		if (sb != null) {
			sb.accept(this);
		}
	}

	@Override
	public void visit(Select select) {
		SelectBody sb = select.getSelectBody();
		if (sb != null) {
			sb.accept(this);
		}
	}

	@Override
	public void visit(Delete delete) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Update update) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Insert insert) {
		Table table = insert.getTable();
		if (table != null) {
			table.accept(this);
		}
		Select select = insert.getSelect();
		if (select != null) {
			select.accept(this);
		}
	}

	@Override
	public void visit(Replace replace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop drop) {
		String name = drop.getName();
		if (layouts.isTempTable(name)) {
    		name = layouts.getShortTempTableName(name);
    	}
		drop.setName(name);
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

}
