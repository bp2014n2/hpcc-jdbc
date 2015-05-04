package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;

public class ECLSelectItemFinder extends FullVisitorAdapter {

	private List<SelectExpressionItem> selectItems = new ArrayList<SelectExpressionItem>();
	private Stack<List<FromItem>> fromItems = new Stack<List<FromItem>>();
	private ECLLayouts layouts;
	
	public ECLSelectItemFinder(ECLLayouts layouts) {
		this.layouts = layouts;
	}

	public List<SelectExpressionItem> find(Statement statement) {
		statement.accept(this);
		return selectItems;
	}
	
	public List<SelectExpressionItem> find(SelectBody selectBody) {
		selectBody.accept(this);
		return selectItems;
	}
	
	@Override
	public void visit(Select select) {
		/* TODO: save columns of withItems when select *
		if(select.getWithItemsList() != null) {
			for(WithItem withItem : select.getWithItemsList()) {
				withItem.accept(this);
			}
		}*/
		tryAccept(select.getSelectBody());
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		List<FromItem> currentFromItems = fromItems.pop();
		for(FromItem fromItem : currentFromItems) {
			ECLAllColumnCollector collector = new ECLAllColumnCollector(layouts);
			selectItems.addAll(collector.collect(fromItem));
		}
	}
	
	@Override
	public void visit(Column tableColumn) {
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		tryAccept(allTableColumns.getTable());
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		selectItems.add(selectExpressionItem);
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		fromItems.push(new ArrayList<FromItem>());
		super.visit(plainSelect);
	}
	
	@Override
	public void visit(Table tableName) {
		fromItems.peek().add(tableName);
	}

	@Override
	public void visit(SubJoin subjoin) {
		fromItems.peek().add(subjoin);
	}
	
	@Override
	public void visit(SubSelect subselect) {
		fromItems.peek().add(subselect);
	}
	
	@Override
	public void visit(Update update) {
		for(Table table : update.getTables()) {
			ECLAllColumnCollector collector = new ECLAllColumnCollector(layouts);
			selectItems.addAll(collector.collect(table));
		}
	}
	
	@Override
	public void visit(Delete delete) {
		ECLAllColumnCollector collector = new ECLAllColumnCollector(layouts);
		selectItems.addAll(collector.collect(delete.getTable()));
	}
	
	@Override
	public void visit(Insert insert) {
		Table table = insert.getTable();
		ECLAllColumnCollector collector = new ECLAllColumnCollector(layouts);
		selectItems.addAll(collector.collect(table));
	}
	
}
