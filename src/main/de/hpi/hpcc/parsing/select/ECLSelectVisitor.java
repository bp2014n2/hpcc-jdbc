package de.hpi.hpcc.parsing.select;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

public class ECLSelectVisitor implements SelectVisitor {

	
	SQLParserSelect parser;
	ECLBuilderSelect builder;
	private ECLLayouts layouts;
	
	
	public ECLSelectVisitor(ECLLayouts layouts) {
		this.layouts = layouts;
	}
	
	public SQLParserSelect findParser(SelectBody selectBody) {
		selectBody.accept(this);
		return parser;
	}
	
	public ECLBuilderSelect findBuilder(SelectBody selectBody) {
		selectBody.accept(this);
		return builder;
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		parser = new SQLParserPlainSelect(plainSelect, layouts);
		builder = new ECLBuilderPlainSelect(plainSelect, layouts);
	}

	@Override
	public void visit(SetOperationList setOpList) {
		parser = new SQLParserSetOperationList(setOpList, layouts);
		builder = new ECLBuilderSetOperationList(setOpList, layouts);
	}

	
	@Override
	public void visit(WithItem withItem) {
		//parser = new SQLParserWithItem(withItem, layouts);
		parser = null;
	}

}
