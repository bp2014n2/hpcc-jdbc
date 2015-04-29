package de.hpi.hpcc.parsing.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ECLAllColumnCollector implements FromItemVisitor {

	private ECLLayouts layouts;
	private List<SelectExpressionItem> expressions = new ArrayList<SelectExpressionItem>();
	
	public ECLAllColumnCollector(ECLLayouts layouts) {
		this.layouts = layouts;
	}
	
	public List<SelectExpressionItem> collect(FromItem fromItem) {
		fromItem.accept(this);
		return expressions;
	}
	
	@Override
	public void visit(Table tableName) {
		Set<String> rawColumns = layouts.getAllColumns(tableName.getName());
		for(String rawColumn : rawColumns) {
			Column column = new Column(rawColumn);
			SelectExpressionItem sei = new SelectExpressionItem(column);
			expressions.add(sei);
		}
	}

	@Override
	public void visit(SubSelect subSelect) {
		ECLSelectItemFinder finder = new ECLSelectItemFinder(layouts);
		if(subSelect.getSelectBody() != null) {
			expressions.addAll(finder.find(subSelect.getSelectBody()));
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

}
