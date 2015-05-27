package de.hpi.hpcc.parsing.visitor;

import de.hpi.hpcc.parsing.ECLLayouts;
import de.hpi.hpcc.parsing.ECLUtils;
import de.hpi.hpcc.parsing.select.ECLSelectParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ECLFromItemGenerator implements FromItemVisitor {

	private String generated;
	private ECLLayouts layouts;
	

	public ECLFromItemGenerator(ECLLayouts layouts) {
		this.layouts = layouts;
	}

	public String generate(FromItem fromItem) {
		fromItem.accept(this);
		return this.generated;
	}
	
	@Override
	public void visit(Table tableName) {
		generated = tableName.getName();
	}

	@Override
	public void visit(SubSelect subSelect) {
		ECLSelectParser selectParser = new ECLSelectParser(layouts);
		generated = ECLUtils.encapsulateWithBrackets(selectParser.parse(subSelect.getSelectBody()));
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
