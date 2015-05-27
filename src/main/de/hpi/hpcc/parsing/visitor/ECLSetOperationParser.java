package de.hpi.hpcc.parsing.visitor;

import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.UnionOp;

public class ECLSetOperationParser {
	
	//TODO: add missing operands
	public String parse(SetOperation op) {
		if (op instanceof UnionOp) {
			return " + ";
		}
		return "";
	}

}
