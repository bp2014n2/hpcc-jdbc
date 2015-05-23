package de.hpi.hpcc.parsing.delete;

import de.hpi.hpcc.parsing.ECLBuilder;
import de.hpi.hpcc.parsing.ECLLayouts;
import net.sf.jsqlparser.statement.delete.Delete;

public class ECLBuilderDelete extends ECLBuilder {

	private Delete delete;
	private SQLParserDelete sqlParser;

	public ECLBuilderDelete(Delete delete, ECLLayouts eclLayouts) {
		super(delete, eclLayouts);
		this.delete = delete;
	}

	@Override
	public String generateECL() {
		sqlParser = new SQLParserDelete(delete, eclLayouts);
		eclCode = new StringBuilder();
		eclCode.append("OUTPUT(DATASET([],{");
		String recordString = eclLayouts.getRecord(sqlParser.getName());
		eclCode.append(recordString);
		eclCode.append("}),,'~%NEWTABLE%',OVERWRITE);");
		
		outputCount++;
		return eclCode.toString();
	}

	@Override
	protected Delete getStatement() {
		return delete;
	}
}
