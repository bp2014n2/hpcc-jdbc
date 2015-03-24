package de.hpi.hpcc.logging;

import java.util.logging.ConsoleHandler;

public class HPCCConsoleHandler extends ConsoleHandler
{
    public HPCCConsoleHandler(HPCCLogFormatter formatter) {
		this.setFormatter(formatter);
		this.setOutputStream();
	}

	protected void setOutputStream() throws SecurityException{
        super.setOutputStream(System.out);
    }
}
