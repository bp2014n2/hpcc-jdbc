package de.hpi.hpcc.logging;

import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.LogRecord;

public class HPCCConsoleHandler extends ConsoleHandler
{
    public HPCCConsoleHandler(HPCCLogFormatter formatter) {
		this.setFormatter(formatter);
	}
	
	@Override
    public void publish(LogRecord record) {
		String message = getFormatter().format(record);
		try {
			System.out.write(message.getBytes());
		} catch (IOException e) {
			System.out.println("Trouble with logger:\n"+e.getMessage());
		}
    }
}
