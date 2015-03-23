package connectionManagement;

import java.util.logging.ConsoleHandler;

public class HPCCJDBCStdOutConsoleHandler extends ConsoleHandler
{
    public HPCCJDBCStdOutConsoleHandler(HPCCLogFormatter formatter) {
		this.setFormatter(formatter);
		this.setOutputStream();
	}

	protected void setOutputStream() throws SecurityException{
        super.setOutputStream(System.out);
    }
}
