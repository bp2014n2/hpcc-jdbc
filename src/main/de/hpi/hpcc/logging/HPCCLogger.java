package de.hpi.hpcc.logging;

import java.util.LinkedList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HPCCLogger extends Logger{
	
	private static final LinkedList<String> levels = new LinkedList<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(Level.ALL.getName());
			add(Level.SEVERE.getName());
			add(Level.WARNING.getName());
			add(Level.INFO.getName());
			add(Level.FINEST.getName());
			add(Level.OFF.getName());
	}};
	
	protected HPCCLogger(String name, String resourceBundleName) {
		super(name, resourceBundleName);
	}
	
	public static void initializeLogging(){
		Logger logger = getLogger();
		HPCCLogFormatter formatter = new HPCCLogFormatter();
		logger.setUseParentHandlers(true);
		try {
//			HPCCConsoleHandler handler = new HPCCConsoleHandler(formatter);
//			logger.addHandler(handler);
		} catch (Exception exception){}
		setLoggingLevel(getDefaultLogLevel());
	}
	
	public static void setLoggingLevel(Level level){
		level = Level.ALL; //TODO: remove (just for testing purposes)
		Logger logger = getLogger();
		for(Handler handler : logger.getHandlers()){
			handler.setLevel(level);
		}
		logger.setLevel(level);
	}
	
	public static Logger getLogger(){
		return Logger.getLogger("jdbc-hpcc-driver-logger");
	}
	
	public static LinkedList<String> getTraceLevels(){
		return levels;
	}
	
	public static Level getDefaultLogLevel(){
		return Level.INFO;
	}
}