package de.hpi.hpcc.main;

public class HPCCDriverInformation {
	static final int MAJOR_VERSION = 1;
	static final int MINOR_VERSION = 0;
	static final String DRIVER_PROTOCOL = "jdbc:hpcc:";
	
	public static int getMajorVersion(){
		return MAJOR_VERSION;
	}
	
	public static int getMinorVersion(){
		return MINOR_VERSION;
	}
	
	public static String getDriverProtocol() {
		return DRIVER_PROTOCOL;
	}
}
