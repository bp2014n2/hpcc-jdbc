package de.hpi.hpcc.main;

public class HPCCDriverInformation {
	private static final int MAJOR_VERSION = 0;
	private static final int MINOR_VERSION = 2;
	private static final int POINT_RELEASE = 6;
	private static final String DRIVER_PROTOCOL = "jdbc:hpcc:";
	private static final String MATURITY   = "Beta";
	
	public static int getMajorVersion(){
		return MAJOR_VERSION;
	}
	
	public static int getMinorVersion(){
		return MINOR_VERSION;
	}
	
	public static int getPointRelease() {
		return POINT_RELEASE;
	}
	
	public static String getDriverProtocol() {
		return DRIVER_PROTOCOL;
	}

	public static String getMaturity() {
		return MATURITY;
	}
}
