package de.hpi.hpcc.main;

import java.sql.DriverPropertyInfo;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import de.hpi.hpcc.logging.HPCCLogger;

public class HPCCDriverProperties extends Properties{
	private static final long serialVersionUID = 1L;
	
	private static final int DEFAULT_VALUE = 0;
	private static final int DESCRIPTION = 1;
	
	private static final String DEFAULT_SERVER_ADDRESS = "//localhost";
	private static final String DEFAULT_PORT = "8010";
	
	private static HashMap<String, String[]> defaultProperties = new HashMap<String, String[]>(){
		private static final long serialVersionUID = 1L;
		{
			put("ConnectTimeoutMilli", 	new String[]{"5000","HPCC requests connection time out value in milliseconds."});
			put("ReadTimeoutMilli", 	new String[]{"15000","HPCC requests connection read time out value in milliseconds."});
			put("LazyLoad", 			new String[]{"true","If disabled, all HPCC metadata loaded and cached at connect time; otherwise HPCC file, and published query info is loaded on-demand."});
			put("EclResultLimit", 		new String[]{"100","Default limit on number of result records returned."});
			put("TraceLevel", 			new String[]{HPCCLogger.getDefaultLogLevel().getName(),	"Logging level (java.util.logging.level)."});
			put("TraceToFile", 			new String[]{"false","false -> System.out, true -> " + HPCCJDBCUtils.workingDir +  HPCCJDBCUtils.traceFileName});
			put("TargetCluster", 		new String[]{"thor","Target cluster on which to execute ECL code."});
			put("QuerySet", 			new String[]{"hthor","Queryset from which published query (Stored Procedure) is chosen."});
			put("PageSize", 			new String[]{"100","Number of HPCC data files (DB tables) or HPCC published queries (DB Stored Procs) displayed."});
			put("PageOffset", 			new String[]{"0","Starting HPCC data file or HPCC published queries displayed."});
			put("password", 			new String[]{"","HPCC password (*Use JDBC client secure interface if available*)."});
			put("username", 			new String[]{"","HPCC username (*Use JDBC client secure interface if available*)."});
			put("Protocol", 			new String[]{"http:","Protocol used to establish the connection to the HPCC Server."});
			put("WsECLDirectPort", 		new String[]{DEFAULT_PORT,"WsECLDirect port (required if HPCC configuration does not use default port)."});
			put("WsECLPort", 			new String[]{"8002","WsECL port (required if HPCC configuration does not use default port)."});
			put("WsECLWatchPort", 		new String[]{DEFAULT_PORT,"WsECLWatch port (required if HPCC configuration does not use default port)."});
			put("ServerAddress", 		new String[]{DEFAULT_SERVER_ADDRESS, "Target HPCC ESP Address (used to contact WsECLWatch, WsECLDirect, or WsECL if override not specified)."});
			put("WsECLWatchAddress", 	new String[]{DEFAULT_SERVER_ADDRESS, "WsECLWatch address (required if different than ServerAddress)."});
			put("WsECLAddress", 		new String[]{DEFAULT_SERVER_ADDRESS, "WsECLAddress Address (required if different than ServerAddress)."});
			put("WsECLDirectAddress", 	new String[]{DEFAULT_SERVER_ADDRESS, "WsECLDirect Address (required if different than ServerAddress)."});
		}
	};
	
	public HPCCDriverProperties(){
		super();
		this.initializeProperties();
	}

	public void reset() {
		this.initializeProperties();
	}
	
	public void initializeProperties() {
		for(String key : defaultProperties.keySet()){
			this.setProperty(key, defaultProperties.get(key)[DEFAULT_VALUE]);
		}
	}
	
	public Object setProperty(String key, String value){
		boolean isInDefaultProperies = defaultProperties.containsKey(key);
		if(isInDefaultProperies){
			super.setProperty(key, value);
		}
		return isInDefaultProperies;
	}
	
	public String getProperty(String key){
		return (String) super.getProperty(key);
	} 
	
	public LinkedList<String> getAllPropertiesUsingServerAddress(){
		return this.getAllPropertiesUsingAttribute(DEFAULT_SERVER_ADDRESS);
	}
	
	public LinkedList<String> getAllPropertiesUsingDefaultPort() {
		return this.getAllPropertiesUsingAttribute(DEFAULT_PORT);
	}
	
	private LinkedList<String> getAllPropertiesUsingAttribute(String attribute){
		LinkedList<String> propertiesWithAttribute = new LinkedList<String>();
		for(String key : defaultProperties.keySet()){
			if(defaultProperties.get(key)[DEFAULT_VALUE].equals(attribute)){
				propertiesWithAttribute.add(key);
			}
		}
		return propertiesWithAttribute;
	}
	
	public DriverPropertyInfo[] getProperties(){
		return this.createPropertyInfo();
	}
	
	private DriverPropertyInfo[] createPropertyInfo(){
		
		DriverPropertyInfo[] propertyInformation = new DriverPropertyInfo[this.size()];
		int propertyCount = 0;
		
		for(String propertyName : defaultProperties.keySet()){
			String propertyValue = this.getProperty(propertyName);
			String defaultValue = defaultProperties.get(propertyName)[DEFAULT_VALUE];
			String propertyDescription = defaultProperties.get(propertyName)[DESCRIPTION]+"(default: "+defaultValue+")";
			
			propertyInformation[propertyCount] = new DriverPropertyInfo(propertyName, propertyValue);
			propertyInformation[propertyCount].description = propertyDescription;
			propertyInformation[propertyCount].required = propertyName.equals("ServerAddress");
			
			if(propertyName.equals("LazyLoad")){
				propertyInformation[propertyCount].choices = new String [] {"true", "false"};
			}
			
			propertyCount++;
		}
		
		return propertyInformation;
	}
}