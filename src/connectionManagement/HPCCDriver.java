package connectionManagement;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HPCCDriver implements Driver{
    
    private HPCCDriverProperties driverProperties;
    private HPCCUrlParser urlParser;
    private static Logger logger = HPCCLogger.getLogger();
    private static final int URI 		= 0;
    private static final int PORT 		= 1;
    
    static{
    	try{
    		HPCCDriver driver = new HPCCDriver();
            DriverManager.registerDriver(driver);
            log("Driver built and registered");
        }
        catch (SQLException sqlException){
        	sqlException.printStackTrace();
        }
    }
    
    public HPCCDriver(){
    	HPCCLogger.initializeLogging();
    	this.driverProperties = new HPCCDriverProperties();
    	this.urlParser = new HPCCUrlParser();
    }
    
    public Connection connect(String url, Properties properties){
    	String[] parsedURL;
    	if(acceptsURL(url)){
    		parsedURL = this.parseURL(url);
    		for(String uriProperty : driverProperties.getAllPropertiesUsingServerAddress()){
    			driverProperties.setProperty(uriProperty, parsedURL[URI]);
    		}
    		if(parsedURL[PORT] != null){
    			for(String portProperty : driverProperties.getAllPropertiesUsingDefaultPort()){
    				driverProperties.setProperty(portProperty, parsedURL[PORT]);
    			}
    		}
    	}else{
    		log(url +" has the wrong format (e.g. missing protocol)");
    		return null;
    	}
        
    	if(properties != null){
	    	for(Object propertyKey : properties.keySet()){
	    		if((driverProperties.keySet().contains(propertyKey) && !propertyKey.equals("ServerAddress")) || (propertyKey.equals("user"))){
	    			String propertyValue = properties.getProperty((String) propertyKey);
	    			String defaultValue = driverProperties.getProperty((String) propertyKey);
	    			switch((String)propertyKey){
	    				case "WsECLWatchAddress":
	    				case "WsECLAddress":
	    				case "WsECLDirectAddress":
	    					if(!acceptsURL(propertyValue)){
	    						log(propertyValue +" is not a valid URL for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					propertyValue = this.parseURL(propertyValue)[URI];
	    					break;
	    				case "PageSize":
	    				case "PageOffset":
	    				case "ConnectTimeoutMilli":
	    				case "ReadTimeoutMilli":
	    					if(!HPCCJDBCUtils.isNumeric(propertyValue)){
	    						log(propertyValue +" is not a number and not valid for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					break;
	    				case "EclResultLimit":
	    					if(!HPCCJDBCUtils.isNumeric(propertyValue) && !propertyValue.equals("ALL")){
	    						log(propertyValue +" is not a valid value for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					break;
	    				case "user":
	    					propertyKey += "name";
	    					break;
	    				case "TraceLevel":
	    					//TODO: isLevelTest
	       			}
	    			driverProperties.setProperty((String) propertyKey, propertyValue);
	    		}
	    	}
    	}
		driverProperties.setProperty("Basic Auth", HPCCConnection.createBasicAuth(driverProperties.getProperty("username"), driverProperties.getProperty("password")));

		log(Level.CONFIG, "Connecting to " + driverProperties.getProperty("Protocol") + driverProperties.getProperty("ServerAddress")+"/");
        return new HPCCConnection(driverProperties);
    }

	public boolean acceptsURL(String url) {
		return (url.startsWith(HPCCDriverInformation.getDriverProtocol()) && urlParser.isValidUrl(url));
	}
	
	public void resetProperties(){
		driverProperties.reset();
	}
	
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
        return driverProperties.getProperties();
    }

    public int getMajorVersion(){
        return HPCCDriverInformation.getMajorVersion();
    }

    public int getMinorVersion(){
        return HPCCDriverInformation.getMinorVersion();
    }

    public boolean jdbcCompliant(){
        return true;
    }

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
	
	private String[] parseURL(String url){
		String[] parsedURL = new String[2];
		parsedURL[URI] 	= urlParser.getFileLocation(url);
		parsedURL[PORT] = urlParser.getPort(url);
		return parsedURL;
	}
	
	private static void log(String infoMessage){
		logger.log(HPCCLogger.getDefaultLogLevel(), HPCCDriver.class.getSimpleName()+": "+infoMessage);
	}
	
	private static void log(Level level, String infoMessage){
		logger.log(level, HPCCDriver.class.getSimpleName()+": "+infoMessage);
	}
	
	private static void log(String infoMessage, String defaultProperty){
		log(Level.WARNING, infoMessage+" (now using: "+defaultProperty+")");
	}
}