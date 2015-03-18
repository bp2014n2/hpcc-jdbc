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
import java.util.regex.Pattern;

public class HPCCDriver implements Driver{
    
    private HPCCDriverProperties driverProperties;
    private HPCCUrlParser urlParser;
    private static final int URI 		= 0;
    private static final int PORT 		= 1;
    
    static{
    	try{
    		HPCCDriver driver = new HPCCDriver();
            DriverManager.registerDriver(driver);
            traceOutLine("Driver registered");
        }
        catch (SQLException sqlException){
        	sqlException.printStackTrace();
        }
    }
    
    public HPCCDriver(){
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
    		traceOutLine(url +" has the wrong format (e.g. missing protocol)");
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
	    						traceOutLine(propertyValue +" is not a valid URL for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					propertyValue = this.parseURL(propertyValue)[URI];
	    					break;
	    				case "PageSize":
	    				case "PageOffset":
	    				case "ConnectTimeoutMilli":
	    				case "ReadTimeoutMilli":
	    					if(!HPCCJDBCUtils.isNumeric(propertyValue)){
	    						traceOutLine(propertyValue +" is not a number and not valid for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					break;
	    				case "EclResultLimit":
	    					if(!HPCCJDBCUtils.isNumeric(propertyValue) && !propertyValue.equals("ALL")){
	    						traceOutLine(propertyValue +" is not a valid value for "+propertyKey, defaultValue);
	    						continue;
	    					}
	    					break;
	    				case "user":
	    					propertyKey += "name";
	    					break;
	    				case "TraceLevel":
	    					HPCCJDBCUtils.initTracing(propertyValue, Boolean.parseBoolean(properties.getProperty("TraceToFile")));
	       			}
	    			driverProperties.setProperty((String) propertyKey, propertyValue);
	    		}
	    	}
    	}
		driverProperties.setProperty("Basic Auth", HPCCConnection.createBasicAuth(driverProperties.getProperty("username"), driverProperties.getProperty("password")));

		traceOutLine("Connecting to " + driverProperties.getProperty("Protocol") + driverProperties.getProperty("ServerAddress"));
        return new HPCCConnection(driverProperties);
    }

	public boolean acceptsURL(String url) {
		Pattern ipAddressRegex = Pattern.compile(HPCCDriverInformation.getDriverProtocol()+"//\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d{2,4})?(/)?");
    	Pattern urlNameRegex = Pattern.compile(HPCCDriverInformation.getDriverProtocol()+"//[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");
    	return ((url != null) && ((ipAddressRegex.matcher(url).matches()) || (urlNameRegex.matcher(url).matches())));
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
		parsedURL[URI] 	= urlParser.getUri(url);
		parsedURL[PORT] = urlParser.getPort(url);
		return parsedURL;
	}
	
	private static void traceOutLine(String infoMessage){
		HPCCJDBCUtils.traceoutln(Level.INFO, HPCCDriver.class.getSimpleName()+": "+infoMessage);
	}
	
	private static void traceOutLine(String infoMessage, String defaultProperty){
		traceOutLine(infoMessage+" (now using: "+defaultProperty+")");
	}
}