/*##############################################################################

Copyright (C) 2011 HPCC Systems.

All rights reserved. This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

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
    }
    
    public Connection connect (){
    	return new HPCCConnection(driverProperties);
    }
    
    public Connection connect(String url){
		return this.connect(url, null);
    }
    
    public Connection connect(String url, Properties properties){
    	if(acceptsURL(url)){
    		for(String property : driverProperties.getAllPropertiesUsingServerAddress()){
    			driverProperties.setProperty(property, url);
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

		traceOutLine("Connecting to " + driverProperties.getProperty("ServerAddress"));
        return new HPCCConnection(driverProperties);
    }
	
	public boolean acceptsURL(String url) {
		Pattern ipAddressRegex = Pattern.compile("\\bjdbc:hpcc://\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b");
    	Pattern urlNameRegex = Pattern.compile("\\bjdbc:hpcc://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]\\b");
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
	
	private static void traceOutLine(String errorMessage){
		HPCCJDBCUtils.traceoutln(Level.INFO, HPCCDriver.class.getSimpleName()+": "+errorMessage);
	}
	
	private static void traceOutLine(String errorMessage, String defaultProperty){
		traceOutLine(errorMessage+" (now using: "+defaultProperty+")");
	}
}