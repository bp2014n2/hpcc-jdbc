package de.hpi.hpcc.main;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HPCCUrlParser {
	private static final Pattern GENERAL_URL_PATTERN = Pattern.compile("(//[\\w-.]*)(:[0-9]{1,5})?(/[\\w]*)?");
	private static final Pattern PORT_URL_PATTERN	 = Pattern.compile("(//[\\w-.]*)(:[0-9]{1,5})(/[\\w]*)?");
	private static final Pattern CLUSTER_URL_PATTERN = Pattern.compile("(//[\\w-.]*)(:[0-9]{1,5})?(/[\\w]*)");
	
	public String getFileLocation(String url) {
		return this.getProperty(url, 1, GENERAL_URL_PATTERN);
	}

	public String getPort(String url) {
		return formatProperty(this.getProperty(url, 2, PORT_URL_PATTERN));
	}
	
	public String getCluster(String url) {
		return formatProperty(this.getProperty(url, 3, CLUSTER_URL_PATTERN));
	}
	
	public boolean isValidUrl(String url){
		if((url == null) || !this.hasValidProtocol(url)){
			return false;
		}
		url = this.removeProtocol(url);
    	return HPCCUrlParser.GENERAL_URL_PATTERN.matcher(url).matches();
	}
	
	private String getProperty(String url, int propertyNumber, Pattern pattern) {
		url = this.removeProtocol(url);
		Matcher matchingUrl = pattern.matcher(url);
		if(matchingUrl.matches() && !matchingUrl.group(propertyNumber).equals("")) {
			return matchingUrl.group(propertyNumber); 
		}
		return null;
	}
	
	private String formatProperty(String property) {
		if(property == null) {
			return null;
		}
		return property.substring(1);
	}
	
	private boolean hasValidProtocol(String url) {
		return (!url.startsWith(":") && url.contains("://"));
	}
	
	private String removeProtocol(String url) {
		return url.substring(url.indexOf("://")+1, url.length());
	}
}
