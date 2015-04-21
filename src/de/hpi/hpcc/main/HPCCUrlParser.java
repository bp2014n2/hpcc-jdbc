package de.hpi.hpcc.main;

import java.util.regex.Pattern;

public class HPCCUrlParser {
	public String getFileLocation(String url) {
		String fileLocation = this.removeProtocol(url);
		String fileLocationWithoutRoot = fileLocation.replaceFirst("//", "");
		int portIndex = getPortIndex(fileLocation);
		if(portIndex > 0){
			return fileLocation.substring(0, portIndex);
		}
		if(fileLocationWithoutRoot.indexOf("/") < 0){
			return fileLocation;
		}
		return "//"+fileLocationWithoutRoot.substring(0, fileLocationWithoutRoot.indexOf("/"));
	}

	public String getPort(String url) {
		String fileLocation = this.removeProtocol(url);
		int portIndex = getPortIndex(fileLocation);
		if(portIndex < 0 ){
			return null;
		}
		String urlAfterPortColon = fileLocation.substring(portIndex+1, fileLocation.length());
		if(urlAfterPortColon.equals("")){
			return null;
		}
		if(urlAfterPortColon.indexOf("/") < 1){
			if(HPCCJDBCUtils.isNumeric(urlAfterPortColon)){
				return urlAfterPortColon;
			}
			return null;
		}
		if(HPCCJDBCUtils.isNumeric(urlAfterPortColon.substring(0, urlAfterPortColon.indexOf("/")))){
			return urlAfterPortColon.substring(0, urlAfterPortColon.indexOf("/"));
		}
		return null;		
	}
	
	public boolean isValidUrl(String url){
		if((url == null) || !this.hasValidProtocol(url)){
			return false;
		}
		url = this.removeProtocol(url);
    	Pattern urlNameRegex = Pattern.compile("//[^\\s/$.?#].[^\\s]*(/\\w)?(\\?\\w)?");
    	return urlNameRegex.matcher(url).matches();
	}
	
	private boolean hasValidProtocol(String url) {
		return (!url.startsWith(":") && url.contains("://"));
	}
	private String removeProtocol(String url) {
		return url.substring(url.indexOf("://")+1, url.length());
	}
	private int getPortIndex(String fileLocation){
		return fileLocation.indexOf(":");
	}
}
