package connectionManagement;

import java.util.regex.Pattern;

public class HPCCUrlParser {
	public String getUrlWithOutProtocol(String url) {
		String source = this.removeProtocol(url);
		String sourceWithoutRoot = source.replaceFirst("//", "");
		int portIndex = getPortIndex(source);
		if(portIndex > 0){
			return source.substring(0, portIndex);
		}
		if(sourceWithoutRoot.indexOf("/") < 0){
			return source;
		}
		return "//"+sourceWithoutRoot.substring(0, sourceWithoutRoot.indexOf("/"));
	}

	public String getPort(String url) {
		String source = this.removeProtocol(url);
		int portIndex = getPortIndex(source);
		if(portIndex < 0 ){
			return null;
		}
		String urlAfterPortColon = source.substring(portIndex+1, source.length());
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
    	Pattern urlNameRegex = Pattern.compile("//[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");
    	return urlNameRegex.matcher(url).matches();
	}
	
	private boolean hasValidProtocol(String url) {
		return (!url.startsWith(":") && url.contains("://"));
	}
	private String removeProtocol(String url) {
		return url.substring(url.indexOf("://")+1, url.length());
	}
	private int getPortIndex(String source){
		return source.indexOf(":");
	}
}
