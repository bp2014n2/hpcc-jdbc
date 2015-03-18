package connectionManagement;

public class HPCCUrlParser {
	public String getSource(String url){
		return url.replaceFirst(HPCCDriverInformation.getDriverProtocol(), "");
	}
	public String getUri(String url) {
		String source = getSource(url);
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
		String source = getSource(url);
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
	
	private int getPortIndex(String source){
		return source.indexOf(":");
	}
}
