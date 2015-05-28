package de.hpi.hpcc.parsing;

public class ECLUtils {
	
	public static StringBuilder convertToTable(StringBuilder eclCode) {
		return new StringBuilder(convertToTable(eclCode.toString()));
	}
	
	public static StringBuilder convertToSet(StringBuilder string) {
		return new StringBuilder(convertToSet(string.toString()));
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with brackets
	 * @param eclCode
	 */
	public static StringBuilder encapsulateWithBrackets(StringBuilder eclCode) {
		return new StringBuilder(encapsulateWithBrackets(eclCode.toString()));
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with curly brackets
	 * @param eclCode
	 */
	public static StringBuilder encapsulateWithCurlyBrackets(StringBuilder eclCode) {
		return new StringBuilder(encapsulateWithCurlyBrackets(eclCode.toString()));
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with square brackets
	 * @param eclCode
	 */
	public static StringBuilder encapsulateWithSquareBrackets(StringBuilder eclCode) {
   		return new StringBuilder(encapsulateWithSquareBrackets(eclCode.toString()));
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with brackets
	 * @param input
	 */
	public static String encapsulateWithBrackets(String input) {
		return encapsulate(input, "(", ")");
	}
	
	public static String encapsulateWithSingleQuote(String input) {
		return encapsulate(input, "'", "'");
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with curly brackets
	 * @param eclCode
	 */
	public static String encapsulateWithCurlyBrackets(String eclCode) {
		return encapsulate(eclCode, "{", "}");
	}
	
	/**
	 * Modifies input StringBuilder and surrounds it with square brackets
	 * @param eclCode
	 */
	public static String encapsulateWithSquareBrackets(String eclCode) {
		return encapsulate(eclCode, "[", "]");
	}
	
	public static String convertToTable(String eclCode) {
		return prepend(encapsulateWithBrackets(eclCode), "TABLE");
	}
	
	public static String convertToIndex(String eclCode) {
		return prepend(encapsulateWithBrackets(eclCode), "INDEX");
	}
	
	public static String encapsulate(String toEncapsulate, String prefix, String suffix) {
		return append(prepend(toEncapsulate, prefix), suffix);
	}
	
	public static String append(String foo, String suffix) {
		return foo+suffix;
	}
	
	public static String prepend(String foo, String prefix) {
		return prefix+foo;
	}
	
	public static String join(Iterable<?> list, String seperator) {
		StringBuilder stringList = new StringBuilder();
		for (Object item : list) {
    		if (stringList.length()!=0) {
    			stringList.append(seperator);
    		}
    		stringList.append(item.toString());
    	}
		return stringList.toString();
	}
	
	public static StringBuilder convertToSort(StringBuilder string) {
		return new StringBuilder(convertToSort(string.toString()));
	}

	public static String convertToSort(String eclCode) {
		return prepend(encapsulateWithBrackets(eclCode), "SORT");
	}

	public static String convertToSet(String string) {
		return prepend(encapsulateWithBrackets(string), "SET");
	}

	public static String convertToBuild(String string) {
		return prepend(encapsulateWithBrackets(string), "BUILD");
	}

	public static String convertToRecord(String string) {
		return "RECORD " + string + "; END;";
	}

	public static String convertToNot(String string) {
		return prepend(encapsulateWithBrackets(string), "NOT");
	}
	
}
