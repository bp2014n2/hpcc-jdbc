package de.hpi.hpcc.main;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.hpi.hpcc.logging.HPCCLogger;

public class HPCCJDBCUtils
{
    public static final String DOTSEPERATORREGEX = "\\.";

    public static String newLine = System.getProperty("line.separator");
    public static String fileSep = System.getProperty("file.separator");;
    public static final String HPCCCATALOGNAME = "HPCC System";

    public final static String traceFileName = "HPCCJDBC.log";
    public final static String workingDir = System.getProperty("user.dir") + fileSep;

    private final static Logger logger = HPCCLogger.getLogger();

    public static void traceoutln(Level level, String message)
    {
        if (logger != null)
        {
            logger.log(level, message);
        }
    }

    public static final ThreadLocal <NumberFormat> NUMFORMATTER =
            new ThreadLocal <NumberFormat>()
            {
                @Override
                protected NumberFormat initialValue()
                {
                    return NumberFormat.getInstance(Locale.US);
                }
            };

    static final char          pad          = '=';
    static final char          BASE64_enc[] =
                                            { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
            'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
            'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
            '4', '5', '6', '7', '8', '9', '+', '"' };

    static final char          BASE64_dec[] =
                                            { (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x3e, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x3f, (char) 0x34, (char) 0x35, (char) 0x36, (char) 0x37, (char) 0x38,
            (char) 0x39, (char) 0x3a, (char) 0x3b, (char) 0x3c, (char) 0x3d, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x01, (char) 0x02, (char) 0x03,
            (char) 0x04, (char) 0x05, (char) 0x06, (char) 0x07, (char) 0x08, (char) 0x09, (char) 0x0a, (char) 0x0b,
            (char) 0x0c, (char) 0x0d, (char) 0x0e, (char) 0x0f, (char) 0x10, (char) 0x11, (char) 0x12, (char) 0x13,
            (char) 0x14, (char) 0x15, (char) 0x16, (char) 0x17, (char) 0x18, (char) 0x19, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x1a, (char) 0x1b, (char) 0x1c, (char) 0x1d,
            (char) 0x1e, (char) 0x1f, (char) 0x20, (char) 0x21, (char) 0x22, (char) 0x23, (char) 0x24, (char) 0x25,
            (char) 0x26, (char) 0x27, (char) 0x28, (char) 0x29, (char) 0x2a, (char) 0x2b, (char) 0x2c, (char) 0x2d,
            (char) 0x2e, (char) 0x2f, (char) 0x30, (char) 0x31, (char) 0x32, (char) 0x33, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00, (char) 0x00,
            (char) 0x00, (char) 0x00, (char) 0x00 };

    private final static Pattern TESTCASEPATTERN = Pattern.compile("\\s*(\\[.*\\])?(.*)\\s*",Pattern.DOTALL);
    public static List<String> returnTestCaseParams(String testcase)
    {
        List<String> containsGroups = new ArrayList<String>();
        Matcher matcher = TESTCASEPATTERN.matcher(testcase);
        if (matcher.matches())
        {
            for (int i = 1; i <= matcher.groupCount(); i++)
            {
                containsGroups.add(matcher.group(i));
            }
        }
        return containsGroups;
    }

    static Pattern CALLSTATEMENTSPATTERN = Pattern.compile("(\\$\\{)|(\\?)",Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    public static int parseCallParameters(String testcase)
    {
        Matcher matcher = CALLSTATEMENTSPATTERN.matcher(testcase);
        int countOccurences = 0;
        while (matcher.find())
        {
            countOccurences++;
        }
        return countOccurences;
    }

    public static String Base64Encode(byte[] input, boolean addLineBreaks)
    {
        int length = input.length;
        StringBuilder out = new StringBuilder("");
        char one;
        char two;
        char three;

        int i;
        for (i = 0; i < length && length - i >= 3;)
        {
            one = (char) input[i++];
            two = (char) input[i++];
            three = (char) input[i++];

            out.append((char) BASE64_enc[one >> 2]);
            out.append((char) BASE64_enc[((one << 4) & 0x30 | (two >> 4))]);
            out.append((char) BASE64_enc[((two << 2) & 0x3c | (three >> 6))]);
            out.append((char) BASE64_enc[three & 0x3f]);

            if (addLineBreaks && (i % 54 == 0))
                out.append("\n");

            switch (length - i)
            {
                case 2:
                    one = (char) input[i++];
                    two = (char) input[i++];

                    out.append((char) BASE64_enc[one >> 2]);
                    out.append((char) BASE64_enc[((one << 4) & 0x30 | (two >> 4))]);
                    out.append((char) BASE64_enc[((two << 2) & 0x3c)]);
                    out.append(pad);
                    break;

                case 1:
                    one = (char) input[i++];

                    out.append((char) BASE64_enc[one >> 2]);
                    out.append((char) BASE64_enc[((one << 4) & 0x30)]);
                    out.append(pad);
                    out.append(pad);
                    break;
            }

        }
        return out.toString();
    }

    public static String removeAllNewLines(String str)
    {
        return str.trim().replaceAll("\\r\\n|\\r|\\n", " ");
    }

//    private final static Pattern QUOTEDFULLFIELDPATTERN = Pattern.compile(
//            "\\s*(\"|\')(.*?){1}(\\.)(.*?){1}(\"|\')\\s*",Pattern.DOTALL);
    


    public static boolean isNumeric(String str)
    {
        try
        {
            NUMFORMATTER.get().parse(str);
        }
        catch (Exception e)
        {
            return false;
        }

        return true;
    }

    private final static Pattern PARENSTRPATTERN = Pattern.compile(
            "\\s*(\\()(.*?)(\\))\\s*",Pattern.DOTALL);

    public static boolean isInParenthesis(String parenstring)
    {
        if (parenstring == null)
            return false;

        Matcher matcher = PARENSTRPATTERN.matcher(parenstring);

         return matcher.matches();
    }

    public final static Pattern FUNCPATTERN = Pattern.compile(
            "\\s*(.*?)(\\()(.*?)(\\))\\s*",Pattern.DOTALL);

    public static boolean isFunction(String aggfunstr)
    {
        if (aggfunstr == null)
            return false;

        Matcher matcher = FUNCPATTERN.matcher(aggfunstr);

         return matcher.matches();
    }

    public static String getParenContents(String parenstring)
    {
        if (parenstring == null)
        return "";

        Matcher matcher = PARENSTRPATTERN.matcher(parenstring);

        if(matcher.matches())
            return matcher.group(2).trim();
        else
            return parenstring;
    }

    public static long stringToLong(String str, long uponError)
    {
        try
        {
            Number num = NUMFORMATTER.get().parse(str);
            return num.longValue();
        }
        catch (Exception e)
        {
            return uponError;
        }
    }

    public static int stringToInt(String str, int uponError)
    {
        try
        {
            Number num = NUMFORMATTER.get().parse(str);
            return num.intValue();
        }
        catch (Exception e)
        {
            return uponError;
        }
    }

    public static String replaceAll(String input, String forReplace, String replaceWith)
    {
        if (input == null)
            return "null";

        StringBuffer result = new StringBuffer();
        boolean hasMore = true;
        while (hasMore)
        {
            int start = input.indexOf(forReplace);
            int end = start + forReplace.length();
            if (start != -1)
            {
                result.append(input.substring(0, start) + replaceWith);
                input = input.substring(end);
            }
            else
            {
                hasMore = false;
                result.append(input);
            }
        }
        if (result.toString().equals(""))
            return input; // nothing is changed
        else
            return result.toString();
    }





    public static boolean isParameterizedStr(String value)
    {
        return  (value.contains("${") || value.equals("?"));
    }

    private static Map<Integer, String> SQLFieldMapping = new HashMap<Integer, String>();;

    static
    {
        Field[] fields = java.sql.Types.class.getFields();

        for (int i = 0; i < fields.length; i++)
        {
            try
            {
                String name = fields[i].getName();
                Integer value = (Integer) fields[i].get(null);
                SQLFieldMapping.put(value, name);
            }
            catch (IllegalAccessException e) {}
        }
    }

    public static String getSQLTypeName(Integer sqltypecode) throws Exception
    {
        if (SQLFieldMapping.size() <= 0)
            throw new Exception("java.sql.Types.class.getFields were not feched, cannot get SQL Type name");

        return SQLFieldMapping.get(sqltypecode);
    }

    public final static HashMap<String, Integer> mapECLTypeNameToSQLType = new HashMap<String, Integer>();
    static
    {
        mapECLTypeNameToSQLType.put("BOOLEAN", java.sql.Types.BOOLEAN);
        mapECLTypeNameToSQLType.put("STRING", java.sql.Types.VARCHAR);
        mapECLTypeNameToSQLType.put("QSTRING", java.sql.Types.VARCHAR);
        mapECLTypeNameToSQLType.put("FLOAT", java.sql.Types.FLOAT);
        mapECLTypeNameToSQLType.put("DOUBLE", java.sql.Types.DOUBLE);
        mapECLTypeNameToSQLType.put("DECIMAL", java.sql.Types.DECIMAL);
        mapECLTypeNameToSQLType.put("INTEGER", java.sql.Types.INTEGER);
        mapECLTypeNameToSQLType.put("LONG", java.sql.Types.NUMERIC);
        mapECLTypeNameToSQLType.put("INT", java.sql.Types.INTEGER);
        mapECLTypeNameToSQLType.put("SHORT", java.sql.Types.SMALLINT);
        mapECLTypeNameToSQLType.put("UNSIGNED", java.sql.Types.NUMERIC);
        mapECLTypeNameToSQLType.put("DATETIME", java.sql.Types.TIMESTAMP);
        mapECLTypeNameToSQLType.put("TIME", java.sql.Types.TIME);
        mapECLTypeNameToSQLType.put("DATE", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("GDAY", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("GMONTH", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("GYEAR", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("GYEARMONTH", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("GMONTHDAY", java.sql.Types.DATE);
        mapECLTypeNameToSQLType.put("DURATION", java.sql.Types.VARCHAR);
        mapECLTypeNameToSQLType.put("STRING1", java.sql.Types.CHAR);
        mapECLTypeNameToSQLType.put("REAL", java.sql.Types.REAL);
        mapECLTypeNameToSQLType.put("UNICODE", java.sql.Types.VARCHAR);
    }

    public final static Pattern TRAILINGNUMERICPATTERN = Pattern.compile(
            "(.*\\s+?)*([A-Z]+)(([0-9]+)(_([0-9]+))?)*",Pattern.DOTALL);

    public static int mapECLtype2SQLtype(String ecltype)
    {
        if (mapECLTypeNameToSQLType.containsKey(ecltype))
        {
            return mapECLTypeNameToSQLType.get(ecltype); //let's try to find the type as is
        }
        else
        {
            String postfixUpper = ecltype.substring(ecltype.lastIndexOf(':') + 1).toUpperCase();
            if (mapECLTypeNameToSQLType.containsKey(postfixUpper))
                return mapECLTypeNameToSQLType.get(postfixUpper);
            else
            {
                //TRAILINGNUMERICPATTERN attemps to match optional leading spaces
                //followed by a string of alphas, followed by optional string of numerics
                //then we look up the string of alphas in the known ECL type map (group(2))
                Matcher m = TRAILINGNUMERICPATTERN.matcher(postfixUpper);
                if (m.matches() && mapECLTypeNameToSQLType.containsKey(m.group(2)))
                    return mapECLTypeNameToSQLType.get(m.group(2));
                else
                    return java.sql.Types.OTHER;
            }
        }
    }

    public enum EclTypes
    {
        ECLTypeboolean(0),
        ECLTypeint(1),
        ECLTypereal(2),
        ECLTypedecimal(3),
        ECLTypestring(4),
        ECLTypeunused1(5),
        ECLTypedate(6),
        ECLTypeunused2(7),
        ECLTypeunused3(8),
        ECLTypebitfield(9),
        ECLTypeunused4(10),
        ECLTypechar(11),
        ECLTypeenumerated(12),
        ECLTyperecord(13),
        ECLTypevarstring(14),
        ECLTypeblob(15),
        ECLTypedata(16),
        ECLTypepointer(17),
        ECLTypeclass(18),
        ECLTypearray(19),
        ECLTypetable(20),
        ECLTypeset(21),
        ECLTyperow(22),
        ECLTypegroupedtable(23),
        ECLTypevoid(24),
        ECLTypealien(25),
        ECLTypeswapint(26),
        ECLTypepackedint(28),
        ECLTypeunused5(29),
        ECLTypeqstring(30),
        ECLTypeunicode(31),
        ECLTypeany(32),
        ECLTypevarunicode(33),
        ECLTypepattern(34),
        ECLTyperule(35),
        ECLTypetoken(36),
        ECLTypefeature(37),
        ECLTypeevent(38),
        ECLTypenull(39),
        ECLTypescope(40),
        ECLTypeutf8(41),
        ECLTypetransform(42),
        ECLTypeifblock(43), // not a real type -but used for the rtlfield serialization
        ECLTypefunction(44),
        ECLTypesortlist(45),
        ECLTypemodifier(0xff), // used  by  getKind()
        ECLTypeunsigned(0x100), // combined with some of the above, when
                                // returning summary type information. Not
                                // returned by getTypeCode()
        ECLTypeebcdic(0x200), // combined with some of the above, when returning
                              // summary type information. Not returned by
                              // getTypeCode()
        // Some pseudo types - never actually created
        ECLTypestringorunicode(0xfc), // any string/unicode variant
        ECLTypenumeric(0xfd),
        ECLTypescalar(0xfe);

        EclTypes(int eclcode){}
        
    }

    private final static HashMap<EclTypes, Integer> mapECLtypeCodeToSQLtype = new HashMap<EclTypes, Integer>();
    static
    {
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeboolean, java.sql.Types.BOOLEAN);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypearray, java.sql.Types.ARRAY);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeblob, java.sql.Types.BLOB);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypechar, java.sql.Types.CHAR);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypedate, java.sql.Types.DATE);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypedecimal, java.sql.Types.DECIMAL);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeint, java.sql.Types.INTEGER);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypenull, java.sql.Types.NULL);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypenumeric, java.sql.Types.NUMERIC);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypepackedint, java.sql.Types.INTEGER);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypepointer, java.sql.Types.REF);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeqstring, java.sql.Types.VARCHAR);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypereal, java.sql.Types.REAL);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypestring, java.sql.Types.VARCHAR);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeunsigned, java.sql.Types.NUMERIC);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypevarstring, java.sql.Types.VARCHAR);
        mapECLtypeCodeToSQLtype.put(EclTypes.ECLTypeunicode, java.sql.Types.VARCHAR);
    }

    /**
     * Translates an ecltype element to sql type int (java.sql.Types value)
     *
     * @param ecltype
     *            The ecl type enumerated value.
     * @return The java.sql.Types value to convert to a string
     *            representation.
     */
    public static int convertECLtypeCode2SQLtype(EclTypes ecltype)
    {
        if(mapECLtypeCodeToSQLtype.containsKey(ecltype))
            return mapECLtypeCodeToSQLtype.get(ecltype);
        else
            return java.sql.Types.OTHER;
    }

    private final static HashMap<Integer, String> mapSQLtypeCodeToJavaClass = new HashMap<Integer, String>();
    static
    {
        //http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html#1051555
        //Adhering to type mapping table in oracle doc referenced above.
        //one exception is the CHAR type, which is mapped to Character in order to
        //appease some ODBC/JDBC bridges.
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.CHAR,          "java.lang.Character");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.VARCHAR,       "java.lang.String");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.LONGVARCHAR,   "java.lang.String");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.NUMERIC,       "java.math.BigDecimal");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.DECIMAL,       "java.math.BigDecimal");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.BIT,           "java.lang.Boolean");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.TINYINT,       "java.lang.Byte");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.SMALLINT,      "java.lang.Short");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.INTEGER,       "java.lang.Integer");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.BIGINT,        "java.lang.Long");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.REAL,          "java.lang.Float");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.FLOAT,         "java.lang.Double");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.DOUBLE,        "java.lang.Double");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.BINARY,        "java.lang.Byte[]");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.VARBINARY,     "java.lang.Byte[]");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.LONGVARBINARY, "java.lang.Byte[]");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.DATE,          "java.sql.Date");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.TIME,          "java.sql.Time");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.TIMESTAMP,     "java.sql.Timestamp");
        mapSQLtypeCodeToJavaClass.put(java.sql.Types.BOOLEAN,       "java.lang.Boolean");
    }

    private final static String JAVA_OBJECT_TYPE_NAME = "java.lang.Object";
    /**
     * Translates a data type from an integer (java.sql.Types value) to a string
     * that represents the corresponding class.
     *
     * @param type
     *            The java.sql.Types value to convert to a string
     *            representation.
     * @return The class name that corresponds to the given java.sql.Types
     *         value, or "java.lang.Object" if the type has no known mapping.
     */
    public static String convertSQLtype2JavaClassName(int type)
    {
        if(mapSQLtypeCodeToJavaClass.containsKey(type))
            return mapSQLtypeCodeToJavaClass.get(type);
        else
            return JAVA_OBJECT_TYPE_NAME;
    }

    public static Object createSqlTypeObjFromStringObj(int sqltype, Object objstrrepresentation)
    {
        if (objstrrepresentation == null)
            return null;
        else
            return createSqlTypeObjFromString(sqltype, objstrrepresentation.toString()); //cast to String instead???
    }

    public static Object createSqlTypeObjFromString(int type, String strrepresentation)
    {
        if (strrepresentation == null)
            return null;
        else
        {
            try
            {
                return Class.forName(convertSQLtype2JavaClassName(type)).getConstructor(String.class).newInstance(strrepresentation);
            }
            catch (Exception e)
            {
                HPCCJDBCUtils.traceoutln(Level.WARNING, "HPCC JDBC: Field of type: java.sql.Types-" + type + " could not be cast to native Java type (treat as String).");
                HPCCJDBCUtils.traceoutln(Level.WARNING,  e.getLocalizedMessage());
                return strrepresentation;
            }
        }
    }

    /**
     * Attempts to map a string value to an enum value of
     * a given enum class.
     *
     * Iterates through all enum values of given enum class,
     * and compares to given string.
     * Returns enum value if it finds match, otherwise throws Exception
     *
     * @param enumclass reference to target enumaration
     * @param strvalue string value to be mapped to enum value
     *
     * @return The corresponding enum value if found
     *
     * @throws IllegalArgumentException if strvalue cannot be mapped to
     * given enum
     *
     **/
    @SuppressWarnings("unchecked")
	public static <T extends Enum<T>> T findEnumValFromString(Class<T> enumclass, String strvalue)
    {
        for(Enum<?> enumValue : enumclass.getEnumConstants())
        {
            if(enumValue.name().equalsIgnoreCase(strvalue))
            {
                return (T) enumValue;
            }
        }
        throw new IllegalArgumentException(enumclass.getName() +".'" + strvalue + "' is not valid.");
    }

    public static final Pattern BOOLEANPATTERN = Pattern.compile(
            "((?i)true|(?i)false)",Pattern.DOTALL);

    public static boolean isBooleanKeyWord(String str)
    {
       return BOOLEANPATTERN.matcher(str).matches();
    }

    public final static HashMap<Integer, Integer> mapSQLTypeToPrecedence = new HashMap<Integer, Integer>();
    static
    {
        int precedence = Integer.MAX_VALUE;
        mapSQLTypeToPrecedence.put(java.sql.Types.DOUBLE, precedence--);
        mapSQLTypeToPrecedence.put(java.sql.Types.REAL, precedence--);
        mapSQLTypeToPrecedence.put(java.sql.Types.DECIMAL, precedence--);
        mapSQLTypeToPrecedence.put(java.sql.Types.INTEGER, precedence--);
        mapSQLTypeToPrecedence.put(java.sql.Types.SMALLINT, precedence--);
    }

    public static int getNumericSqlTypePrecedence(int sqlType)
    {
        if (mapSQLTypeToPrecedence.containsKey(sqlType))
            return mapSQLTypeToPrecedence.get(sqlType);
        else
            return Integer.MIN_VALUE;
    }

    //public static final Pattern URLPATTERN = Pattern.compile("\\b(?:(https?|ftp|file)://|www\\.)?[-A-Z0-9+&#/%?=~_|$!:,.;]*[A-Z0-9+&@#/%=~_|$]\\.[-A-Z0-9+&@#/%?=~_|$!:,.;]*[A-Z0-9+&@#/%=~_|$]", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    public static final Pattern URLPROTPATTERN = Pattern.compile("((https?|ftp|file)://|www\\.).+", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

    public static URL makeURL(String urlstr)
    {
        URL theURL = null;
        try
        {
            theURL = new URL(ensureURLProtocol(urlstr));
        }
        catch (MalformedURLException e)
        {
            e.printStackTrace();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return theURL;
    }

    public static String ensureURLProtocol(String urlstr)
    {
        if (!URLPROTPATTERN.matcher(urlstr).matches())
        {
            return "http://" + urlstr;
        }
        else
            return urlstr;
    }
    
    public static boolean containsStringCaseInsensitive(List<String> list, String string) {
    	for (String listItem : list) {
			if (listItem.toLowerCase().equals(string.toLowerCase())) {
				return true;
			} 
		}
    	return false;
    }
    
    public static boolean containsStringCaseInsensitive(Set<String> set, String string) {
    	if (set == null) return false;
    	return containsStringCaseInsensitive(new ArrayList<String>(set), string);
    }
}
