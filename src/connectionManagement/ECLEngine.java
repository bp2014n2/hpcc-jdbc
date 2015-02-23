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

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import connectionManagement.HPCCColumnMetaData.ColumnType;
import net.sf.jsqlparser.schema.Table;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class ECLEngine
{
    private HPCCQuery               hpccPublishedQuery = null;
    private String                  expectedDSName = null;
    private NodeList                resultSchema = null;
    private final Properties        hpccConnProps;
    private SQLParser               sqlParser;
    private String					sqlQuery;
    private HPCCDatabaseMetaData    dbMetadata;

    private StringBuilder           eclCode = new StringBuilder();
    private URL                     hpccRequestUrl;
    private ArrayList<HPCCColumnMetaData> storeProcInParams = null;
    private String[]                    procInParamValues = null;
    private List<HPCCColumnMetaData>    expectedretcolumns = null;

    private static final int            INDEXSCORECRITERIAVARS         = 3;

    private static final int            NumberOfCommonParamInThisIndex_INDEX = 0;
    private static final int            LeftMostKeyIndexPosition_INDEX       = 1;
    private static final int            NumberofColsKeyedInThisIndex_INDEX   = 2;

    private static final int            NumberOfCommonParamInThisIndex_WEIGHT = 5;
    private static final int            LeftMostKeyIndexPosition_WEIGHT       = 3;
    private static final int            NumberofColsKeyedInThisIndex_WEIGHT   = 2;

    private static final String         SELECTOUTPUTNAME = "JDBCSelectQueryResult";
    private static final String			HPCCEngine = "THOR";
    

    private DocumentBuilderFactory      dbf = DocumentBuilderFactory.newInstance();

    public ECLEngine(HPCCDatabaseMetaData dbmetadata, Properties props, String sql)
    {
        this.hpccConnProps = props;
        this.dbMetadata = dbmetadata;
        this.sqlQuery = sql;
        
        System.out.println("sql: \n"+ sql);
        this.sqlParser = new SQLParser(sql);
        
        
    }

    public List<HPCCColumnMetaData> getExpectedRetCols()
    {
        return expectedretcolumns;
    }

    private void addFileColsToAvailableCols(DFUFile dfufile, HashMap<String, HPCCColumnMetaData> availablecols)
    {
        Enumeration fields = dfufile.getAllFields();
        while (fields.hasMoreElements())
        {
            HPCCColumnMetaData col = (HPCCColumnMetaData) fields.nextElement();
            availablecols.put(col.getTableName().toUpperCase() + "." + col.getColumnName().toUpperCase(), col);
        }
    }
    
    

    public NodeList executeSelectConstant()
    {
        try
        {
            long startTime = System.currentTimeMillis();

            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(hpccRequestUrl);

            return parseDataset(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    	
    public void generateECL() throws SQLException
    {
    	/*
        eclCode = new StringBuilder("");
        hpccRequestUrl = null;
        
//      TODO: implement layout-generation and loading of all datasets

    	for (String table : tables) {
    		eclCode.append(table.split("::")[1]).append(" := ").append("DATASET(");
    		eclCode.append("'~").append(table).append("'");
        	eclCode.append(", ").append("Layout_"+table.split("::")[1]).append(",").append(HPCCEngine).append(");\n");
    	}
        */
    	eclCode.append("#OPTION('outputlimit', 2000);\n");
    	eclCode.append(generateImports());
        eclCode.append(generateLayouts());
		eclCode.append(generateTables());
		
		eclCode.append("OUTPUT(");
		ECLBuilder eclBuilder = new ECLBuilder();
    	eclCode.append(eclBuilder.generateECL(sqlQuery));
    	eclCode.append(");");
    	
    	System.out.println(eclCode.toString());
    	
    	String tables = sqlParser.getAllTables().get(0);
    	DFUFile hpccQueryFile = dbMetadata.getDFUFile(tables);
    	HashMap<String, HPCCColumnMetaData> availablecols = new HashMap<String, HPCCColumnMetaData>();
    	addFileColsToAvailableCols(hpccQueryFile, availablecols);
    	expectedretcolumns = new LinkedList<HPCCColumnMetaData>();
  
    	for (int i=0; i<sqlParser.getSelectItems().size(); i++) {
    		String column = sqlParser.getSelectItems().get(i).toString();
    		expectedretcolumns.add(new HPCCColumnMetaData(column, i, null));
    	}
    	
    	
        generateSelectURL();
    }
    
    private String generateImports() {
    	return "IMPORT STD;\n";
    }
    
    private String generateLayouts() {
		StringBuilder layoutsString = new StringBuilder();
		for (String table : sqlParser.getAllTables()) {
			String tableName = table.split("\\.")[1];
			layoutsString.append("Layout_"+tableName+" := ");
			layoutsString.append(ECLBuilder.getLayouts().get(tableName));
			layoutsString.append("\n");
			
		}
//		layoutsString.append("\n");
//		System.out.println("layouts_string: \n"+layoutsString.toString());
		return layoutsString.toString();
	}
    
    private String generateTables() {
    	StringBuilder datasetsString = new StringBuilder();
    	for (String table : sqlParser.getAllTables()) {
			String tableName = table.split("\\.")[1];
			datasetsString.append(tableName).append(" := ").append("DATASET(");
			datasetsString.append("'~").append(table.replaceAll("\\.", "::")).append("'");
			datasetsString.append(", ").append("Layout_"+tableName).append(",").append(HPCCEngine).append(");\n");
			
		}
    	return datasetsString.toString();
    }

    private void generateSelectURL() throws SQLException
    {
        try
        {
            String urlString = hpccConnProps.getProperty("WsECLDirectAddress") + ":"
                    + hpccConnProps.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit";

            if (hpccConnProps.containsKey("TargetCluster"))
                urlString += "&cluster=" + hpccConnProps.getProperty("TargetCluster");
            else
                HPCCJDBCUtils.traceoutln(Level.INFO,  "No cluster property found, executing query on EclDirect default cluster");

            hpccRequestUrl = HPCCJDBCUtils.makeURL(urlString);

            HPCCJDBCUtils.traceoutln(Level.INFO,  "HPCC URL created: " + urlString);
        }
        catch (Exception e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    public NodeList execute(Map inParameters) throws Exception
    {
        return executeSelect(inParameters);
    }

    public NodeList executeSelect(Map inParameters)
    {
        int responseCode = -1;

        try
        {
            StringBuilder sb = new StringBuilder();

            sb.append("&eclText=\n");

            /*int expectedParamCount = sqlParser.getParameterizedCount();
            if (expectedParamCount > 0 && inParameters != null)
            {
                if (expectedParamCount <= inParameters.size())
                {
                    for (int paramIndex = 1; paramIndex <= inParameters.size(); paramIndex++)
                    {
                        Object invalue = inParameters.get(paramIndex);
                        String value = null;

                        if (invalue != null)
                        {
                            try
                            {
                                if (invalue instanceof String)
                                {
                                    sb.append("STRING ");
                                    value = (String)invalue;
                                    if (value.isEmpty())
                                        value = "''";
                                }
                                else if (invalue instanceof Boolean)
                                {
                                    sb.append("BOOLEAN ");
                                    value = ((Boolean) invalue).toString();
                                }
                                else if (invalue instanceof Byte)
                                {
                                    value = ((Byte) invalue).toString();
                                }
                                else if (invalue instanceof Short)
                                {
                                    value = ((Short) invalue).toString();
                                }
                                else if (invalue instanceof Integer)
                                {
                                    sb.append("INTEGER ");
                                    value = ((Integer) invalue).toString();
                                }
                                else if (invalue instanceof Long)
                                {
                                    sb.append("INTEGER ");
                                    value = ((Long) invalue).toString();
                                }
                                else if (invalue instanceof Float)
                                {
                                    sb.append("REAL ");
                                    value = ((Float) invalue).toString();
                                }
                                else if (invalue instanceof Double)
                                {
                                    sb.append("DECIMAL");
                                    value = ((Double) invalue).toString();
                                }
                                else if (invalue instanceof BigDecimal)
                                {
                                    sb.append("DECIMAL");
                                    value = ((BigDecimal)invalue).toString();
                                }
                                else if (invalue instanceof byte[])
                                {
                                    sb.append("STRING ");
                                    value = ((byte[]) invalue).toString();
                                }
                                else if (invalue instanceof Time)
                                {
                                    sb.append("STRING ");
                                    value = ((Time) invalue).toString();
                                }
                                else if (invalue instanceof java.sql.Date)
                                {
                                    sb.append("STRING ");
                                    value = ((java.sql.Date) invalue).toString();
                                }
                                else if (invalue instanceof Timestamp)
                                {
                                    sb.append("STRING ");
                                    value = ((Timestamp) invalue).toString();
                                }
                                else if (invalue instanceof InputStream)
                                {
                                    sb.append("STRING ");
                                    value = ((InputStream) invalue).toString();
                                }
                                else
                                {
                                    sb.append("STRING ");
                                    value = invalue.toString();
                                }
                            }
                            catch (Exception e)
                            {
                                throw new SQLException("Error while converting input parameter(" + paramIndex + ") to string representation.");
                            }
                        }
                        else
                            throw new SQLException("Could not bind parameter (null)");

                        sb.append(SQLParser.parameterizedPrefix).append(paramIndex).append(" := ").append(value).append(";\n");
                    }
                }
                else
                    throw new Exception("Insufficient number of parameters provided");
            }*/
            
            System.out.println(eclCode.toString());
            sb.append(eclCode.toString());
            sb.append("\n");

            long startTime = System.currentTimeMillis();
            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(hpccRequestUrl);

            //HPCCJDBCUtils.traceoutln(Level.INFO,  "Executing ECL: " + sb);
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(sb.toString());
            wr.flush();

            responseCode = conn.getResponseCode();

            return parseDataset(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            if (responseCode != 200)
            {
                throw new RuntimeException("HTTP Connection Response code: " + responseCode
                        + "\nVerify access to WsECLDirect: " + hpccRequestUrl, e);
            }
            else
                throw new RuntimeException(e);
        }
    }

    /*private void addFilterClause(StringBuilder sb)
    {
        String whereclause = sqlParser.getWhereClauseString();
        if (whereclause != null && whereclause.length() > 0)
        {
            sb.append("( ");
            sb.append(whereclause);
            sb.append(" )");
        }
    }*/

    /*private boolean appendTranslatedHavingClause(StringBuilder sb, String latesDSName)
    {
        if (sqlParser.hasHavingClause())
        {
            HashMap<String, String> translator = new HashMap<String, String>();
            List<SQLTable> sqltables = sqlParser.getSQLTables();
            for(SQLTable table : sqltables)
            {
                translator.put(table.getName().toUpperCase(), "LEFT");
            }

            String havingclause = sqlParser.getHavingClause().toECLStringTranslateSource(translator, false, true, false, false);

            if (havingclause.length() > 0)
            {
                sb.append(latesDSName).append("Having").append(" := HAVING( ");
                sb.append(latesDSName);
                sb.append(", ");
                sb.append(havingclause);
                sb.append(" );\n");

                return true;
            }
        }

        return false;
    }*/

    /*public NodeList executeCall( Map inParameters)
    {
        StringBuilder sb = new StringBuilder();
        try
        {
            if (procInParamValues != null)
            {
                for (int columindex = 0, parameterindex = 0; columindex < procInParamValues.length && columindex < storeProcInParams.size(); columindex++)
                {
                    String key = storeProcInParams.get(columindex).getColumnName();
                    String value = procInParamValues[columindex];

                    if (HPCCJDBCUtils.isParameterizedStr(value))
                    {
                        if (inParameters != null && parameterindex <= inParameters.size())
                        {
                            Object invalue = inParameters.get(parameterindex + 1);

                            if (invalue != null)
                            {
                                try
                                {
                                    if (invalue instanceof String)
                                        value = (String)invalue;
                                    else if (invalue instanceof Boolean)
                                        value = ((Boolean) invalue).toString();
                                    else if (invalue instanceof Byte)
                                        value = ((Byte) invalue).toString();
                                    else if (invalue instanceof Short)
                                        value = ((Short) invalue).toString();
                                    else if (invalue instanceof Integer)
                                        value = ((Integer) invalue).toString();
                                    else if (invalue instanceof Long)
                                        value = ((Long) invalue).toString();
                                    else if (invalue instanceof Float)
                                        value = ((Float) invalue).toString();
                                    else if (invalue instanceof Double)
                                        value = ((Double) invalue).toString();
                                    else if (invalue instanceof BigDecimal)
                                        value = ((BigDecimal)invalue).toString();
                                    else if (invalue instanceof byte[])
                                        value = ((byte[]) invalue).toString();
                                    else if (invalue instanceof Time)
                                        value = ((Time) invalue).toString();
                                    else if (invalue instanceof java.sql.Date)
                                        value = ((java.sql.Date) invalue).toString();
                                    else if (invalue instanceof Timestamp)
                                        value = ((Timestamp) invalue).toString();
                                    else if (invalue instanceof InputStream)
                                        value = ((InputStream) invalue).toString();
                                    else
                                        value = invalue.toString();
                                }
                                catch (Exception e)
                                {
                                    throw new SQLException("Error while converting input parameter(" + parameterindex +") to string representation.");
                                }
                            }
                            else
                                throw new SQLException("Could not bind parameter");

                            if (value == null)
                                throw new SQLException("Could not bind parameter");
                            parameterindex++;
                        }
                        else
                            throw new SQLException("Detected empty input parameter list");
                    }
                    if (value != null && !value.isEmpty())
                        sb.append("&").append(key).append("=").append(URLEncoder.encode(value, "UTF-8"));
                }
            }

            long startTime = System.currentTimeMillis();

            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(hpccRequestUrl);

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(sb.toString());
            wr.flush();

            HPCCJDBCUtils.traceoutln(Level.INFO,  "Executing: " + hpccRequestUrl + " : " + sb.toString());

            return parseDataset(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }*/

    public NodeList parseDataset(InputStream xml, long startTime) throws Exception
    {
        NodeList rowList = null;

        DocumentBuilder db = dbf.newDocumentBuilder();
        Document dom = db.parse(xml);

        long elapsedTime = System.currentTimeMillis() - startTime;

        HPCCJDBCUtils.traceoutln(Level.INFO, "Total elapsed http request/response time in milliseconds: " + elapsedTime);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("Dataset");

        HPCCJDBCUtils.traceoutln(Level.INFO, "Parsing results...");

        int dsCount = 0;
        if (dsList != null && (dsCount = dsList.getLength()) > 0)
        {
            HPCCJDBCUtils.traceoutln(Level.INFO, "Results datsets found: " + dsList.getLength());

            // The dataset element is encapsulated within a Result element
            // need to fetch appropriate resulst dataset

            for (int i = 0; i < dsCount; i++)
            {
                Element ds = (Element) dsList.item(i);
                String currentdatsetname = ds.getAttribute("name");
                if (expectedDSName == null || expectedDSName.length() == 0
                        || currentdatsetname.equalsIgnoreCase(expectedDSName))
                {
                    rowList = ds.getElementsByTagName("Row");
                    break;
                }
            }
        }
        else if (docElement.getElementsByTagName("Exception").getLength() > 0)
        {
            NodeList exceptionlist = docElement.getElementsByTagName("Exception");

            if (exceptionlist.getLength() > 0)
            {
                Exception resexception = null;
                NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

                for (int j = 0; j < currexceptionelements.getLength(); j++)
                {
                    Node exceptionelement = currexceptionelements.item(j);
                    if (exceptionelement.getNodeName().equals("Message"))
                    {
                        resexception = new Exception("HPCCJDBC: Error in response: \'"
                                + exceptionelement.getTextContent() + "\'");
                    }
                }
                if (dsList == null || dsList.getLength() <= 0)
                    throw resexception;
            }
        }
        else
        {
            // The root element is itself the Dataset element
            if (dsCount == 0)
            {
                rowList = docElement.getElementsByTagName("Row");
            }
        }
        HPCCJDBCUtils.traceoutln(Level.INFO,  "Finished Parsing results.");

        return rowList;
    }

    public boolean hasResultSchema()
    {
        return (this.resultSchema != null && this.resultSchema.getLength() > 0);
    }

    public void setResultschema(NodeList resultschema)
    {
        this.resultSchema = resultschema;

        if (this.resultSchema != null && this.resultSchema.getLength() > 0)
        {
            HPCCJDBCUtils.traceoutln(Level.INFO,  "contains resultschema");
        }
    }

    public NodeList getResultschema()
    {
        return resultSchema;
    }

    /*public String findAppropriateIndex(String index, List<HPCCColumnMetaData> expectedretcolumns, SQLParser parser)
    {
        List<String> indexhint = new ArrayList<String>();
        indexhint.add(index);
        return findAppropriateIndex(indexhint, expectedretcolumns, parser);
    }*/

    /*public String findAppropriateIndex(List<String> relindexes, List<HPCCColumnMetaData> expectedretcolumns, SQLParser parser)
    {
        String indextouse = null;
        List<String> sqlqueryparamnames = new ArrayList<String>();
        parser.getUniqueWhereClauseColumnNames(sqlqueryparamnames);
        if (sqlqueryparamnames == null || sqlqueryparamnames.size() <= 0)
            return indextouse;

        int totalparamcount = parser.getWhereClauseExpressionsCount();
        /*[ FieldsInIndexCount ][LeftMostKeyIndex][ColsKeyedcount]*
        int indexscore[][] = new int[relindexes.size()][INDEXSCORECRITERIAVARS];
        int highscore = Integer.MIN_VALUE;
        boolean payloadIdxWithAtLeast1KeyedFieldFound = false;
        for (int indexcounter = 0; indexcounter < relindexes.size(); indexcounter++)
        {
            String indexname = relindexes.get(indexcounter);
            DFUFile indexfile = dbMetadata.getDFUFile(indexname);
            if (indexfile != null && indexfile.isKeyFile() && indexfile.hasValidIdxFilePosField())
            {
                for (int j = 0; j < expectedretcolumns.size(); j++)
                {
                    if (indexfile.containsField(expectedretcolumns.get(j), true))
                        ++indexscore[indexcounter][NumberOfCommonParamInThisIndex_INDEX];
                }
                if (payloadIdxWithAtLeast1KeyedFieldFound
                        && indexscore[indexcounter][NumberOfCommonParamInThisIndex_INDEX] == 0)
                    break; // Don't bother with this index
                int localleftmostindex = Integer.MAX_VALUE;

                Properties KeyColumns = indexfile.getKeyedColumns();
                if (KeyColumns != null)
                {
                    for (String currentparam : sqlqueryparamnames)
                    {
                        if (KeyColumns.contains(currentparam))
                        {
                            ++indexscore[indexcounter][NumberofColsKeyedInThisIndex_INDEX];
                            int paramindex = indexfile.getKeyColumnIndex(currentparam);
                            if (localleftmostindex > paramindex)
                                localleftmostindex = paramindex;
                        }
                    }
                    indexscore[indexcounter][LeftMostKeyIndexPosition_INDEX] = localleftmostindex;
                }
                if (indexscore[indexcounter][NumberOfCommonParamInThisIndex_INDEX] == expectedretcolumns.size()
                        && indexscore[indexcounter][NumberofColsKeyedInThisIndex_INDEX] > 0
                        && (!parser.whereClauseContainsOrOperator()))
                    payloadIdxWithAtLeast1KeyedFieldFound = true; // during scoring, give this priority
            }
        }

        for (int i = 0; i < relindexes.size(); i++)
        {
            if (indexscore[i][NumberofColsKeyedInThisIndex_INDEX] == 0) // does one imply the other?
                continue; // not good enough
            if (payloadIdxWithAtLeast1KeyedFieldFound
                    && indexscore[i][NumberOfCommonParamInThisIndex_INDEX] < expectedretcolumns.size())
                continue; // not good enough
            if (indexscore[i][NumberofColsKeyedInThisIndex_INDEX] < totalparamcount
                    && parser.whereClauseContainsOrOperator())
                continue; // not so sure about this rule.

            int localscore =
                    ((indexscore[i][NumberOfCommonParamInThisIndex_INDEX] / expectedretcolumns.size()) * NumberOfCommonParamInThisIndex_WEIGHT)
                    - (((indexscore[i][LeftMostKeyIndexPosition_INDEX] / totalparamcount) - 1) * LeftMostKeyIndexPosition_WEIGHT)
                    + ((indexscore[i][NumberofColsKeyedInThisIndex_INDEX]) * NumberofColsKeyedInThisIndex_WEIGHT);

            if (highscore < localscore)
            {
                highscore = localscore;
                indextouse = relindexes.get(i);
            }
        }
        return indextouse;
    }*/
}
