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

package de.hpi.hpcc.main;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import de.hpi.hpcc.logging.HPCCLogger;

/**
 *
 * @author rpastrana
 */

public class HPCCResultSet implements ResultSet
{
    private boolean                             closed = false;
    private List<List>                          rows;
    private int                                 index = -1;
    private HPCCResultSetMetadata               resultMetadata;
    private Statement                           statement = null;
    private Object                              lastResult;
    private SQLWarning                          warnings = null;
    private static final Logger	logger = HPCCLogger.getLogger();

    public HPCCResultSet(List recrows, ArrayList<HPCCColumnMetaData> metadatacols, String tablename) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: HPCCResultSet(recrows, metadatacols, " + tablename +")");
        resultMetadata = new HPCCResultSetMetadata(metadatacols, tablename);
        rows = new ArrayList<List>(recrows);
        lastResult = new Object();
    }

    public HPCCResultSet(Statement statement, NodeList rowList, HPCCResultSetMetadata resultMetadata)
    {
        log(Level.FINEST, "HPCCResultSet: HPCCResultSet(statement, rowList, resultMetadata)");
        this.resultMetadata = resultMetadata;
        rows = new ArrayList();
        lastResult = new Object();

        this.statement = statement;

        encapsulateDataSet(rowList);
    }

    public void encapsulateDataSet(NodeList rowList)
    {
        log("HPCCResultSet encapsulateDataSet");
        int rowCount = 0;
        if (rowList != null && (rowCount = rowList.getLength()) > 0)
        {
            log("Results rows found: " + rowCount);

            for (int j = 0; j < rowCount; j++)
            {
                ArrayList<String> rowValues = resultMetadata.createDefaultResultRow();
                rows.add(rowValues);

                Element row = (Element) rowList.item(j);

                NodeList columnList = row.getChildNodes();

                for (int k = 0; k < columnList.getLength(); k++)
                {
                    Node resultRowElement = columnList.item(k);
                    String resultRowElementName = resultRowElement.getNodeName();

                    if (resultMetadata.containsColByNameOrAlias(resultRowElementName))
                    {
                        HPCCColumnMetaData col = resultMetadata.getColByNameOrAlias(resultRowElementName);
                        rowValues.set(col.getIndex(), resultRowElement.getTextContent().trim());
                    }
                }
            }
        }
    }

    public int getRowCount()
    {
        log(Level.FINEST, "HPCCResultSet getRowCount");
        return rows.size();
    }

	public boolean next() throws SQLException {
		log(Level.FINEST, "HPCCResultSet next");
		index++;
		return !(index >= rows.size());
	}

    public void close() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet close");
        closed = true;
    }

    public boolean wasNull() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet wasNull: " + String.valueOf(lastResult == null).toString());
        return lastResult == null;
    }

    public String getString(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet:getString(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                {
                    log(Level.FINEST, "....Returned: null");
                    return null;
                }
                log(Level.FINEST, "....Returned: "+ lastResult.toString());
                return lastResult.toString();
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public boolean getBoolean(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBoolean(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                // content of row field is Object string, need to get value of
                // string and parse as boolean
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return false;
                return Boolean.parseBoolean(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getByte(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                return String.valueOf(lastResult).getBytes()[0];
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public short getShort(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getShort(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Short
                return Short.parseShort(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public int getInt(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getInt(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Int
                return Integer.parseInt(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public long getLong(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getLong(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Long
                return Long.parseLong(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public float getFloat(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getFloat(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Float
                return Float.parseFloat(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getDouble(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Double
                return Double.parseDouble(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBigDecimal(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as BigDecimal
                BigDecimal bd = new BigDecimal(String.valueOf(lastResult));
                return bd.setScale(scale);
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBytes(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return String.valueOf(lastResult).getBytes();
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public Date getDate(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getDate(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Date
                return Date.valueOf(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public Time getTime(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getTime(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Time
                return Time.valueOf(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getTimestamp(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                /*
                 * TODO: 
                 * why is it an empty string instead of null?!
                 */
                if (lastResult == null || lastResult.equals(""))
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Timestamp
                return Timestamp.valueOf(String.valueOf(lastResult).trim());
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getAsciiStream(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getUnicodeStream(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBinaryStream(" + columnIndex + ")");
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public String getString(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet:getString(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found: " + columnLabel);
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return null;

                return lastResult.toString();
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public boolean getBoolean(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBoolean(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return false;
                return Boolean.parseBoolean(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public byte getByte(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getByte(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                return String.valueOf(lastResult).getBytes()[0];
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public short getShort(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet:getShort(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Short
                return Short.parseShort(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public int getInt(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getInt(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Int
                return Integer.parseInt(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public long getLong(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getLong(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Long
                return Long.parseLong(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public float getFloat(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getFloat(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Float
                return Float.parseFloat(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public double getDouble(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getDouble(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int column = resultMetadata.getColumnIndex(columnLabel);
            if (column < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(column - 1);
                if (lastResult == null)
                    return 0;
                // content of row field is Object string, need to get value of
                // string and parse as Double
                return Double.parseDouble(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBigDecimal(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as BigDecimal
                BigDecimal bd = new BigDecimal(String.valueOf(lastResult));
                return bd.setScale(scale);
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public byte[] getBytes(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBytes(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return String.valueOf(lastResult).getBytes();
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public Date getDate(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getDate(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Date
                return Date.valueOf(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public Time getTime(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getTime(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Time
                return Time.valueOf(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getTimestamp(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                /*
                 * TODO: 
                 * why is it an empty string instead of null?!
                 */
                if (lastResult == null || lastResult.equals(""))
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as Timestamp
                return Timestamp.valueOf(String.valueOf(lastResult).trim());
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getAsciiStream(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getUnicodeStream(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet: getBinaryStream(" + columnLabel + ")");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                return new ByteArrayInputStream(String.valueOf(lastResult).getBytes());
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public SQLWarning getWarnings() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getWarnings");
        return warnings;
    }

    public void clearWarnings() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet clearWarnings");
        warnings = null;
    }

    public String getCursorName() throws SQLException{
        handleUnsupportedMethod("getCursorName()");
        return null;
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getMetaData");
        return resultMetadata;
    }

    public Object getObject(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getObject( " + columnIndex + " )" );
        if (index >= 0 && index <= rows.size())
        {
            lastResult = HPCCJDBCUtils.createSqlTypeObjFromStringObj(resultMetadata.getColumnType(columnIndex), rows.get(index).get(columnIndex - 1));
            return lastResult;
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public Object getObject(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getObject( " + columnLabel + " )" );
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = HPCCJDBCUtils.createSqlTypeObjFromStringObj(
                                resultMetadata.getColumnType(columnIndex),
                                row.get(columnIndex - 1));
                return lastResult;
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public int findColumn(String columnLabel) throws SQLException{
        log(Level.FINEST, "HPCCResultSet findColumn( " + columnLabel + " )" );
        return resultMetadata.getColumnIndex(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getCharacterStream(int columnIndex)");
        return null;
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getCharacterStream(String columnLabel)");
        return null;
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getBigDecimal( " + columnIndex + " )" );
        if (index >= 0 && index <= rows.size())
            if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
            {
                lastResult = rows.get(index).get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as BigDecimal
                return new BigDecimal(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Invalid Column Index");
        else
            throw new SQLException("Invalid Row Index");
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getBigDecimal( " + columnLabel + " )" );
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                lastResult = row.get(columnIndex - 1);
                if (lastResult == null)
                    return null;
                // content of row field is Object string, need to get value of
                // string and parse as BigDecimal
                return new BigDecimal(String.valueOf(lastResult));
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public boolean isBeforeFirst() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet isBeforeFirst");
        return (index < 0) ? true : false;
    }

    public boolean isAfterLast() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet isAfterLast");
        return (index > rows.size() - 1) ? true : false;
    }

    public boolean isFirst() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet isFirst");
        return index == 0 ? true : false;
    }

    public boolean isLast() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet isLast");
        return (index == rows.size() - 1) ? true : false;
    }

    public void beforeFirst() throws SQLException
    {
    	index = -1;
//        log(Level.FINEST, "HPCCResultSet beforeFirst");
//        handleUnsupportedMethod(("Not supported");
    }

    public void afterLast() throws SQLException{
        handleUnsupportedMethod("afterLast()");
    }

    public boolean first() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet first");
        if (rows.size() > 0)
        {
            index = 0;
            return true;
        }
        else
            return false;
    }

    public boolean last() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet last");
        if (rows.size() > 0)
        {
            index = rows.size() - 1;
            return true;
        }
        else
            return false;
    }

    public int getRow() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getRow");
        return index + 1;
    }

    public boolean absolute(int row) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet absolute");
        if (row > 0 && row <= rows.size())
        {
            index = row - 1;
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean relative(int rows) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet relative");
        int tmpindex = index + rows;
        if (tmpindex > 0 && tmpindex <= this.rows.size())
        {
            index = tmpindex;
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean previous() throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet previous");
        if (index > 1)
        {
            index--;
            return true;
        }
        else
        {
            return false;
        }
    }

    public void setFetchDirection(int direction) throws SQLException{
        handleUnsupportedMethod("setFetchDirection(int direction)");
    }

    public int getFetchDirection() throws SQLException{
        log(Level.FINEST, "HPCCResultSet getFetchDirection");
        return ResultSet.FETCH_FORWARD;
    }

    public void setFetchSize(int rows) throws SQLException{
    	return;
//        handleUnsupportedMethod("setFetchSize(int rows)");
    }

    public int getFetchSize() throws SQLException{
        handleUnsupportedMethod("getFetchSize()");
        return 0;
    }

    public int getType() throws SQLException{
        log(Level.FINEST, "HPCCResultSet getType");
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public int getConcurrency() throws SQLException{
        handleUnsupportedMethod("getConcurrency()");
        return 0;
    }

    public boolean rowUpdated() throws SQLException{
        handleUnsupportedMethod("rowUpdated()");
        return false;
    }

    public boolean rowInserted() throws SQLException{
        handleUnsupportedMethod("rowInserted()");
        return false;
    }

    public boolean rowDeleted() throws SQLException{
        handleUnsupportedMethod("rowDeleted()");
        return false;
    }

    public void updateNull(int columnIndex) throws SQLException{
        handleUnsupportedMethod("updateNull(int columnIndex)");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException{
        handleUnsupportedMethod("updateBoolean(int columnIndex, boolean x)");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException{
        handleUnsupportedMethod("updateByte(int columnIndex, byte x)");
    }

    public void updateShort(int columnIndex, short x) throws SQLException{
        handleUnsupportedMethod("updateShort(int columnIndex, short x)");
    }

    public void updateInt(int columnIndex, int x) throws SQLException{
        handleUnsupportedMethod("updateInt(int columnIndex, int x)");
    }

    public void updateLong(int columnIndex, long x) throws SQLException{
        handleUnsupportedMethod("updateLong(int columnIndex, long x)");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException{
        handleUnsupportedMethod("updateFloat(int columnIndex, float x)");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException{
        handleUnsupportedMethod("updateDouble(int columnIndex, double x)");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException{
        handleUnsupportedMethod("updateBigDecimal(int columnIndex, BigDecimal x)");
    }

    public void updateString(int columnIndex, String x) throws SQLException{
        handleUnsupportedMethod("updateString(int columnIndex, String x)");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException{
        handleUnsupportedMethod("updateBytes(int columnIndex, byte[] x)");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException{
        handleUnsupportedMethod("updateDate(int columnIndex, Date x)");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException{
        handleUnsupportedMethod("updateTime(int columnIndex, Time x)");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException{
        handleUnsupportedMethod("updateTimestamp(int columnIndex, Timestamp x)");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(int columnIndex, InputStream x, int length)");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(int columnIndex, InputStream x, int length)");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(int columnIndex, Reader x, int length)");
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException{
        handleUnsupportedMethod("updateObject(int columnIndex, Object x, int scaleOrLength)");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException{
        handleUnsupportedMethod("updateObject(int columnIndex, Object x)");
    }

    public void updateNull(String columnLabel) throws SQLException{
        handleUnsupportedMethod("updateNull(String columnLabel)");
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException{
        handleUnsupportedMethod("updateBoolean(String columnLabel, boolean x)");
    }

    public void updateByte(String columnLabel, byte x) throws SQLException{
        handleUnsupportedMethod("updateByte(String columnLabel, byte x)");
    }

    public void updateShort(String columnLabel, short x) throws SQLException{
        handleUnsupportedMethod("updateShort(String columnLabel, short x)");
    }

    public void updateInt(String columnLabel, int x) throws SQLException{
        handleUnsupportedMethod("updateInt(String columnLabel, int x)");
    }

    public void updateLong(String columnLabel, long x) throws SQLException{
        handleUnsupportedMethod("updateLong(String columnLabel, long x)");
    }

    public void updateFloat(String columnLabel, float x) throws SQLException{
        handleUnsupportedMethod("updateFloat(String columnLabel, float x)");
    }

    public void updateDouble(String columnLabel, double x) throws SQLException{
        handleUnsupportedMethod("updateDouble(String columnLabel, double x)");
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException{
        handleUnsupportedMethod("updateBigDecimal(String columnLabel, BigDecimal x)");
    }

    public void updateString(String columnLabel, String x) throws SQLException{
        handleUnsupportedMethod("updateString(String columnLabel, String x)");
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException{
        handleUnsupportedMethod("updateBytes(String columnLabel, byte[] x)");
    }

    public void updateDate(String columnLabel, Date x) throws SQLException{
    	handleUnsupportedMethod("updateDate(String columnLabel, Date x)");
    }

    public void updateTime(String columnLabel, Time x) throws SQLException{
        handleUnsupportedMethod("updateTime(String columnLabel, Time x)");
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException{
        handleUnsupportedMethod("updateTimestamp(String columnLabel, Timestamp x)");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(String columnLabel, InputStream x, int length)");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(String columnLabel, InputStream x, int length)");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(String columnLabel, Reader reader, int length)");
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException{
        handleUnsupportedMethod("updateObject(String columnLabel, Object x, int scaleOrLength)");
    }

    public void updateObject(String columnLabel, Object x) throws SQLException{
        handleUnsupportedMethod("updateObject(String columnLabel, Object x)");
    }

    public void insertRow() throws SQLException{
        handleUnsupportedMethod("insertRow()");
    }

    public void updateRow() throws SQLException{
        handleUnsupportedMethod("updateRow()");
    }

    public void deleteRow() throws SQLException{
        handleUnsupportedMethod("deleteRow()");
    }

    public void refreshRow() throws SQLException{
        handleUnsupportedMethod("refreshRow()");
    }

    public void cancelRowUpdates() throws SQLException{
        handleUnsupportedMethod("cancelRowUpdates()");
    }

    public void moveToInsertRow() throws SQLException{
        handleUnsupportedMethod("moveToInsertRow()");
    }

    public void moveToCurrentRow() throws SQLException{
        handleUnsupportedMethod("moveToCurrentRow()");
    }

    public Statement getStatement() throws SQLException{
        log(Level.FINEST, "HPCCResultSet getStatement");
        return statement;
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException{
        handleUnsupportedMethod("getObject(int columnIndex, Map<String, Class<?>> map)");
        return null;
    }

    public Ref getRef(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getRef(int columnIndex)");
        return null;
    }

    public Blob getBlob(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getBlob(int columnIndex)");
        return null;
    }

    public Clob getClob(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getClob(int columnIndex)");
        return null;
    }

    public Array getArray(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getArray(int columnIndex)");
        return null;
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException{
        handleUnsupportedMethod("getObject(String columnLabel, Map<String, Class<?>> map)");
        return null;
    }

    public Ref getRef(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getRef(String columnLabel)");
        return null;
    }

    public Blob getBlob(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getBlob(String columnLabel)");
        return null;
    }

    public Clob getClob(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getClob(String columnLabel)");
        return null;
    }

    public Array getArray(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getArray(String columnLabel)");
        return null;
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getDate(int columnIndex, Calendar cal)");
        return null;
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getDate(String columnLabel, Calendar cal)");
        return null;
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getTime(int columnIndex, Calendar cal)");
        return null;
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getTime(String columnLabel, Calendar cal)");
        return null;
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getTimestamp(int columnIndex, Calendar cal)");
        return null;
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException{
        handleUnsupportedMethod("getTimestamp(String columnLabel, Calendar cal)");
        return null;
    }

    public URL getURL(int columnIndex) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getURL");
        try
        {
            if (index >= 0 && index <= rows.size())
                if (columnIndex >= 1 && columnIndex <= rows.get(index).size())
                {
                    lastResult = rows.get(index).get(columnIndex - 1);
                    if (lastResult == null)
                        return null;
                    // content of row field is Object string, need to get value
                    // of string and parse as BigDecimal
                    return new URL(String.valueOf(lastResult));
                }
                else
                    throw new SQLException("Invalid Column Index");
            else
                throw new SQLException("Invalid Row Index");
        }
        catch (MalformedURLException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    public URL getURL(String columnLabel) throws SQLException
    {
        log(Level.FINEST, "HPCCResultSet getURL");
        if (index >= 0 && index <= rows.size())
        {
            int columnIndex = resultMetadata.getColumnIndex(columnLabel);
            if (columnIndex < 0)
                throw new SQLException("Invalid Column Label found");
            List<?> row = rows.get(index);
            if (row != null)
            {
                try
                {
                    lastResult = row.get(columnIndex - 1);
                    if (lastResult == null)
                        return null;
                    return new URL(String.valueOf(lastResult));
                }
                catch (MalformedURLException e)
                {
                    throw new SQLException(e.getMessage());
                }
            }
            else
                throw new SQLException("Null Row found");
        }
        else
            throw new SQLException("Invalid Row Index");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException{
        handleUnsupportedMethod("updateRef(int columnIndex, Ref x)");
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException{
        handleUnsupportedMethod("updateRef(String columnLabel, Ref x)");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException{
        handleUnsupportedMethod("updateBlob(int columnIndex, Blob x)");
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException{
        handleUnsupportedMethod("updateBlob(String columnLabel, Blob x)");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException{
        handleUnsupportedMethod("updateClob(int columnIndex, Clob x)");
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException{
        handleUnsupportedMethod("updateClob(String columnLabel, Clob x)");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException{
        handleUnsupportedMethod("updateArray(int columnIndex, Array x)");
    }

    public void updateArray(String columnLabel, Array x) throws SQLException{
        handleUnsupportedMethod("updateArray(String columnLabel, Array x)");
    }

    public RowId getRowId(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getRowId(int columnIndex)");
        return null;
    }

    public RowId getRowId(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getRowId(String columnLabel)");
        return null;
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException{
        handleUnsupportedMethod("updateRowId(int columnIndex, RowId x)");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException{
        handleUnsupportedMethod("updateRowId(String columnLabel, RowId x)");
    }

    public int getHoldability() throws SQLException{
        handleUnsupportedMethod("getHoldability()");
        return 0;
    }

    public boolean isClosed() throws SQLException{
        log(Level.FINEST, "HPCCResultSet isClosed");
        return closed;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException{
        handleUnsupportedMethod("updateNString(int columnIndex, String nString)");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException{
        handleUnsupportedMethod("updateNString(String columnLabel, String nString)");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException{
        handleUnsupportedMethod("Not supported yet.");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException{
        handleUnsupportedMethod("updateNClob(String columnLabel, NClob nClob)");
    }

    public NClob getNClob(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getNClob(int columnIndex)");
        return null;
    }

    public NClob getNClob(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getNClob(String columnLabel)");
        return null;
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getSQLXML(int columnIndex)");
        return null;
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getSQLXML(String columnLabel)");
        return null;
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException{
        handleUnsupportedMethod("updateSQLXML(int columnIndex, SQLXML xmlObject)");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException{
        handleUnsupportedMethod("updateSQLXML(String columnLabel, SQLXML xmlObject)");
    }

    public String getNString(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getNString(int columnIndex)");
        return null;
    }

    public String getNString(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getNString(String columnLabel)");
        return null;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException{
        handleUnsupportedMethod("getNCharacterStream(int columnIndex)");
        return null;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException{
        handleUnsupportedMethod("getNCharacterStream(String columnLabel)");
        return null;
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException{
        handleUnsupportedMethod("updateNCharacterStream(int columnIndex, Reader x, long length)");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateNCharacterStream(String columnLabel, Reader reader, long length)");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(int columnIndex, InputStream x, long length)");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(int columnIndex, InputStream x, long length)");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(int columnIndex, Reader x, long length)");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(String columnLabel, InputStream x, long length)");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(String columnLabel, InputStream x, long length)");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(String columnLabel, Reader reader, long length)");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException{
        handleUnsupportedMethod("updateBlob(int columnIndex, InputStream inputStream, long length)");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException{
        handleUnsupportedMethod("updateBlob(String columnLabel, InputStream inputStream, long length)");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateClob(int columnIndex, Reader reader, long length)");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateClob(String columnLabel, Reader reader, long length)");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateNClob(int columnIndex, Reader reader, long length)");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException{
        handleUnsupportedMethod("updateNClob(String columnLabel, Reader reader, long length)");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException{
        handleUnsupportedMethod("updateNCharacterStream(int columnIndex, Reader x)");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateNCharacterStream(String columnLabel, Reader reader)");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(int columnIndex, InputStream x)");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(int columnIndex, InputStream x)");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(int columnIndex, Reader x)");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException{
        handleUnsupportedMethod("updateAsciiStream(String columnLabel, InputStream x)");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException{
        handleUnsupportedMethod("updateBinaryStream(String columnLabel, InputStream x)");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateCharacterStream(String columnLabel, Reader reader)");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException{
        handleUnsupportedMethod("updateBlob(int columnIndex, InputStream inputStream)");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException{
        handleUnsupportedMethod("updateBlob(String columnLabel, InputStream inputStream)");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateClob(int columnIndex, Reader reader)");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateClob(String columnLabel, Reader reader)");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateNClob(int columnIndex, Reader reader)");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException{
        handleUnsupportedMethod("updateNClob(String columnLabel, Reader reader)");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException{
        handleUnsupportedMethod("unwrap(Class<T> iface)");
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        handleUnsupportedMethod("isWrapperFor(Class<?> iface)");
        return false;
    }

	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static void log(String infoMessage){
		log(Level.INFO, infoMessage);
	}
	
	private static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCResultSet.class.getSimpleName()+": "+infoMessage);
	}
	
	private void handleUnsupportedMethod(String methodSignature) throws SQLException {
		logger.log(Level.SEVERE, methodSignature+" is not supported yet.");
        throw new UnsupportedOperationException();
	}    
}
