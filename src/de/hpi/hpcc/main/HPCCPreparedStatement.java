package de.hpi.hpcc.main;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Level;

public class HPCCPreparedStatement extends HPCCStatement implements PreparedStatement{

    protected static final String      className = "HPCCPreparedStatement";

    public HPCCPreparedStatement(HPCCConnection connection, String query)
    {
        super(connection);

        log("Constructor: Sqlquery: " + query);

        if (query != null)
            executeQuery(query);
    }

    public ResultSet executeQuery(){
        log("executeQuery()");
        executeHPCCQuery(parameters);

        return result;
    }
    
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        log(Level.FINEST, "setNull(" + parameterIndex + ", " + sqlType + " )");

        /*if( this.eclQuery.getQueryType() == SQLType.CALL)
            parameters.put(parameterIndex, "");
        else
            throw new SQLException("NULL cannot be represented in ECL.");*/
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException{
        log(Level.FINEST,"setBoolean(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException{
        log(Level.FINEST, "setByte(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException{
        log(Level.FINEST,"setShort(" + parameterIndex + ", " + x + ")");
        parameters.put(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException
    {
        log(Level.FINEST, "setInt(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException
    {
        log(Level.FINEST, "setLong(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        log(Level.FINEST, "setFloat(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        log(Level.FINEST, "setDouble(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        log(Level.FINEST, "setBigDecimal(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException
    {
        log(Level.FINEST, "setString(" + parameterIndex + ", " + x + " )");
        try
        {
            /*if( this.eclQuery.getQueryType() == SQLType.CALL)
                parameters.put(parameterIndex, x);
            else*/
                parameters.put(parameterIndex, HPCCJDBCUtils.ensureECLString(x));
        }
        catch (Exception e)
        {
            throw new SQLException("Cannot setString: " + e.getLocalizedMessage());
        }
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        log(Level.FINEST, "setBytes(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        log(Level.FINEST, "setDate(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) {
        log(Level.FINEST, "setTime(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        log(Level.FINEST, "setTimestamp(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void clearParameters() throws SQLException
    {
        log(Level.FINEST, "clearParameters( )");
        parameters.clear();
    }

    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        log(Level.FINEST, "setObject(" + parameterIndex + ", " + x + " )");

        if (x != null)
        {
            if (x instanceof String)
                setString(parameterIndex, (String) x);
            else if (x instanceof Boolean)
                setBoolean(parameterIndex, ((Boolean) x).booleanValue());
            else if (x instanceof Byte)
                setByte(parameterIndex, (Byte) x);
            else if (x instanceof Short)
                setShort(parameterIndex, ((Short) x).shortValue());
            else if (x instanceof Integer)
                setInt(parameterIndex, ((Integer) x).intValue());
            else if (x instanceof Long)
                setLong(parameterIndex, ((Long) x).longValue());
            else if (x instanceof Float)
                setFloat(parameterIndex, ((Float) x).floatValue());
            else if (x instanceof Double)
                setDouble(parameterIndex, ((Double) x).doubleValue());
            else if (x instanceof BigDecimal)
                setBigDecimal(parameterIndex, (BigDecimal) x);
            else if (x instanceof byte[])
                setBytes(parameterIndex, (byte[]) x);
            else if (x instanceof Time)
                setTime(parameterIndex, (Time) x);
            else if (x instanceof java.sql.Date)
                setDate(parameterIndex, (java.sql.Date) x);
            else if (x instanceof Timestamp)
                setTimestamp(parameterIndex, (Timestamp) x);
            else if (x instanceof InputStream)
                setBinaryStream(parameterIndex, (InputStream) x, -1);
            else
                parameters.put(parameterIndex, x);
        }
        else
        {
            setNull(parameterIndex, java.sql.Types.OTHER);
        }
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        log(Level.FINEST, "setRef(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        log(Level.FINEST, "setBlob(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        log(Level.FINEST, "setClob(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        log(Level.FINEST, "setArray(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        log(Level.FINEST, "getMetaData( )");
        return result != null ? result.getMetaData() : null;
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        log(Level.FINEST, "setNull(" + parameterIndex + ", " + sqlType + " )");
        setNull(parameterIndex, sqlType);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        log(Level.FINEST, "setURL(" + parameterIndex + ", " + x + " )");
        parameters.put(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        log(Level.FINEST, "setNString(" + parameterIndex + ", " + value + " )");
        parameters.put(parameterIndex, value);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        log(Level.FINEST, "setNClob(" + parameterIndex + ", " + value + " )");
        parameters.put(parameterIndex, value);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        log(Level.FINEST, "setSQLXML(" + parameterIndex + ", " + xmlObject + " )");
        parameters.put(parameterIndex, xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        log(Level.FINEST, "setObject(" + parameterIndex + ", " + x + ", " + targetSqlType + " )");
        if (x != null)
        {
            setObject(parameterIndex, x, targetSqlType, 0);
        }
        else
        {
            setNull(parameterIndex, targetSqlType);
        }
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        log(Level.FINEST, "setObject(" + parameterIndex + ", " + x + " )");

        String targetSqlTypeName = null;
        try
        {
            targetSqlTypeName = HPCCJDBCUtils.getSQLTypeName(targetSqlType);
        }
        catch (Exception e)
        {
            targetSqlTypeName = "java.sql.Types." + targetSqlType;
        }
        if (x != null)
        {
            try
            {
                Class<?> clazz = x.getClass();

                switch (targetSqlType)
                {
                    case java.sql.Types.CHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                        if (clazz.equals(String.class))
                           setString(parameterIndex, (String)x);
                        else
                           setString(parameterIndex, x.toString());
                        break;
                    case java.sql.Types.BIT:
                    case java.sql.Types.BOOLEAN:
                        if (clazz.equals(String.class))
                            setBoolean(parameterIndex, Boolean.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setBoolean(parameterIndex, (Integer)x <= 0 ? false : true);
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setBoolean(parameterIndex, (Boolean)x);
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setBoolean(parameterIndex, Boolean.valueOf(x.toString()));
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setBoolean(parameterIndex, ((Byte)x).intValue() <= 0 ? false : true);
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setBoolean(parameterIndex, ((Double)x) <= 0 ? false : true);
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setBoolean(parameterIndex, ((Long)x) <= 0 ? false : true);
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setBoolean(parameterIndex, ((Float)x) <= 0 ? false : true);
                        else if (clazz.equals(Character.TYPE))
                            throw new Exception();
                        break;
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                        if (clazz.equals(String.class))
                            setDouble(parameterIndex, java.lang.Double.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setDouble(parameterIndex, ((Integer)x).doubleValue());
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setDouble(parameterIndex, ((Boolean)x) == true ? 1.0 : 0.0);
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setDouble(parameterIndex, ((Byte)x).doubleValue());
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setDouble(parameterIndex, ((Short)x).doubleValue());
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setDouble(parameterIndex, (Double)x);
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setDouble(parameterIndex, ((Long)x).doubleValue());
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setDouble(parameterIndex, ((Float)x).doubleValue());
                        else if (clazz.equals(Character.TYPE))
                            throw new Exception();
                        break;

                    case java.sql.Types.NUMERIC:
                    case java.sql.Types.DECIMAL:
                    {
                        BigDecimal bd = null;
                        if (clazz.equals(String.class))
                            bd = new BigDecimal((String)x);
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            bd = new BigDecimal((Integer)x);
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            bd = new BigDecimal((Boolean)x == true ? 1 : 0);
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                            bd = new BigDecimal((Character)x);
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            bd = new BigDecimal((Byte)x);
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            bd = new BigDecimal((Short)x);
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            bd = new BigDecimal((Double)x);
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            bd = new BigDecimal((Long)x);
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            bd = new BigDecimal((Float)x);

                        if (bd != null)
                        {
                            if (scaleOrLength > 0)
                                bd.setScale(scaleOrLength);
                            setBigDecimal(parameterIndex, bd);
                        }
                        else
                            throw new Exception();
                        break;
                    }
                    case java.sql.Types.TINYINT:
                        if (clazz.equals(String.class))
                            setByte(parameterIndex,java.lang.Byte.parseByte((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setByte(parameterIndex, ((Integer)x).byteValue());
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setByte(parameterIndex, ((Boolean)x == true ? ((Integer)1).byteValue() : ((Integer)0).byteValue()));
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                            setByte(parameterIndex,java.lang.Byte.parseByte(x.toString()));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setByte(parameterIndex, (Byte)x);
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setByte(parameterIndex, ((Short)x).byteValue());
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setByte(parameterIndex, ((Double)x).byteValue());
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setByte(parameterIndex, ((Long)x).byteValue());
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setByte(parameterIndex, ((Float)x).byteValue());
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.SMALLINT:
                        if (clazz.equals(String.class))
                            setShort(parameterIndex, java.lang.Short.parseShort((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setShort(parameterIndex, ((Integer)x).shortValue());
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setShort(parameterIndex, ((Boolean)x == true ? ((Integer)1).shortValue() : ((Integer)0).shortValue()));
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                            setShort(parameterIndex, java.lang.Short.parseShort(x.toString()));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setShort(parameterIndex, ((Byte)x).shortValue());
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setShort(parameterIndex, (Short)x);
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setShort(parameterIndex, ((Double)x).shortValue());
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setShort(parameterIndex, ((Long)x).shortValue());
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setShort(parameterIndex, ((Float)x).shortValue());
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.INTEGER:
                        if (clazz.equals(String.class))
                            setInt(parameterIndex, java.lang.Integer.parseInt((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setInt(parameterIndex, ((Integer)x));
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setInt(parameterIndex, ((Boolean)x == true ? 1 : 0));
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                               setInt(parameterIndex, java.lang.Integer.parseInt((String)x));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setInt(parameterIndex, ((Byte)x).intValue());
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setInt(parameterIndex, ((Short)x).intValue());
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setInt(parameterIndex, ((Double)x).intValue());
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setInt(parameterIndex, ((Long)x).intValue());
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setInt(parameterIndex, ((Float)x).intValue());
                        else
                            throw new Exception();
                        break;

                    case java.sql.Types.BIGINT:
                        Long l = null;
                        if (clazz.equals(String.class))
                        {
                            if (scaleOrLength > 0)
                                l = java.lang.Long.parseLong((String)x, scaleOrLength);
                            else
                                l = java.lang.Long.parseLong((String)x);
                        }
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            l = ((Integer)x).longValue();
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            l = ((Boolean)x == true ? ((Integer)1).longValue() : ((Integer)0).longValue());
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                        {
                            if (scaleOrLength > 0)
                                l = java.lang.Long.parseLong(x.toString(), scaleOrLength);
                            else
                                l = java.lang.Long.parseLong(x.toString());
                        }
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            l = ((Byte)x).longValue();
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            l = ((Short)x).longValue();
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            l = ((Double)x).longValue();
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            l = ((Long)x).longValue();
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            l = ((Float)x).longValue();

                        if (l != null)
                        {
                            setLong(parameterIndex, l);
                        }
                        else
                            throw new Exception();

                        break;
                    case java.sql.Types.REAL:
                        if (clazz.equals(String.class))
                            setFloat(parameterIndex, java.lang.Float.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setFloat(parameterIndex, ((Integer)x).floatValue());
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setFloat(parameterIndex, ((Boolean)x == true ? ((Integer)1).floatValue() : ((Integer)0).floatValue()));
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                            setFloat(parameterIndex, java.lang.Float.valueOf((x.toString())));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setFloat(parameterIndex, ((Byte)x).floatValue());
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setFloat(parameterIndex, ((Short)x).floatValue());
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setFloat(parameterIndex, ((Double)x).floatValue());
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setFloat(parameterIndex, (Long)x);
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setFloat(parameterIndex, ((Float)x).floatValue());
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.LONGVARBINARY:
                        if (clazz.equals(String.class))
                            setBytes(parameterIndex, ((String) x).getBytes());
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setBytes(parameterIndex, (((Integer)x).toString()).getBytes());
                        else if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE))
                            setBytes(parameterIndex, ((Boolean)x == true ? "1".getBytes() : "0".getBytes()));
                        else if (clazz.equals(Character.class) || clazz.equals(Character.TYPE))
                            setBytes(parameterIndex, x.toString().getBytes());
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setBytes(parameterIndex, (byte [])x);
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setBytes(parameterIndex, ((Short) x).toString().getBytes());
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setBytes(parameterIndex, ((Double)x).toString().getBytes());
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setBytes(parameterIndex, ((Long)x).toString().getBytes());
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setBytes(parameterIndex, ((Float)x).toString().getBytes());
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.DATE:
                        if (clazz.equals(String.class))
                            setDate(parameterIndex,java.sql.Date.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Integer)x).longValue()));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Byte)x).longValue()));
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Short)x).longValue()));
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Double)x).longValue()));
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Long)x)));
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setDate(parameterIndex, new java.sql.Date(((Float)x).longValue()));
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.TIME:
                        if (clazz.equals(String.class))
                            setTime(parameterIndex, java.sql.Time.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Integer)x).longValue()));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Byte)x).longValue()));
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Short)x).longValue()));
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Double)x).longValue()));
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Long)x)));
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setTime(parameterIndex, new java.sql.Time(((Float)x).longValue()));
                        else
                            throw new Exception();
                        break;
                    case java.sql.Types.TIMESTAMP:
                        if (clazz.equals(String.class))
                            setTimestamp(parameterIndex, java.sql.Timestamp.valueOf((String)x));
                        else if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Integer)x).longValue()));
                        else if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Byte)x).longValue()));
                        else if (clazz.equals(Short.class) || clazz.equals(Short.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Short)x).longValue()));
                        else if (clazz.equals(Double.class) || clazz.equals(Double.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Double)x).longValue()));
                        else if (clazz.equals(Long.class) || clazz.equals(Long.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Long)x)));
                        else if (clazz.equals(Float.class) || clazz.equals(Float.TYPE))
                            setTimestamp(parameterIndex, new java.sql.Timestamp(((Float)x).longValue()));
                        else
                            throw new Exception();
                        break;
                    default:
                        throw new Exception();
                }
            }
            catch (Exception e)
            {
                throw new SQLException("Cannot convert " + x.toString() + "(" + x.getClass() + ") to: " + targetSqlTypeName);
            }
        }
        else
        {
            setNull(parameterIndex, targetSqlType);
        }
    }
    
    //Override super method
    protected static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCPreparedStatement.class.getSimpleName()+": "+infoMessage);
	}
    
    //Unsuppported methods!!
    public void addBatch() throws SQLException {
        handleUnsupportedMethod("addBatch()");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader, int length)");
    }
    
    public int executeUpdate() throws SQLException{
        handleUnsupportedMethod("executeUpdate()");
		return 0;
    }
    
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x, int length)");
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setUnicodeStream(int parameterIndex, InputStream x, int length)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x, int length)");
    }
    
    public void setDate(int parameterIndex, Date x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setDate(int parameterIndex, Date x, Calendar calendar)");
    }

    public void setTime(int parameterIndex, Time x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setTime(int parameterIndex, Time x, Calendar calendar)");
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar calendar) throws SQLException {
        handleUnsupportedMethod("setTimestamp(int parameterIndex, Timestamp x, Calendar calendar)");
    }
    
    public ParameterMetaData getParameterMetaData() throws SQLException {
        handleUnsupportedMethod("getParameterMetaData()");
		return null;
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        handleUnsupportedMethod("setRowId(int parameterIndex, RowId x)");
    }
    
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        handleUnsupportedMethod("setNCharacterStream(int parameterIndex, Reader value, long length)");
    }
    
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setClob(int parameterIndex, Reader reader, long length)");
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        handleUnsupportedMethod("setBlob(int parameterIndex, InputStream inputStream, long length)");
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setNClob(int parameterIndex, Reader reader, long length)");
    }
    
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x, long length)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x, long length)");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader, long length)");
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        handleUnsupportedMethod("setAsciiStream(int parameterIndex, InputStream x)");
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        handleUnsupportedMethod("setBinaryStream(int parameterIndex, InputStream x)");
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setCharacterStream(int parameterIndex, Reader reader)");
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        handleUnsupportedMethod("setNCharacterStream(int parameterIndex, Reader value)");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setClob Not supported yet.");
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        handleUnsupportedMethod("setBlob Not supported yet.");
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        handleUnsupportedMethod("setNClob Not supported yet.");
    }
}
