package com.cnso.flinkcdc.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

/**
 * @author lihao
 * @date 2023/4/14
 */
public class PreparedStatementUtils {

    public static String getStringVal(String key, Map<String, Object> map){
        Object o = map.get(key);
        if (null == o){
            return null;
        }else {
            return o.toString();
        }
    }

    public static void setBoolean(int parameterIndex, Boolean x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setBoolean(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.BOOLEAN);
        }
    };


    public static void setByte(int parameterIndex, Byte x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setByte(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.BIT);
        }
    }


    public static void setShort(int parameterIndex, Short x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setShort(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.SMALLINT);
        }
    }


    public static void setInt(int parameterIndex, Integer x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setInt(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.INTEGER);
        }
    }

    public static void setInt(int parameterIndex, Object x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setInt(parameterIndex,new Integer(x.toString()));
        }else{
            preparedStatement.setNull(parameterIndex, Types.INTEGER);
        }
    }


    public static void setLong(int parameterIndex, Long x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setLong(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.BIGINT);
        }
    }

    public static void setLong(int parameterIndex, Object x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setLong(parameterIndex,new Long(x.toString()));
        }else{
            preparedStatement.setNull(parameterIndex, Types.BIGINT);
        }
    }


    public static void setFloat(int parameterIndex, Float x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setFloat(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.FLOAT);
        }
    }

    public static void setDouble(int parameterIndex, Object x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setDouble(parameterIndex,new Double(x.toString()));
        }else{
            preparedStatement.setNull(parameterIndex, Types.DOUBLE);
        }
    }

    public static void setDouble(int parameterIndex, Double x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setDouble(parameterIndex,x);
        }else{
            preparedStatement.setNull(parameterIndex, Types.DOUBLE);
        }
    }

    public static void setDate(int parameterIndex, Object x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setDate(parameterIndex,new Date(TableUtils.getDateLong(x.toString())));
        }else{
            preparedStatement.setNull(parameterIndex, Types.DOUBLE);
        }
    }

    public static void setBigDecimal(int parameterIndex, Object x, PreparedStatement preparedStatement) throws SQLException{
        if(x != null){
            preparedStatement.setBigDecimal(parameterIndex,new BigDecimal(x.toString()));
        }else{
            preparedStatement.setNull(parameterIndex, Types.DECIMAL);
        }
    }
}
