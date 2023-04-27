package com.cnso.flinkcdc.util;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DRIVER;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_USERNAME;

public class TiDBUtil {

    private static BasicDataSource dataSource = null;

    public static void init(String driver, String url, String user, String pwd) throws SQLException {
        if (null == dataSource){
            dataSource = new BasicDataSource();
            dataSource.setInitialSize(1);
            dataSource.setMinIdle(3);
            dataSource.setMaxIdle(5);
            dataSource.setMaxTotal(5);

            dataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000);
            dataSource.setTestOnBorrow(false);
            dataSource.setTestOnCreate(false);
            dataSource.setTestWhileIdle(true);
            dataSource.setTimeBetweenEvictionRunsMillis(30 * 1000);
            dataSource.setValidationQuery("select 1");
            dataSource.setNumTestsPerEvictionRun(5);

            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(pwd);
        }
    }

    public static void close() {
        try {
            dataSource.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn() throws SQLException {
        return dataSource.getConnection();
    }

}
