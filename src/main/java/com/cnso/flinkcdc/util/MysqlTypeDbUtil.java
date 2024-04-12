package com.cnso.flinkcdc.util;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlTypeDbUtil {
    public static BasicDataSource init(String driver, String url, String user, String pwd) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setInitialSize(40);
        dataSource.setMinIdle(10);
        dataSource.setMaxIdle(25);
        dataSource.setMaxTotal(50);

        dataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000);
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnCreate(true);
        dataSource.setTestWhileIdle(true);

        dataSource.setTimeBetweenEvictionRunsMillis(30 * 1000);
        dataSource.setValidationQuery("select 1");
        dataSource.setNumTestsPerEvictionRun(5);

        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(pwd);
        return dataSource;
    }

    public static void close(BasicDataSource dataSource) {
        try {
            dataSource.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn(BasicDataSource dataSource) throws SQLException {
        return dataSource.getConnection();
    }
}
