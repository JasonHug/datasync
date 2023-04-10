package com.cnso.flinkcdc.util;

import com.alibaba.fastjson.JSONObject;
import com.cnso.flinkcdc.deserialization.CustomerDeserialization;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.*;

public class Utils {

    public static MySqlSource<JSONObject> getMysqlSourceBuilder(ParameterTool config) {

        // 获取mysql-source连接相关配置信息
        String mysqlHost = config.get(SOURCE_MYSQL_HOST, null);
        int mysqlPort = config.getInt(SOURCE_MYSQL_PORT, -1);
        String mysqlUser = config.get(SOURCE_MYSQL_USERNAME, null);
        String mysqlPwd = config.get(SOURCE_MYSQL_PASSWORD, null);
        String mysqlDbs = config.get(SOURCE_MYSQL_DATABASES, null);
        String mysqlTbs = config.get(SOURCE_MYSQL_TABLES, null);
        String serverId = config.get(SOURCE_MYSQL_SERVER_ID, null);
        String startupOption = config.get(START_OPTION, null);
        Long startupOptionTimestamp = config.getLong(START_OPTION_TIMESTAMP, 1672394400000L);

        String specificOffsetFile = config.get(START_OFFSET_FILE);
        Long specificOffsetPos = config.getLong(START_OFFSET_POS, 294658239L);


        if (!Utils.isNotBlank(mysqlHost, mysqlUser, mysqlPwd, serverId) || !Utils.isNotBlank(mysqlPort)) {
            throw new RuntimeException("Please check parameter!!!");
        }

        MySqlSourceBuilder<JSONObject> builder = MySqlSource.<JSONObject>builder();
        builder.hostname(mysqlHost);
        builder.port(mysqlPort);
        builder.username(mysqlUser);
        builder.password(mysqlPwd);
        if (StringUtils.isNotBlank(mysqlDbs)) {
            String[] mysqlDb = mysqlDbs.split(",");
            builder.databaseList(mysqlDb);
        }
        if (StringUtils.isNotBlank(mysqlTbs)) {
            String[] mysqlTb = mysqlTbs.split(",");
            builder.tableList(mysqlTb);
        }
        builder.serverId(serverId);

        switch (startupOption) {
            case "earliest":
                builder.startupOptions(StartupOptions.earliest());
                break;
            case "initial":
                builder.startupOptions(StartupOptions.initial());
                break;
            case "latest":
                builder.startupOptions(StartupOptions.latest());
                break;
            case "timestamp":
                builder.startupOptions(StartupOptions.timestamp(startupOptionTimestamp));
                break;
            case "specific":
                builder.startupOptions(StartupOptions.specificOffset(specificOffsetFile, specificOffsetPos));
                break;
            default:
                throw new IllegalStateException("startup option exception!!!");
        }

        builder.deserializer(new CustomerDeserialization());
        builder.serverTimeZone("Asia/Shanghai");
        builder.scanNewlyAddedTableEnabled(true);
        builder.connectTimeout(Duration.ofHours(1));

        return builder.build();
    }

    private static boolean isNotBlank(String... param) {

        for (int i = 0; i < param.length; i++) {
            String item = param[i];
            if (StringUtils.isBlank(item)) {
                return false;
            }
        }

        return true;
    }

    private static boolean isNotBlank(int... param) {

        for (int i = 0; i < param.length; i++) {
            int item = param[i];
            if (item == -1) {
                return false;
            }
        }

        return true;
    }

}
