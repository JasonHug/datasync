package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class WholeParkServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_whole_park_tmp` (`id`,`name`,`self_id`,`province`,`city`,`district`,`district_code`,`area`,`description`,`ploy`,`url`,`address`,`industry_collect`,`longitude`,`latitude`,`u_tags`,`is_history`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id").toString(),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("self_id", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("city", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("district", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("district_code", datum));
        PreparedStatementUtils.setLong(8, datum.get("area").toString(),ps);
        ps.setString(9, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("ploy", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("industry_collect", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("longitude", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("latitude", datum));
        PreparedStatementUtils.setInt(16, datum.get("u_tags").toString(),ps);
        PreparedStatementUtils.setInt(17, datum.get("is_history").toString(),ps);
        PreparedStatementUtils.setLong(18, datum.get("created_time").toString(),ps);
        ps.setTimestamp(19, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(20, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
