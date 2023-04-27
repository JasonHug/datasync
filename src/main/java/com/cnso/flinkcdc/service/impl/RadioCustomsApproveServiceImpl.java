package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class RadioCustomsApproveServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_radio_customs_approve_tmp` (`id`,`_id`,`md5`,`type`,`license_name`,`register_no`,`country_from`,`customs_arrive`,`manufacturer`,`product_name`,`model`,`frequency_range`,`transmit_power`,`unit`,`number`,`model_check_num`,`issue_date`,`note`,`valid_end`,`state`,`state_code`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("md5", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("country_from", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("customs_arrive", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("manufacturer", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("model", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("frequency_range", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("transmit_power", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("unit", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("model_check_num", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("issue_date", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("note", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("state_code", datum));
        PreparedStatementUtils.setInt(22, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(23, datum.get("create_time"),ps);
        ps.setTimestamp(24, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(25, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
