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
public class SewagePermitServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_sewage_permit_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`province`,`province_code`,`address`,`industry`,`area`,`issue_unit`,`steps`,`main_pollutants`,`atmosphere_pollutants_kind`,`atmosphere_pollutants_law`,`atmosphere_pollutants_standard`,`water_pollutants_kind`,`water_pollutants_law`,`water_pollutants_standard`,`use_right`,`report`,`supervise_info`,`self_monitor_info`,`pic1_url_ori`,`pic1_url_oss`,`pic2_url_ori`,`pic2_url_oss`,`reason`,`date`,`source`,`url`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("industry", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("issue_unit", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("steps", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("main_pollutants", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("atmosphere_pollutants_kind", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("atmosphere_pollutants_law", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("atmosphere_pollutants_standard", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("water_pollutants_kind", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("water_pollutants_law", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("water_pollutants_standard", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("use_right", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("report", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("supervise_info", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("self_monitor_info", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("pic1_url_ori", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("pic1_url_oss", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("pic2_url_ori", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("pic2_url_oss", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("reason", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("source", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(36, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(37, datum.get("create_time"),ps);
        ps.setTimestamp(38, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(39, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
