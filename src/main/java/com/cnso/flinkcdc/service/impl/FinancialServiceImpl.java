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
public class FinancialServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_financial_tmp` (`id`,`_id`,`type`,`license_name`,`institution_type_first`,`institution_type_second`,`institution_type_third`,`institution_level`,`regulator_first`,`regulator_second`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`abbreviation`,`ename_en`,`address`,`location`,`postal_code`,`establish_date`,`issue_organ`,`serial_number`,`business_scope`,`change_before_info`,`related_companies`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("institution_type_first", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("institution_type_second", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("institution_type_third", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("institution_level", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("regulator_first", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("regulator_second", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("abbreviation", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("ename_en", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("postal_code", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("establish_date", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("issue_organ", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("serial_number", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("business_scope", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("change_before_info", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("related_companies", datum));
        PreparedStatementUtils.setInt(27, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(28, datum.get("create_time"),ps);
        ps.setTimestamp(29, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(30, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
