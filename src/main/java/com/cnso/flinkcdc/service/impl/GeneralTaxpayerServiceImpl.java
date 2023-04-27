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
public class GeneralTaxpayerServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_general_taxpayer_tmp` (`md5`,`name`,`eid`,`zz_eid`,`tax_num`,`qualification`,`area`,`judge_date`,`start_date`,`end_date`,`management_range`,`manage_organ`,`has_qualific`,`legal_person`,`state`,`register_type`,`duty`,`credit_code`,`register_addre`,`tax_manager`,`filing_status`,`has_quarterly`,`created_time`,`row_update_time`,`common_taxpayer`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("md5", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("tax_num", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("qualification", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("judge_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("management_range", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("manage_organ", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("has_qualific", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("legal_person", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("register_type", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("duty", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("credit_code", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("register_addre", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("tax_manager", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("filing_status", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("has_quarterly", datum));
        PreparedStatementUtils.setLong(23, datum.get("created_time"),ps);
        ps.setTimestamp(24, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        PreparedStatementUtils.setInt(25, datum.get("common_taxpayer"),ps);
        PreparedStatementUtils.setInt(26, datum.get("is_history"),ps);
        ps.setTimestamp(27, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
