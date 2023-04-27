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
public class ReportDetailsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_report_details_tmp` (`zz_id`,`eid`,`zz_eid`,`report_year`,`reg_no`,`credit_no`,`name`,`report_name`,`report_date`,`net_amount`,`debit_amount`,`prac_person_num`,`telephone`,`email`,`sale_income`,`profit_total`,`profit_reta`,`tax_total`,`total_equity`,`address`,`fare_scope`,`oper_name`,`zip_code`,`serv_fare_income`,`reg_capi`,`indiv_form_mode`,`collegues_num`,`if_website`,`if_invest`,`if_external_guarantee`,`if_equity`,`create_date`,`status`,`female_collegues_num`,`enterprise_holding_situation`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("report_year", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("report_name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("report_date", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("net_amount", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("debit_amount", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("prac_person_num", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("telephone", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("email", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("sale_income", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("profit_total", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("profit_reta", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("tax_total", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("total_equity", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("fare_scope", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("zip_code", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("serv_fare_income", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("reg_capi", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("indiv_form_mode", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("collegues_num", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("if_website", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("if_invest", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("if_external_guarantee", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("if_equity", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("create_date", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("female_collegues_num", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("enterprise_holding_situation", datum));
        ps.setTimestamp(36, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(37, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
