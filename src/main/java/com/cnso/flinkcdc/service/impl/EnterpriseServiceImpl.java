package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class EnterpriseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_enterprise_tmp` (`zz_eid`,`eid`,`id`,`reg_no`,`credit_no`,`org_no`,`name`,`format_name`,`econ_kind`,`regist_capi`,`actual_capi`,`scope`,`term_start`,`term_end`,`check_date`,`belong_org`,`oper_name`,`oper_type`,`oper_name_id`,`start_date`,`status`,`title`,`longitude`,`latitude`,`gd_longitude`,`gd_latitude`,`collegues_num`,`created_time`,`logo_url`,`econ_type`,`department`,`url`,`row_update_time`,`province_code`,`district_code`,`title_code`,`econ_kind_code`,`regist_capi_new`,`currency_unit`,`revoke_reason`,`revoke_date`,`logout_reason`,`logout_date`,`revoked_certificates`,`new_status_code`,`type_new`,`category_new`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        PreparedStatementUtils.setLong(3, datum.get("id").toString(),ps);
        ps.setString(4, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("org_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("format_name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("econ_kind", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("regist_capi", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("actual_capi", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("term_start", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("term_end", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("check_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("belong_org", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("oper_type", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("oper_name_id", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("title", datum));
        PreparedStatementUtils.setDouble(23, datum.get("longitude").toString(), ps);
        PreparedStatementUtils.setDouble(24, datum.get("latitude").toString(), ps);
        PreparedStatementUtils.setDouble(25, datum.get("gd_longitude").toString(), ps);
        PreparedStatementUtils.setDouble(26, datum.get("gd_latitude").toString(), ps);
        ps.setString(27, PreparedStatementUtils.getStringVal("collegues_num", datum));
        PreparedStatementUtils.setLong(28, datum.get("created_time").toString(),ps);
        ps.setString(29, PreparedStatementUtils.getStringVal("logo_url", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("econ_type", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("url", datum));
        ps.setTimestamp(33, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(34, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("district_code", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("title_code", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("econ_kind_code", datum));
        ps.setBigDecimal(38, new BigDecimal(datum.get("regist_capi_new").toString()));
        ps.setString(39, PreparedStatementUtils.getStringVal("currency_unit", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("revoke_reason", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("revoke_date", datum));
        ps.setString(42, PreparedStatementUtils.getStringVal("logout_reason", datum));
        ps.setString(43, PreparedStatementUtils.getStringVal("logout_date", datum));
        ps.setString(44, PreparedStatementUtils.getStringVal("revoked_certificates", datum));
        ps.setString(45, PreparedStatementUtils.getStringVal("new_status_code", datum));
        ps.setString(46, PreparedStatementUtils.getStringVal("type_new", datum));
        ps.setString(47, PreparedStatementUtils.getStringVal("category_new", datum));
        ps.setTimestamp(48, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
