package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class AdministrativePunishmentServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_administrative_punishment_tmp` (`id`,`md5_id`,`eid`,`zz_eid`,`name`,`number`,`illegal_type`,`content`,`department`,`date`,`reg_no`,`oper_name`,`punishment_text`,`based_on`,`remark`,`public_date`,`credit_no`,`org_no`,`tax_no`,`push_type_one`,`push_type_two`,`reason`,`end_date`,`document`,`postcode`,`updated_time`,`status`,`area`,`created_time`,`description`,`row_update_time`,`relief_channel`,`public_scope`,`pdf_url`,`pdf_content`,`is_gs`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("md5_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("illegal_type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("department", datum));
        PreparedStatementUtils.setDate(10, datum.get("date"), ps);
        ps.setString(11, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("punishment_text", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("based_on", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("remark", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("public_date", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("org_no", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("tax_no", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("push_type_one", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("push_type_two", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("reason", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("document", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("postcode", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("updated_time", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("created_time", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("description", datum));
        ps.setTimestamp(31, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(32, PreparedStatementUtils.getStringVal("relief_channel", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("public_scope", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("pdf_url", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("pdf_content", datum));
        PreparedStatementUtils.setInt(36, datum.get("is_gs"),ps);
        ps.setTimestamp(37, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
