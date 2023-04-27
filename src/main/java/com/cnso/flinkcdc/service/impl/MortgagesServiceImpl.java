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
public class MortgagesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_mortgages_tmp` (`id`,`eid`,`zz_eid`,`number`,`date`,`status`,`status_code`,`department`,`type`,`amount`,`amount_new`,`currency`,`period`,`period_start`,`period_end`,`scope`,`remarks`,`debit_type`,`debit_currency`,`debit_amount`,`debit_scope`,`debit_period`,`debit_remarks`,`close_date`,`close_reason`,`public_date`,`u_tags`,`mortgagees`,`guarantees`,`changeInfo`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("status", datum));
        PreparedStatementUtils.setInt(7, datum.get("status_code"),ps);
        ps.setString(8, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("amount", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("amount_new", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("currency", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("period", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("period_start", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("period_end", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("remarks", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("debit_type", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("debit_currency", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("debit_amount", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("debit_scope", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("debit_period", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("debit_remarks", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("close_date", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("close_reason", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("public_date", datum));
        PreparedStatementUtils.setInt(27, datum.get("u_tags"),ps);
        ps.setString(28, PreparedStatementUtils.getStringVal("mortgagees", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("guarantees", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("changeInfo", datum));
        ps.setTimestamp(31, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(32, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
