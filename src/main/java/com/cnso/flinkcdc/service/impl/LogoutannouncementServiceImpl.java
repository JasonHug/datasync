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
public class LogoutannouncementServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_logoutannouncement_tmp` (`id`,`eid`,`zz_eid`,`ename`,`credit_no`,`belong_org`,`audit_reg_date`,`audit_start_date`,`logout_reason`,`audit_address`,`audit_leader`,`audit_employees`,`creditor_start_date`,`creditor_end_date`,`creditor_announcement`,`creditor_person`,`creditor_address`,`cancel_date`,`status`,`is_history`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("belong_org", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("audit_reg_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("audit_start_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("logout_reason", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("audit_address", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("audit_leader", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("audit_employees", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("creditor_start_date", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("creditor_end_date", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("creditor_announcement", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("creditor_person", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("creditor_address", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("cancel_date", datum));
        PreparedStatementUtils.setInt(19, datum.get("status"),ps);
        PreparedStatementUtils.setInt(20, datum.get("is_history"),ps);
        ps.setTimestamp(21, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
