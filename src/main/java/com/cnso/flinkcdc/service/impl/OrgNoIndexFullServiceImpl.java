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
public class OrgNoIndexFullServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_org_no_index_full_tmp` (`zz_id`,`id`,`org_no`,`eid`,`zz_eid`,`province`,`credit_no`,`name`,`reg_no`,`status`,`oper_name`,`start_date`,`history_names`,`his_eids`,`data`,`number`,`update_time`,`row_update_time`,`local_row_update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("org_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("history_names", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("his_eids", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("data", datum));
        PreparedStatementUtils.setLong(16, datum.get("number"),ps);
        PreparedStatementUtils.setLong(17, datum.get("update_time"),ps);
        ps.setTimestamp(18, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
