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
public class ExecutedpersonsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_executedpersons_tmp` (`_id`,`eid`,`zz_eid`,`name`,`ename`,`case_date`,`case_number`,`type`,`case_id`,`amount`,`court`,`status`,`created_time`,`row_update_time`,`pid`,`p_eid`,`zz_p_eid`,`p_ename`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        PreparedStatementUtils.setDate(6, datum.get("case_date"), ps);
        ps.setString(7, PreparedStatementUtils.getStringVal("case_number", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("case_id", datum));
        PreparedStatementUtils.setLong(10, datum.get("amount"),ps);
        ps.setString(11, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("status", datum));
        PreparedStatementUtils.setLong(13, datum.get("created_time"),ps);
        ps.setTimestamp(14, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(15, PreparedStatementUtils.getStringVal("pid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("p_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_p_eid", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("p_ename", datum));
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
