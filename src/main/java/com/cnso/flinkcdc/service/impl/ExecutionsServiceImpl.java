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
public class ExecutionsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_executions_tmp` (`_id`,`name`,`oper_name`,`sex`,`age`,`court`,`type`,`province`,`doc_number`,`date`,`case_number`,`ex_department`,`final_duty`,`execution_status`,`execution_desc`,`publish_date`,`related_companies`,`amount`,`eid`,`zz_eid`,`last_update_time`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("sex", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("age", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("doc_number", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("case_number", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("ex_department", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("final_duty", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("execution_status", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("execution_desc", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("publish_date", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("related_companies", datum));
        PreparedStatementUtils.setDouble(18, datum.get("amount"), ps);
        ps.setString(19, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("zz_eid", datum));
        PreparedStatementUtils.setLong(21, datum.get("last_update_time"),ps);
        PreparedStatementUtils.setLong(22, datum.get("created_time"),ps);
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
