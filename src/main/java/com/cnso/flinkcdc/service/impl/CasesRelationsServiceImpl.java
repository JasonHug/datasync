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
public class CasesRelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_cases_relations_tmp` (`id`,`eid`,`zz_eid`,`obj_id`,`ename`,`role`,`type`,`court`,`start_date`,`end_date`,`assistant`,`region`,`case_no`,`agent`,`hearing_date`,`related_companies`,`case_status`,`order_date`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("role", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("assistant", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("region", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("agent", datum));
        PreparedStatementUtils.setDate(15, datum.get("hearing_date"), ps);
        ps.setString(16, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("case_status", datum));
        PreparedStatementUtils.setDate(18, datum.get("order_date"), ps);
        PreparedStatementUtils.setLong(19, datum.get("created_time"),ps);
        ps.setTimestamp(20, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(21, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
