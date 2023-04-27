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
public class KaitinggonggaosRelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_kaitinggonggaos_relations_tmp` (`id`,`eid`,`zz_eid`,`obj_id`,`pure_role`,`cause_action`,`case_no`,`title`,`content`,`court`,`department`,`hearing_date`,`judge`,`tribunal`,`url`,`ename`,`related_companies`,`month_date`,`year_date`,`video_url`,`court_code`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("pure_role", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("cause_action", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("department", datum));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("hearing_date").toString())));
        ps.setString(13, PreparedStatementUtils.getStringVal("judge", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("tribunal", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("month_date", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("year_date", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("video_url", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("court_code", datum));
        PreparedStatementUtils.setLong(22, datum.get("created_time"),ps);
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
