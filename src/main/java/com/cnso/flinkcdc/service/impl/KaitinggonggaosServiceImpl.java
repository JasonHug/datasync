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
public class KaitinggonggaosServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_kaitinggonggaos_tmp` (`_id`,`release_time`,`case_reason`,`hearing_date`,`department`,`court`,`related_companies`,`case_no`,`content`,`author`,`tribunal`,`judge`,`title`,`url`,`region`,`cause_action`,`u_tags`,`code_source`,`video_url`,`court_code`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("release_time", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("case_reason", datum));
        ps.setTimestamp(4, new Timestamp(TableUtils.getLong(datum.get("hearing_date").toString())));
        ps.setString(5, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("author", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("tribunal", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("judge", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("region", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("cause_action", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("u_tags", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("code_source", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("video_url", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("court_code", datum));
        PreparedStatementUtils.setLong(21, datum.get("created_time"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
