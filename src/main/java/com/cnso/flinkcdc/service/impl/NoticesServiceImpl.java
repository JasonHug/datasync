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
public class NoticesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_notices_tmp` (`_id`,`ex_id`,`type`,`type_code`,`type_name`,`people`,`people_ori`,`content`,`content_ori`,`date`,`court`,`url`,`related_companies_ori`,`case_reason`,`relation_details`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("ex_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        PreparedStatementUtils.setInt(4, datum.get("type_code"),ps);
        ps.setString(5, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("people", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("people_ori", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("content_ori", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("related_companies_ori", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("case_reason", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("relation_details", datum));
        PreparedStatementUtils.setLong(16, datum.get("created_time"),ps);
        ps.setTimestamp(17, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(18, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
