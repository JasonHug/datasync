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
public class SurveyingServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_surveying_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`name`,`valid_start`,`valid_end`,`state`,`state_code`,`location`,`qualification_level`,`scope`,`oper_name`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("qualification_level", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("oper_name", datum));
        PreparedStatementUtils.setInt(15, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(16, datum.get("create_time"),ps);
        ps.setTimestamp(17, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(18, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
