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
public class MiningExplorationServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_mining_exploration_tmp` (`id`,`_id`,`license_type`,`type`,`license_name`,`register_no`,`project_name`,`project_type`,`survey_unit`,`survey_mineral`,`coordinate`,`area`,`location`,`issue_unit`,`pub_date`,`valid_start`,`valid_end`,`state`,`state_code`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("license_type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("project_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("project_type", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("survey_unit", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("survey_mineral", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("coordinate", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("issue_unit", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("pub_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("state_code", datum));
        PreparedStatementUtils.setInt(20, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(21, datum.get("create_time"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
