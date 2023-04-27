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
public class ComputerProjectManagerServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_computer_project_manager_tmp` (`id`,`_id`,`name`,`type`,`license_name`,`level`,`register_no`,`check_date`,`valid_start`,`state`,`state_code`,`location`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("level", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("check_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("location", datum));
        PreparedStatementUtils.setInt(13, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(14, datum.get("create_time"),ps);
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(16, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
