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
public class AdminDivisionCodeChangeHistoryServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_admin_division_code_change_history_tmp` (`id`,`qdu_id`,`before_admin_name`,`after_admin_name`,`before_type_code`,`after_type_code`,`change_year`,`qds`,`is_history`,`u_tags`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("qdu_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("before_admin_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("after_admin_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("before_type_code", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("after_type_code", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("change_year", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("qds", datum));
        PreparedStatementUtils.setInt(9, datum.get("is_history"),ps);
        PreparedStatementUtils.setInt(10, datum.get("u_tags"),ps);
        ps.setTimestamp(11, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
