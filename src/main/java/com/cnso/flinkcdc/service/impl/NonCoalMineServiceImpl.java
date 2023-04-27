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
public class NonCoalMineServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_non_coal_mine_tmp` (`id`,`_id`,`type`,`manufacture`,`license_name`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`decision_date`,`license_scope`,`mining_method`,`issue_date`,`department`,`related_companies`,`qds`,`url`,`u_tags`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("manufacture", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("decision_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("license_scope", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("mining_method", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("issue_date", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("qds", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(19, datum.get("u_tags"),ps);
        PreparedStatementUtils.setInt(20, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(21, datum.get("create_time"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
