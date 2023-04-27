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
public class WholeParkRelationServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_whole_park_relation_tmp` (`id`,`name`,`self_id`,`eid`,`zz_eid`,`company_name`,`u_tags`,`is_history`,`last_updated_time`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id").toString(),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("self_id", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("company_name", datum));
        PreparedStatementUtils.setInt(7, datum.get("u_tags").toString(),ps);
        PreparedStatementUtils.setInt(8, datum.get("is_history").toString(),ps);
        PreparedStatementUtils.setLong(9, datum.get("last_updated_time").toString(),ps);
        PreparedStatementUtils.setLong(10, datum.get("created_time").toString(),ps);
        ps.setTimestamp(11, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(12, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
