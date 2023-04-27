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
public class BestBriefServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_best_brief_tmp` (`id`,`eid`,`zz_eid`,`name`,`brief`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setTimestamp(6, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(7, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(8, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
