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
public class DoubleCheckupsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_double_checkups_tmp` (`id`,`eid`,`zz_eid`,`raninsPlanId`,`raninsPlaneName`,`raninsTaskId`,`raninsTaskName`,`raninsTypeName`,`insAuth`,`insDate`,`details`,`is_history`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("raninsPlanId", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("raninsPlaneName", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("raninsTaskId", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("raninsTaskName", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("raninsTypeName", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("insAuth", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("insDate", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("details", datum));
        PreparedStatementUtils.setInt(12, datum.get("is_history"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
