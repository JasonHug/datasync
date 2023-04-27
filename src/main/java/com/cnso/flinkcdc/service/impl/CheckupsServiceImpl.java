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
public class CheckupsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_checkups_tmp` (`id`,`eid`,`zz_eid`,`name`,`reg_no`,`province`,`department`,`type`,`date`,`result`,`last_update_time`,`row_update_time`,`result_type`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("result", datum));
        PreparedStatementUtils.setLong(11, datum.get("last_update_time"),ps);
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        PreparedStatementUtils.setInt(13, datum.get("result_type"),ps);
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
