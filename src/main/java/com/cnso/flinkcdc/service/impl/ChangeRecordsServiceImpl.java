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
public class ChangeRecordsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_change_records_tmp` (`id`,`u_id`,`eid`,`zz_eid`,`after_content`,`before_content`,`change_date`,`change_item`,`type`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("u_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("after_content", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("before_content", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("change_item", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("type", datum));
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(11, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
