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
public class SimpleCancellationAnnouncementServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_simple_cancellation_announcement_tmp` (`id`,`eid`,`zz_eid`,`name`,`credit_reg_no`,`department`,`notice_period`,`gs_sca_objections`,`gs_sca_result`,`row_update_time`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("credit_reg_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("notice_period", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("gs_sca_objections", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("gs_sca_result", datum));
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        PreparedStatementUtils.setInt(11, datum.get("is_history"),ps);
        ps.setTimestamp(12, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
