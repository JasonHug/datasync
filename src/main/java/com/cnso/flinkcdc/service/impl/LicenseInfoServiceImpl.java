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
public class LicenseInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_license_info_tmp` (`id`,`eid`,`zz_eid`,`u_id`,`number`,`start_date`,`end_date`,`department`,`name`,`content`,`status`,`status_code`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("u_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("status", datum));
        PreparedStatementUtils.setInt(12, datum.get("status_code"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
