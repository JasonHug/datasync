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
public class ElectronicCertificationAgencyServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_electronic_certification_agency_tmp` (`id`,`_id`,`type`,`license_name`,`province`,`province_code`,`register_no`,`valid_start`,`state`,`state_code`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("state_code", datum));
        PreparedStatementUtils.setInt(11, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(12, datum.get("create_time"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
