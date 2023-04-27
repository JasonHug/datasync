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
public class AdminDivisionCodeServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_admin_division_code_tmp` (`id`,`type_code`,`admin_name`,`short_name`,`series`,`zip_code`,`last_update_time`,`row_update_time`,`CityCode`,`mca_code`,`nbs_code`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("type_code", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("admin_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("short_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("series", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zip_code", datum));
        PreparedStatementUtils.setLong(7, datum.get("last_update_time"),ps);
        ps.setTimestamp(8, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(9, PreparedStatementUtils.getStringVal("CityCode", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("mca_code", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("nbs_code", datum));
        PreparedStatementUtils.setInt(12, datum.get("is_history"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
