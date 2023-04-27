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
public class MedicalInstrumentsRegisteredServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_medical_instruments_registered_tmp` (`id`,`_id`,`register_no`,`type`,`license_name`,`kind`,`new_or_history`,`address`,`production_address`,`agency_name`,`agency_eid`,`zz_agency_eid`,`agency_address`,`name`,`model`,`structure`,`description`,`manage_type`,`scope`,`other`,`remarks`,`attention`,`date`,`valid_start`,`valid_end`,`state`,`state_code`,`attach`,`standard`,`change_date`,`zipcode`,`components`,`intended_use`,`storage`,`department`,`change_records`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("kind", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("new_or_history", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("production_address", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("agency_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("agency_eid", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("zz_agency_eid", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("agency_address", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("model", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("structure", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("manage_type", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("other", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("remarks", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("attention", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("attach", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("standard", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("zipcode", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("components", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("intended_use", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("storage", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("change_records", datum));
        PreparedStatementUtils.setInt(37, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(38, datum.get("create_time"),ps);
        ps.setTimestamp(39, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(40, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
