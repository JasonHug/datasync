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
public class MedicalInstrumentsImportServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_medical_instruments_import_tmp` (`id`,`_id`,`register_no`,`kind`,`new_or_history`,`type`,`license_name`,`address`,`production_address`,`reg_ename`,`reg_eid`,`zz_reg_eid`,`agency_address`,`name`,`description`,`manage_type`,`model`,`structure`,`scope`,`country`,`attach`,`other`,`attention`,`remarks`,`valid_start`,`valid_end`,`state`,`state_code`,`ename_cn`,`name_cn`,`standard`,`country_cn`,`service_name`,`service_eid`,`zz_service_eid`,`change_date`,`components`,`intended_use`,`storage`,`department`,`change_records`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("kind", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("new_or_history", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("production_address", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("reg_ename", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("reg_eid", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("zz_reg_eid", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("agency_address", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("manage_type", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("model", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("structure", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("country", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("attach", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("other", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("attention", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("remarks", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("ename_cn", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("name_cn", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("standard", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("country_cn", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("service_name", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("service_eid", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("zz_service_eid", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("components", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("intended_use", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("storage", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("change_records", datum));
        PreparedStatementUtils.setInt(42, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(43, datum.get("create_time"),ps);
        ps.setTimestamp(44, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(45, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
