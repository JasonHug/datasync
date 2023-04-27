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
public class DrugProduceServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_drug_produce_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`credit_no`,`type_code`,`province`,`province_code`,`agency_people`,`principal`,`quality_people`,`registered_address`,`production_address`,`production_scope`,`valid_start`,`valid_end`,`state`,`state_code`,`issue_unit`,`signer`,`sup_agency`,`sup_staff`,`telephone`,`database_Inquire`,`note`,`attention`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type_code", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("agency_people", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("principal", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("quality_people", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("registered_address", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("production_address", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("production_scope", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("issue_unit", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("signer", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("sup_agency", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("sup_staff", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("telephone", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("database_Inquire", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("note", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("attention", datum));
        PreparedStatementUtils.setInt(28, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(29, datum.get("create_time"),ps);
        ps.setTimestamp(30, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(31, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
