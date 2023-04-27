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
public class ProductSafeLicenseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_product_safe_license_tmp` (`id`,`_id`,`register_no`,`type`,`license_name`,`register_form`,`agency_people`,`credit_no`,`register_address`,`province`,`province_code`,`economic_type`,`permit_scope`,`permit_agency`,`valid_start`,`valid_end`,`issue_date`,`last_valid_date`,`extend_date`,`state`,`state_code`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("register_form", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("agency_people", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("register_address", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("economic_type", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("permit_scope", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("permit_agency", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("issue_date", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("last_valid_date", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("extend_date", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("state_code", datum));
        PreparedStatementUtils.setInt(22, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(23, datum.get("create_time"),ps);
        ps.setTimestamp(24, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(25, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
