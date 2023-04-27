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
public class FoodProductLicenseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_food_product_license_tmp` (`id`,`_id`,`type`,`license_name`,`cert_type`,`credit_no`,`oper_name`,`address`,`production_address`,`pro_type`,`register_no`,`check_type`,`level`,`institution_name`,`people_name`,`department`,`signer`,`valid_start`,`valid_end`,`state`,`state_code`,`composition`,`license_details`,`product_name`,`description`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("cert_type", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("production_address", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("pro_type", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("check_type", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("level", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("institution_name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("people_name", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("signer", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("composition", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("license_details", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("description", datum));
        PreparedStatementUtils.setInt(26, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(27, datum.get("create_time"),ps);
        ps.setTimestamp(28, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(29, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
