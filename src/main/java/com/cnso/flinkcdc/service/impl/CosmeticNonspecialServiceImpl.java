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
public class CosmeticNonspecialServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_cosmetic_nonspecial_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`product_company`,`product_company_address`,`content`,`model`,`note`,`product_name`,`address`,`country`,`note_check`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("product_company", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("product_company_address", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("model", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("note", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("country", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("note_check", datum));
        PreparedStatementUtils.setInt(19, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(20, datum.get("create_time"),ps);
        ps.setTimestamp(21, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(22, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
