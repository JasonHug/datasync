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
public class BusinessConcessionServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_business_concession_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`address`,`oper_name`,`start_date`,`websites`,`telephone`,`fax`,`email`,`change_info`,`territory_store`,`credit_info`,`related_companies`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
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
        ps.setString(10, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("websites", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("telephone", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("fax", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("email", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("change_info", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("territory_store", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("credit_info", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("related_companies", datum));
        PreparedStatementUtils.setInt(21, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(22, datum.get("create_time"),ps);
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}