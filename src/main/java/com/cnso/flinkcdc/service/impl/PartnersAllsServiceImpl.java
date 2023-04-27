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
public class PartnersAllsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_partners_alls_tmp` (`id`,`eid`,`zz_eid`,`seq_no`,`stock_name`,`stock_id`,`zz_stock_id`,`stock_type`,`stock_type_code`,`stock_percent`,`country`,`country_code`,`identify_type`,`total_real_capi`,`total_real_capi_new`,`real_currency_unit`,`total_should_capi`,`total_should_capi_new`,`should_currency_unit`,`should_capi_items`,`real_capi_items`,`u_tags`,`entity_type`,`end_date`,`is_history`,`end_is_actual`,`start_date`,`start_is_actual`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        PreparedStatementUtils.setLong(4, datum.get("seq_no"),ps);
        ps.setString(5, PreparedStatementUtils.getStringVal("stock_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("stock_id", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("zz_stock_id", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("stock_type", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("stock_type_code", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("stock_percent", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("country", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("country_code", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("identify_type", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("total_real_capi", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("total_real_capi_new", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("real_currency_unit", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("total_should_capi", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("total_should_capi_new", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("should_currency_unit", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("should_capi_items", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("real_capi_items", datum));
        PreparedStatementUtils.setInt(22, datum.get("u_tags"),ps);
        ps.setString(23, PreparedStatementUtils.getStringVal("entity_type", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("end_date", datum));
        PreparedStatementUtils.setInt(25, datum.get("is_history"),ps);
        PreparedStatementUtils.setInt(26, datum.get("end_is_actual"),ps);
        ps.setString(27, PreparedStatementUtils.getStringVal("start_date", datum));
        PreparedStatementUtils.setInt(28, datum.get("start_is_actual"),ps);
        PreparedStatementUtils.setLong(29, datum.get("create_time"),ps);
        ps.setTimestamp(30, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(31, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
