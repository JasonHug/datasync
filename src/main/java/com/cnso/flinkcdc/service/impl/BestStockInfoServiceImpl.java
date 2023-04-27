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
public class BestStockInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_best_stock_info_tmp` (`id`,`eid`,`zz_eid`,`stock_name`,`stock_id`,`zz_stock_id`,`stock_type`,`share_type`,`stock_percent`,`should_capi_conv`,`should_capi`,`real_capi`,`currency_code`,`stock_num`,`con_date`,`create_time`,`row_update_time`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("stock_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("stock_id", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_stock_id", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("stock_type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("share_type", datum));
        PreparedStatementUtils.setBigDecimal(9, datum.get("stock_percent"), ps);
        PreparedStatementUtils.setBigDecimal(10, datum.get("should_capi_conv"), ps);
        PreparedStatementUtils.setBigDecimal(11, datum.get("should_capi"), ps);
        PreparedStatementUtils.setBigDecimal(12, datum.get("real_capi"), ps);
        ps.setString(13, PreparedStatementUtils.getStringVal("currency_code", datum));
        PreparedStatementUtils.setBigDecimal(14, datum.get("stock_num"), ps);
        ps.setString(15, PreparedStatementUtils.getStringVal("con_date", datum));
        PreparedStatementUtils.setLong(16, datum.get("create_time"),ps);
        ps.setTimestamp(17, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        PreparedStatementUtils.setInt(18, datum.get("is_history"),ps);
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
