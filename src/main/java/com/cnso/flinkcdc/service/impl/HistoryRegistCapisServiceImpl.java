package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class HistoryRegistCapisServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_history_regist_capis_tmp` (`id`,`eid`,`zz_eid`,`date`,`regist_capi`,`regist_capi_new`,`currency_unit`,`source`,`is_actual`,`u_tags`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("regist_capi", datum));
        PreparedStatementUtils.setBigDecimal(6, datum.get("regist_capi_new"), ps);
        ps.setString(7, PreparedStatementUtils.getStringVal("currency_unit", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("source", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("is_actual", datum));
        PreparedStatementUtils.setInt(10, datum.get("u_tags"),ps);
        PreparedStatementUtils.setLong(11, datum.get("create_time"),ps);
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
