package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class CurrencyCodeServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_currency_code_tmp` (`id`,`type_name`,`type_code`,`type_en_code`,`last_update_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id").toString(),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type_code", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type_en_code", datum));
        PreparedStatementUtils.setLong(5, datum.get("last_update_time").toString(),ps);
        ps.setTimestamp(6, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(7, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
