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
public class InvestInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_invest_info_tmp` (`id`,`invest_id`,`invest_name`,`website`,`establish_date`,`description`,`logo`,`location`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("invest_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("invest_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("website", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("establish_date", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("logo", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("location", datum));
        ps.setTimestamp(9, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(11, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
