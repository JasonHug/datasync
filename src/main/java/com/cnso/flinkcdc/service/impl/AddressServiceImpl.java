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
public class AddressServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_address_tmp` (`id`,`eid`,`zz_eid`,`address`,`name`,`postcode`,`date`,`update_date`,`created_time`,`row_update_time`,`address_code`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("postcode", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("update_date", datum));
        ps.setTimestamp(9, new Timestamp(TableUtils.getLong(datum.get("created_time").toString())));
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(11, PreparedStatementUtils.getStringVal("address_code", datum));
        PreparedStatementUtils.setInt(12, datum.get("is_history"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
