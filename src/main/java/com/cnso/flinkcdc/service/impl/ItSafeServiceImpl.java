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
public class ItSafeServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_it_safe_tmp` (`id`,`_id`,`type`,`license_name`,`register_no`,`valid_start`,`valid_end`,`state`,`state_code`,`state_change_date`,`state_change_reason`,`first_date`,`product_name`,`application_level`,`principal_name`,`principal_eid`,`zz_principal_eid`,`producer_name`,`producer_eid`,`zz_producer_eid`,`standard`,`source`,`url`,`u_tags`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
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
        ps.setString(10, PreparedStatementUtils.getStringVal("state_change_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("state_change_reason", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("first_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("application_level", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("principal_name", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("principal_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_principal_eid", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("producer_name", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("producer_eid", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("zz_producer_eid", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("standard", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("source", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(24, datum.get("u_tags"),ps);
        PreparedStatementUtils.setInt(25, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(26, datum.get("create_time"),ps);
        ps.setTimestamp(27, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(28, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
