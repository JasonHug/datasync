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
public class DomainsAllsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_domains_alls_tmp` (`id`,`eid`,`zz_eid`,`number`,`ename`,`domain`,`site_name`,`home_url`,`url`,`body_name`,`check_date`,`type`,`resp_person`,`province`,`city`,`town`,`body_number`,`address`,`possible_malinfo`,`create_time`,`row_update_time`,`local_update_time`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("domain", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("site_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("home_url", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("body_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("check_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("resp_person", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("city", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("town", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("body_number", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("address", datum));
        PreparedStatementUtils.setInt(19, datum.get("possible_malinfo"),ps);
        ps.setTimestamp(20, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(21, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        PreparedStatementUtils.setInt(23, datum.get("is_history"),ps);
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
