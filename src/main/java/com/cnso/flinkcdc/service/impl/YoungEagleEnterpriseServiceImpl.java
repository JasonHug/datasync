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
public class YoungEagleEnterpriseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_young_eagle_enterprise_tmp` (`id`,`_id`,`eid`,`zz_eid`,`name`,`district`,`district_code`,`year`,`publish_date`,`remarks`,`state`,`state_code`,`level`,`url`,`is_history`,`u_tags`,`create_time`,`oss_url`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("district", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("district_code", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("year", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("publish_date", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("remarks", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("level", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(15, datum.get("is_history"),ps);
        PreparedStatementUtils.setInt(16, datum.get("u_tags"),ps);
        PreparedStatementUtils.setLong(17, datum.get("create_time"),ps);
        ps.setString(18, PreparedStatementUtils.getStringVal("oss_url", datum));
        ps.setTimestamp(19, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(20, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
