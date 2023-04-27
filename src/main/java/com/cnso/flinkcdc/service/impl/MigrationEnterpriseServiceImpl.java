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
public class MigrationEnterpriseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_migration_enterprise_tmp` (`id`,`_id`,`eid`,`zz_eid`,`move_date`,`move_from_address`,`move_from_province_code`,`move_from_city_code`,`move_from_region_code`,`move_to_address`,`move_to_province_code`,`move_to_city_code`,`move_to_region_code`,`source_type`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("move_date", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("move_from_address", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("move_from_province_code", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("move_from_city_code", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("move_from_region_code", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("move_to_address", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("move_to_province_code", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("move_to_city_code", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("move_to_region_code", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("source_type", datum));
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(16, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(17, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
