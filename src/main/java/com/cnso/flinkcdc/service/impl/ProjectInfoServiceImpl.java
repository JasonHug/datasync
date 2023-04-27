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
public class ProjectInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_project_info_tmp` (`id`,`proj_id`,`proj_name`,`ename`,`eid`,`zz_eid`,`round_type`,`logo`,`website`,`brief`,`intro`,`finance_round`,`district_code`,`phone`,`email`,`prime`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("proj_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("proj_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("round_type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("logo", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("website", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("intro", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("finance_round", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("district_code", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("phone", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("email", datum));
        PreparedStatementUtils.setInt(16, datum.get("prime"),ps);
        ps.setTimestamp(17, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(18, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
