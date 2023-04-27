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
public class ProjectMemberServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_project_member_tmp` (`id`,`proj_id`,`pid`,`ename`,`eid`,`zz_eid`,`pname`,`pos`,`edu`,`works`,`brief`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("proj_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("pid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("pname", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("pos", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("edu", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("works", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
