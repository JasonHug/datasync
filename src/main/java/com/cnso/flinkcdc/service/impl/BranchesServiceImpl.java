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
public class BranchesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_branches_tmp` (`id`,`eid`,`zz_eid`,`seq_no`,`name`,`sub_eid`,`zz_sub_eid`,`belong_org`,`oper_name`,`reg_no`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        PreparedStatementUtils.setLong(4, datum.get("seq_no"),ps);
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("sub_eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("zz_sub_eid", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("belong_org", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setTimestamp(11, new Timestamp(TableUtils.getLong(datum.get("created_time").toString())));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
