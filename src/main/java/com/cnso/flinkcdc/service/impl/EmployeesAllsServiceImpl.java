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
public class EmployeesAllsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_employees_alls_tmp` (`zz_id`,`id`,`eid`,`zz_eid`,`name`,`job_title`,`name_type`,`sex`,`is_history`,`start_date`,`start_is_actual`,`end_date`,`is_actual`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("job_title", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("name_type", datum));
        PreparedStatementUtils.setInt(8, datum.get("sex"),ps);
        PreparedStatementUtils.setInt(9, datum.get("is_history"),ps);
        ps.setString(10, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("start_is_actual", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("is_actual", datum));
        ps.setTimestamp(14, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(16, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
