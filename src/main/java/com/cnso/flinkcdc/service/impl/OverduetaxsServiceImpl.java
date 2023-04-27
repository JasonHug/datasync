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
public class OverduetaxsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_overduetaxs_tmp` (`_id`,`taxpayer_type`,`ename`,`pub_department`,`overdue_type`,`history_overdue_amount`,`area`,`overdue_amount`,`oper_name`,`taxpayer_num`,`reg_type`,`curr_overdue_amount`,`type`,`overdue_time`,`oper_id_type`,`pub_date`,`tax_bureau`,`eid`,`zz_eid`,`overdue_period`,`address`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("taxpayer_type", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("pub_department", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("overdue_type", datum));
        PreparedStatementUtils.setDouble(6, datum.get("history_overdue_amount"), ps);
        ps.setString(7, PreparedStatementUtils.getStringVal("area", datum));
        PreparedStatementUtils.setDouble(8, datum.get("overdue_amount"), ps);
        ps.setString(9, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("taxpayer_num", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("reg_type", datum));
        PreparedStatementUtils.setDouble(12, datum.get("curr_overdue_amount"), ps);
        ps.setString(13, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("overdue_time", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("oper_id_type", datum));
        PreparedStatementUtils.setDate(16, datum.get("pub_date"), ps);
        ps.setString(17, PreparedStatementUtils.getStringVal("tax_bureau", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("overdue_period", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("address", datum));
        PreparedStatementUtils.setLong(22, datum.get("created_time"),ps);
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
