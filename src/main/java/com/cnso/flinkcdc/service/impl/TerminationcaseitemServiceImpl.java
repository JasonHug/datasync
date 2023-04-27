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
public class TerminationcaseitemServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_terminationcaseitem_tmp` (`id`,`eid`,`zz_eid`,`name`,`address`,`sex`,`filing_date`,`case_no_terminal`,`case_no_origin`,`court`,`terminate_date`,`amount`,`fail_perform_amount`,`case_relation`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("sex", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("filing_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("case_no_terminal", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("case_no_origin", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("terminate_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("amount", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("fail_perform_amount", datum));
        PreparedStatementUtils.setLong(14, datum.get("case_relation"),ps);
        PreparedStatementUtils.setLong(15, datum.get("created_time"),ps);
        ps.setString(16, PreparedStatementUtils.getStringVal("row_update_time", datum));
        ps.setTimestamp(17, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
