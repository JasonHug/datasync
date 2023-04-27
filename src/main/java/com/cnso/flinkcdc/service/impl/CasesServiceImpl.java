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
public class CasesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_cases_tmp` (`_id`,`court`,`end_date`,`assistant`,`region`,`case_no`,`agent`,`hearing_date`,`related_companies`,`case_status`,`start_date`,`created_time`,`last_updated_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("assistant", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("region", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("agent", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("hearing_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("case_status", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("start_date", datum));
        PreparedStatementUtils.setLong(12, datum.get("created_time"),ps);
        PreparedStatementUtils.setLong(13, datum.get("last_updated_time"),ps);
        ps.setTimestamp(14, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
