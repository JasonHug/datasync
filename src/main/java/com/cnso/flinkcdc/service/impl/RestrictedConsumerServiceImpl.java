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
public class RestrictedConsumerServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_restricted_consumer_tmp` (`_id`,`name`,`company_name`,`company_name_eid`,`zz_company_name_eid`,`eid`,`zz_eid`,`sex`,`filing_date`,`case_no`,`court`,`court_code`,`execution_applicant`,`execution_applicant_eid`,`zz_execution_applicant_eid`,`case_reason`,`release_date`,`content`,`created_time`,`row_update_time`,`case_relation`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("company_name", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("company_name_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_company_name_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("sex", datum));
        PreparedStatementUtils.setDate(9, datum.get("filing_date"), ps);
        ps.setString(10, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("court_code", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("execution_applicant", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("execution_applicant_eid", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("zz_execution_applicant_eid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("case_reason", datum));
        PreparedStatementUtils.setDate(17, datum.get("release_date"), ps);
        ps.setString(18, PreparedStatementUtils.getStringVal("content", datum));
        PreparedStatementUtils.setLong(19, datum.get("created_time"),ps);
        ps.setTimestamp(20, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        PreparedStatementUtils.setLong(21, datum.get("case_relation"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
