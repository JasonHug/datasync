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
public class LawsuitsRolerelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_lawsuits_rolerelations_tmp` (`zz_id`,`id`,`obj_id`,`eid`,`zz_eid`,`ename`,`role`,`clean_role`,`trial_result`,`md5`,`title`,`pub_date`,`year_pubdate`,`case_no`,`case_cause`,`cause_action`,`case_causes`,`type`,`case_type`,`doc_id`,`doc_type`,`verdict_type`,`related_case_no`,`case_status`,`judgeresult`,`date`,`year_date`,`court`,`initial_court_code`,`court_area_code`,`related_companies`,`sub_amount`,`freezing_info`,`u_tags`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`,`table_suffix`,`db_name`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?,?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("role", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("clean_role", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("trial_result", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("md5", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("pub_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("year_pubdate", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("case_no", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("case_cause", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("cause_action", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("case_causes", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("case_type", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("doc_id", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("doc_type", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("verdict_type", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("related_case_no", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("case_status", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("judgeresult", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("year_date", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("initial_court_code", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("court_area_code", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("related_companies", datum));
        PreparedStatementUtils.setBigDecimal(32, datum.get("sub_amount"), ps);
        ps.setString(33, PreparedStatementUtils.getStringVal("freezing_info", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("u_tags", datum));
        ps.setTimestamp(35, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(36, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(37, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
        ps.setString(38, PreparedStatementUtils.getStringVal("table_suffix", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("db_name", datum));
    }

}
