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
public class HugeTaxPunishmentServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_huge_tax_punishment_tmp` (`name`,`tax_num`,`org_code`,`judge_people`,`financial_people`,`agency_people`,`case_type`,`truth`,`law_punishment`,`police`,`pub_department`,`check_department`,`type`,`time`,`md5`,`eid`,`zz_eid`,`area`,`created_time`,`person_name`,`info_type`,`his_judge_people`,`actual_person`,`start_date`,`end_date`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("tax_num", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("org_code", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("judge_people", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("financial_people", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("agency_people", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("case_type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("truth", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("law_punishment", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("police", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("pub_department", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("check_department", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("time", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("md5", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("area", datum));
        PreparedStatementUtils.setLong(19, datum.get("created_time"),ps);
        ps.setString(20, PreparedStatementUtils.getStringVal("person_name", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("info_type", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("his_judge_people", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("actual_person", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setTimestamp(26, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(27, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
