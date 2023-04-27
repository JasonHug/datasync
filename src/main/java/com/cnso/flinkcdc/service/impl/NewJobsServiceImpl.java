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
public class NewJobsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_new_jobs_tmp` (`_id`,`eid`,`zz_eid`,`job_3rd_class`,`job_type`,`number`,`sex`,`welfare`,`common_name`,`education`,`size`,`title`,`location`,`ops_flag`,`start_date`,`job_1st_class`,`end_date`,`date`,`years`,`salary`,`name`,`employer_type`,`url`,`industry`,`job_2nd_class`,`job_tag`,`age`,`description`,`skill`,`position`,`qualification`,`created_time`,`title_abbr`,`title_code`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("job_3rd_class", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("job_type", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("sex", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("welfare", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("common_name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("education", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("size", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("location", datum));
        PreparedStatementUtils.setLong(14, datum.get("ops_flag"),ps);
        ps.setString(15, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("job_1st_class", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("end_date", datum));
        PreparedStatementUtils.setDate(18, datum.get("date"), ps);
        ps.setString(19, PreparedStatementUtils.getStringVal("years", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("salary", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("employer_type", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("industry", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("job_2nd_class", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("job_tag", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("age", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("skill", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("position", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("qualification", datum));
        PreparedStatementUtils.setLong(32, datum.get("created_time"),ps);
        ps.setString(33, PreparedStatementUtils.getStringVal("title_abbr", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("title_code", datum));
        ps.setTimestamp(35, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(36, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
