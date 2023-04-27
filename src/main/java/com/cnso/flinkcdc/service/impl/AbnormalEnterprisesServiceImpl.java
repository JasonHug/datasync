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
public class AbnormalEnterprisesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_abnormal_enterprises_tmp` (`ID`,`area`,`certificates_type`,`judge_area`,`judge_date`,`judge_department`,`judge_phone`,`legal_rep`,`name`,`overdue_type`,`pub_date`,`reason`,`state`,`tax_num`,`overdue_amount`,`eid`,`zz_eid`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("ID"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("certificates_type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("judge_area", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("judge_date", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("judge_department", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("judge_phone", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("legal_rep", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("overdue_type", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("pub_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("reason", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("tax_num", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("overdue_amount", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setTimestamp(18, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
