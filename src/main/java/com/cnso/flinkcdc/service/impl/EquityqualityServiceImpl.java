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
public class EquityqualityServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_equityquality_tmp` (`id`,`eid`,`zz_eid`,`u_id`,`number`,`pledgor`,`pledgor_amount`,`pledgor_unit`,`pledgor_currency`,`pawnee`,`date`,`status`,`status_code`,`remark`,`public_date`,`pawnee_eid`,`zz_pawnee_eid`,`object_company`,`pledgor_eid`,`zz_pledgor_eid`,`object_company_eid`,`zz_object_company_eid`,`row_update_time`,`cancel_date`,`cancel_content`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("u_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("pledgor", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("pledgor_amount", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("pledgor_unit", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("pledgor_currency", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("pawnee", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("status", datum));
        PreparedStatementUtils.setInt(13, datum.get("status_code"),ps);
        ps.setString(14, PreparedStatementUtils.getStringVal("remark", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("public_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("pawnee_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_pawnee_eid", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("object_company", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("pledgor_eid", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("zz_pledgor_eid", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("object_company_eid", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("zz_object_company_eid", datum));
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(24, PreparedStatementUtils.getStringVal("cancel_date", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("cancel_content", datum));
        ps.setTimestamp(26, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
