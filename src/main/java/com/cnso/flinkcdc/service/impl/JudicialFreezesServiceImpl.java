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
public class JudicialFreezesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_judicial_freezes_tmp` (`id`,`eid`,`zz_eid`,`u_id`,`be_executed_person`,`amount`,`amount_new`,`currency`,`executive_court`,`number`,`status`,`status_code`,`type`,`type_code`,`fre_eid`,`zz_fre_eid`,`source_eid`,`zz_source_eid`,`lose_efficacy_reason`,`lose_efficacy_date`,`detail_corp_name`,`detail_public_date`,`detail_assist_name`,`detail_freeze_amount`,`detail_assist_ident_type`,`detail_freeze_year_month`,`detail_freeze_start_date`,`detail_freeze_end_date`,`detail_notice_no`,`detail_assist_item`,`detail_eid`,`zz_detail_eid`,`detail_execute_court`,`detail_adjudicate_no`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("u_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("be_executed_person", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("amount", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("amount_new", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("currency", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("executive_court", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("status", datum));
        PreparedStatementUtils.setInt(12, datum.get("status_code"),ps);
        ps.setString(13, PreparedStatementUtils.getStringVal("type", datum));
        PreparedStatementUtils.setInt(14, datum.get("type_code"),ps);
        ps.setString(15, PreparedStatementUtils.getStringVal("fre_eid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("zz_fre_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("source_eid", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("zz_source_eid", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("lose_efficacy_reason", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("lose_efficacy_date", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("detail_corp_name", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("detail_public_date", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("detail_assist_name", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("detail_freeze_amount", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("detail_assist_ident_type", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("detail_freeze_year_month", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("detail_freeze_start_date", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("detail_freeze_end_date", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("detail_notice_no", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("detail_assist_item", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("detail_eid", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("zz_detail_eid", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("detail_execute_court", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("detail_adjudicate_no", datum));
        ps.setTimestamp(35, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(36, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
