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
public class TelecomLicenceServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_telecom_licence_tmp` (`id`,`_id`,`type`,`license_name`,`eid`,`zz_eid`,`ename`,`register_no`,`profession_kind`,`scope`,`valid_end`,`state`,`state_code`,`report_company_name`,`report_company_eid`,`zz_report_company_eid`,`credit_no`,`oper_name`,`regist_address`,`regist_province`,`regist_capi`,`licence_kind`,`ent_nature`,`is_quoted`,`complaint_phone_num`,`complaint_no`,`complaint_reply_rate`,`related_companies`,`qds`,`url`,`u_tags`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("profession_kind", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("scope", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("report_company_name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("report_company_eid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("zz_report_company_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("regist_address", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("regist_province", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("regist_capi", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("licence_kind", datum));
        PreparedStatementUtils.setInt(23, datum.get("ent_nature"),ps);
        ps.setString(24, PreparedStatementUtils.getStringVal("is_quoted", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("complaint_phone_num", datum));
        PreparedStatementUtils.setLong(26, datum.get("complaint_no"),ps);
        PreparedStatementUtils.setBigDecimal(27, datum.get("complaint_reply_rate"), ps);
        ps.setString(28, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("qds", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(31, datum.get("u_tags"),ps);
        PreparedStatementUtils.setInt(32, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(33, datum.get("create_time"),ps);
        ps.setTimestamp(34, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(35, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
