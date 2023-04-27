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
public class CccServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_ccc_tmp` (`id`,`_id`,`register_no`,`type`,`license_name`,`product_company_en`,`state`,`state_code`,`valid_start`,`valid_end`,`shangbao_date`,`pro_type`,`skill`,`product_name`,`product_name_en`,`unit_main`,`unit_se`,`model`,`pdf`,`principal_name`,`principal_eid`,`zz_principal_eid`,`principal_area`,`principal_code`,`principal_address`,`producer_name`,`producer_eid`,`zz_producer_eid`,`producer_area`,`producer_code`,`producer_address`,`applicant`,`applicant_en`,`manufacturer_name`,`manufacturer_en`,`manufacturer_address`,`manufacturer_eid`,`zz_manufacturer_eid`,`agency_name`,`agency_eid`,`zz_agency_eid`,`agency_no`,`agency_date`,`agency_phone`,`agency_domain`,`agency_status`,`agency_address`,`agency_scope`,`agency_punishment_info`,`pause_start_date`,`pause_end_date`,`change_date`,`revocation_date`,`logout_date`,`first_date`,`cert_change_date`,`cert_change_reason`,`self_declaration_no`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("product_company_en", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("shangbao_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("pro_type", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("skill", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("product_name_en", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("unit_main", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("unit_se", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("model", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("pdf", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("principal_name", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("principal_eid", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("zz_principal_eid", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("principal_area", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("principal_code", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("principal_address", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("producer_name", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("producer_eid", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("zz_producer_eid", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("producer_area", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("producer_code", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("producer_address", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("applicant", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("applicant_en", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("manufacturer_name", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("manufacturer_en", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("manufacturer_address", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("manufacturer_eid", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("zz_manufacturer_eid", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("agency_name", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("agency_eid", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("zz_agency_eid", datum));
        ps.setString(42, PreparedStatementUtils.getStringVal("agency_no", datum));
        ps.setString(43, PreparedStatementUtils.getStringVal("agency_date", datum));
        ps.setString(44, PreparedStatementUtils.getStringVal("agency_phone", datum));
        ps.setString(45, PreparedStatementUtils.getStringVal("agency_domain", datum));
        ps.setString(46, PreparedStatementUtils.getStringVal("agency_status", datum));
        ps.setString(47, PreparedStatementUtils.getStringVal("agency_address", datum));
        ps.setString(48, PreparedStatementUtils.getStringVal("agency_scope", datum));
        ps.setString(49, PreparedStatementUtils.getStringVal("agency_punishment_info", datum));
        ps.setString(50, PreparedStatementUtils.getStringVal("pause_start_date", datum));
        ps.setString(51, PreparedStatementUtils.getStringVal("pause_end_date", datum));
        ps.setString(52, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(53, PreparedStatementUtils.getStringVal("revocation_date", datum));
        ps.setString(54, PreparedStatementUtils.getStringVal("logout_date", datum));
        ps.setString(55, PreparedStatementUtils.getStringVal("first_date", datum));
        ps.setString(56, PreparedStatementUtils.getStringVal("cert_change_date", datum));
        ps.setString(57, PreparedStatementUtils.getStringVal("cert_change_reason", datum));
        ps.setString(58, PreparedStatementUtils.getStringVal("self_declaration_no", datum));
        PreparedStatementUtils.setInt(59, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(60, datum.get("create_time"),ps);
        ps.setTimestamp(61, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(62, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
