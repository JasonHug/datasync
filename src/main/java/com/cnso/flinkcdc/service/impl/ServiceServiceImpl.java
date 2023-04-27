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
public class ServiceServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_service_tmp` (`id`,`_id`,`register_no`,`type`,`license_name`,`province`,`province_code`,`credit_no`,`location`,`postal_code`,`state`,`state_code`,`valid_start`,`valid_end`,`shangbao_date`,`market`,`certification_basis`,`pdf`,`agency_name`,`agency_eid`,`zz_agency_eid`,`agency_no`,`agency_date`,`agency_phone`,`agency_domain`,`agency_status`,`agency_address`,`agency_scope`,`agent_punishment`,`pause_start_date`,`pause_end_date`,`change_date`,`revocation_date`,`first_date`,`monitoring_times`,`re_certification_times`,`item`,`fugai_scope`,`is_multiplace`,`fugai_address`,`change_type`,`is_sub_certificate`,`main_certificate`,`organization_name`,`organization_eid`,`zz_organization_eid`,`organization_area`,`organization_area_code`,`organization_people`,`organization_no`,`organization_address`,`renzheng_num`,`renzheng_scope`,`production_info`,`change_history`,`renzheng_type`,`renzheng_level`,`renzheng_model`,`out_reason`,`logout_date`,`change_cert_date`,`permit_no_old`,`register_no_old`,`gap_no`,`service_scope`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("postal_code", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("shangbao_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("market", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("certification_basis", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("pdf", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("agency_name", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("agency_eid", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("zz_agency_eid", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("agency_no", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("agency_date", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("agency_phone", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("agency_domain", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("agency_status", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("agency_address", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("agency_scope", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("agent_punishment", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("pause_start_date", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("pause_end_date", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("revocation_date", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("first_date", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("monitoring_times", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("re_certification_times", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("item", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("fugai_scope", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("is_multiplace", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("fugai_address", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("change_type", datum));
        ps.setString(42, PreparedStatementUtils.getStringVal("is_sub_certificate", datum));
        ps.setString(43, PreparedStatementUtils.getStringVal("main_certificate", datum));
        ps.setString(44, PreparedStatementUtils.getStringVal("organization_name", datum));
        ps.setString(45, PreparedStatementUtils.getStringVal("organization_eid", datum));
        ps.setString(46, PreparedStatementUtils.getStringVal("zz_organization_eid", datum));
        ps.setString(47, PreparedStatementUtils.getStringVal("organization_area", datum));
        ps.setString(48, PreparedStatementUtils.getStringVal("organization_area_code", datum));
        ps.setString(49, PreparedStatementUtils.getStringVal("organization_people", datum));
        ps.setString(50, PreparedStatementUtils.getStringVal("organization_no", datum));
        ps.setString(51, PreparedStatementUtils.getStringVal("organization_address", datum));
        ps.setString(52, PreparedStatementUtils.getStringVal("renzheng_num", datum));
        ps.setString(53, PreparedStatementUtils.getStringVal("renzheng_scope", datum));
        ps.setString(54, PreparedStatementUtils.getStringVal("production_info", datum));
        ps.setString(55, PreparedStatementUtils.getStringVal("change_history", datum));
        ps.setString(56, PreparedStatementUtils.getStringVal("renzheng_type", datum));
        ps.setString(57, PreparedStatementUtils.getStringVal("renzheng_level", datum));
        ps.setString(58, PreparedStatementUtils.getStringVal("renzheng_model", datum));
        ps.setString(59, PreparedStatementUtils.getStringVal("out_reason", datum));
        ps.setString(60, PreparedStatementUtils.getStringVal("logout_date", datum));
        ps.setString(61, PreparedStatementUtils.getStringVal("change_cert_date", datum));
        ps.setString(62, PreparedStatementUtils.getStringVal("permit_no_old", datum));
        ps.setString(63, PreparedStatementUtils.getStringVal("register_no_old", datum));
        ps.setString(64, PreparedStatementUtils.getStringVal("gap_no", datum));
        ps.setString(65, PreparedStatementUtils.getStringVal("service_scope", datum));
        PreparedStatementUtils.setInt(66, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(67, datum.get("create_time"),ps);
        ps.setTimestamp(68, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(69, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
