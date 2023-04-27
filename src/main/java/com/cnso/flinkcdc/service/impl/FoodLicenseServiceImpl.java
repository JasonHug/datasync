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
public class FoodLicenseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_food_license_tmp` (`id`,`_id`,`register_no`,`state`,`state_code`,`type`,`license_name`,`credit_no`,`valid_start`,`valid_end`,`first_date`,`shangbao_date`,`market`,`renzheng_scope`,`renzheng_num`,`certification_basis`,`fugai_address`,`renzheng_type`,`renzheng_item`,`fugai_scope`,`change_type`,`organization_people`,`product_name`,`permit_production`,`is_multi_scene`,`renzheng_level`,`gap_no`,`renzheng_model`,`monitoring_times`,`re_certification_times`,`pdf`,`province`,`province_code`,`location`,`postal_code`,`organization_name`,`organization_eid`,`zz_organization_eid`,`organization_credit_no`,`organization_province`,`organization_address`,`change_history`,`principal_name`,`principal_postcode`,`principal_area`,`principal_credit_no`,`principal_address`,`service_scope`,`trademark_name`,`production_info_no`,`pause_start_date`,`pause_end_date`,`revocation_date`,`logout_date`,`agent_name`,`agent_no`,`agent_valid_end`,`agent_state`,`agent_url`,`agent_address`,`agent_scope`,`agent_punishment`,`production_information`,`is_sub_certificate`,`main_cert_no`,`change_date`,`out_reason`,`change_cert_date`,`permit_no_old`,`register_no_old`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("first_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("shangbao_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("market", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("renzheng_scope", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("renzheng_num", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("certification_basis", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("fugai_address", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("renzheng_type", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("renzheng_item", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("fugai_scope", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("change_type", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("organization_people", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("product_name", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("permit_production", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("is_multi_scene", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("renzheng_level", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("gap_no", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("renzheng_model", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("monitoring_times", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("re_certification_times", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("pdf", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("postal_code", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("organization_name", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("organization_eid", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("zz_organization_eid", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("organization_credit_no", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("organization_province", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("organization_address", datum));
        ps.setString(42, PreparedStatementUtils.getStringVal("change_history", datum));
        ps.setString(43, PreparedStatementUtils.getStringVal("principal_name", datum));
        ps.setString(44, PreparedStatementUtils.getStringVal("principal_postcode", datum));
        ps.setString(45, PreparedStatementUtils.getStringVal("principal_area", datum));
        ps.setString(46, PreparedStatementUtils.getStringVal("principal_credit_no", datum));
        ps.setString(47, PreparedStatementUtils.getStringVal("principal_address", datum));
        ps.setString(48, PreparedStatementUtils.getStringVal("service_scope", datum));
        ps.setString(49, PreparedStatementUtils.getStringVal("trademark_name", datum));
        ps.setString(50, PreparedStatementUtils.getStringVal("production_info_no", datum));
        ps.setString(51, PreparedStatementUtils.getStringVal("pause_start_date", datum));
        ps.setString(52, PreparedStatementUtils.getStringVal("pause_end_date", datum));
        ps.setString(53, PreparedStatementUtils.getStringVal("revocation_date", datum));
        ps.setString(54, PreparedStatementUtils.getStringVal("logout_date", datum));
        ps.setString(55, PreparedStatementUtils.getStringVal("agent_name", datum));
        ps.setString(56, PreparedStatementUtils.getStringVal("agent_no", datum));
        ps.setString(57, PreparedStatementUtils.getStringVal("agent_valid_end", datum));
        ps.setString(58, PreparedStatementUtils.getStringVal("agent_state", datum));
        ps.setString(59, PreparedStatementUtils.getStringVal("agent_url", datum));
        ps.setString(60, PreparedStatementUtils.getStringVal("agent_address", datum));
        ps.setString(61, PreparedStatementUtils.getStringVal("agent_scope", datum));
        ps.setString(62, PreparedStatementUtils.getStringVal("agent_punishment", datum));
        ps.setString(63, PreparedStatementUtils.getStringVal("production_information", datum));
        ps.setString(64, PreparedStatementUtils.getStringVal("is_sub_certificate", datum));
        ps.setString(65, PreparedStatementUtils.getStringVal("main_cert_no", datum));
        ps.setString(66, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(67, PreparedStatementUtils.getStringVal("out_reason", datum));
        ps.setString(68, PreparedStatementUtils.getStringVal("change_cert_date", datum));
        ps.setString(69, PreparedStatementUtils.getStringVal("permit_no_old", datum));
        ps.setString(70, PreparedStatementUtils.getStringVal("register_no_old", datum));
        PreparedStatementUtils.setInt(71, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(72, datum.get("create_time"),ps);
        ps.setTimestamp(73, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(74, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
