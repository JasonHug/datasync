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
public class PrivatefundManagerServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_privatefund_manager_tmp` (`id`,`eid`,`zz_eid`,`name`,`oper_name`,`oper_id`,`zz_oper_id`,`TYPE`,`reg_city`,`city_code`,`fund_no`,`start_date`,`fund_date`,`en_name`,`org_no`,`address`,`office_address`,`reg_capi`,`real_capi`,`real_capi_percent`,`ent_type`,`business_type`,`employee_num`,`qualified_num`,`websites`,`is_qualified`,`is_member`,`member_deputy`,`member_type`,`member_time`,`law_status`,`law_firm`,`law_firm_eid`,`zz_law_firm_eid`,`lawyer`,`controller`,`update_time`,`reminder`,`dishonest`,`manage_interval`,`url`,`is_history`,`create_time`,`local_update_time`,`local_row_update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("oper_id", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("zz_oper_id", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("TYPE", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("reg_city", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("city_code", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("fund_no", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("fund_date", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("en_name", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("org_no", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("office_address", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("reg_capi", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("real_capi", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("real_capi_percent", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("ent_type", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("business_type", datum));
        PreparedStatementUtils.setLong(23, datum.get("employee_num"),ps);
        PreparedStatementUtils.setLong(24, datum.get("qualified_num"),ps);
        ps.setString(25, PreparedStatementUtils.getStringVal("websites", datum));
        PreparedStatementUtils.setInt(26, datum.get("is_qualified"),ps);
        PreparedStatementUtils.setInt(27, datum.get("is_member"),ps);
        ps.setString(28, PreparedStatementUtils.getStringVal("member_deputy", datum));
        PreparedStatementUtils.setInt(29, datum.get("member_type"),ps);
        ps.setString(30, PreparedStatementUtils.getStringVal("member_time", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("law_status", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("law_firm", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("law_firm_eid", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("zz_law_firm_eid", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("lawyer", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("controller", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("update_time", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("reminder", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("dishonest", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("manage_interval", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setInt(42, datum.get("is_history"),ps);
        ps.setTimestamp(43, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(44, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(45, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
