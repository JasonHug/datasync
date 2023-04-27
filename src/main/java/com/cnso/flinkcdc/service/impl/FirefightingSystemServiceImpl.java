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
public class FirefightingSystemServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_firefighting_system_tmp` (`id`,`_id`,`register_no`,`type`,`license_name`,`pro_type`,`product`,`product_model`,`principal`,`principal_history_name`,`principal_eid`,`zz_principal_eid`,`principal_address`,`producer`,`producer_eid`,`zz_producer_eid`,`producer_address`,`check_basis`,`check_reason`,`province`,`province_code`,`zipcode`,`contact_person`,`telephone`,`production_address`,`report_no`,`report_pub_date`,`check_unit`,`check_type`,`sample_grade`,`check_project`,`check_conclusion`,`valid_start`,`valid_end`,`renzheng_model`,`state`,`state_code`,`change_date`,`inspection_report`,`is_history`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("register_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("license_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("pro_type", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("product", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("product_model", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("principal", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("principal_history_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("principal_eid", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("zz_principal_eid", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("principal_address", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("producer", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("producer_eid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("zz_producer_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("producer_address", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("check_basis", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("check_reason", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("province_code", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("zipcode", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("contact_person", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("telephone", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("production_address", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("report_no", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("report_pub_date", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("check_unit", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("check_type", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("sample_grade", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("check_project", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("check_conclusion", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("valid_start", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("valid_end", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("renzheng_model", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("state", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("state_code", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("change_date", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("inspection_report", datum));
        PreparedStatementUtils.setInt(40, datum.get("is_history"),ps);
        PreparedStatementUtils.setLong(41, datum.get("create_time"),ps);
        ps.setTimestamp(42, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(43, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
