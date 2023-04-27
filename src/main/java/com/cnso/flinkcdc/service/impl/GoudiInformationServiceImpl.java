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
public class GoudiInformationServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_goudi_information_tmp` (`md5`,`eid`,`zz_eid`,`name_before`,`eid_before`,`zz_eid_before`,`province`,`location`,`area`,`land_use`,`signing_mode`,`release_time`,`content`,`request_type`,`elect_no`,`project_name`,`land_source`,`use_year`,`industry`,`land_level`,`price`,`fqzf`,`ydrjl_sx`,`ydrjl_xx`,`ydjdrq`,`ydkgrq`,`ydjgrq`,`sjkgrq`,`sjjgrq`,`pzdw`,`title`,`gonggao_type`,`online_time`,`marked`,`code`,`use_type`,`use_situation`,`transfer_type`,`transfer_price`,`use_code`,`rent_year`,`due_time`,`rent_amount`,`diya_area`,`user_code`,`diya_type`,`diya_use`,`shuxing`,`pro_amount`,`min_rj`,`max_rj`,`ydjgsj`,`public_date`,`email`,`contact_person`,`contact_tel`,`zip`,`address`,`ent`,`note`,`gsks`,`gsjs`,`entity_type_before`,`entity_type_after`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("md5", datum));
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name_before", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("eid_before", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_eid_before", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("location", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("area", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("land_use", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("signing_mode", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("release_time", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("request_type", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("elect_no", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("project_name", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("land_source", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("use_year", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("industry", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("land_level", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("price", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("fqzf", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("ydrjl_sx", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("ydrjl_xx", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("ydjdrq", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("ydkgrq", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("ydjgrq", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("sjkgrq", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("sjjgrq", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("pzdw", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("gonggao_type", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("online_time", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("marked", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("code", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("use_type", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("use_situation", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("transfer_type", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("transfer_price", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("use_code", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("rent_year", datum));
        ps.setString(42, PreparedStatementUtils.getStringVal("due_time", datum));
        ps.setString(43, PreparedStatementUtils.getStringVal("rent_amount", datum));
        ps.setString(44, PreparedStatementUtils.getStringVal("diya_area", datum));
        ps.setString(45, PreparedStatementUtils.getStringVal("user_code", datum));
        ps.setString(46, PreparedStatementUtils.getStringVal("diya_type", datum));
        ps.setString(47, PreparedStatementUtils.getStringVal("diya_use", datum));
        ps.setString(48, PreparedStatementUtils.getStringVal("shuxing", datum));
        ps.setString(49, PreparedStatementUtils.getStringVal("pro_amount", datum));
        ps.setString(50, PreparedStatementUtils.getStringVal("min_rj", datum));
        ps.setString(51, PreparedStatementUtils.getStringVal("max_rj", datum));
        ps.setString(52, PreparedStatementUtils.getStringVal("ydjgsj", datum));
        ps.setString(53, PreparedStatementUtils.getStringVal("public_date", datum));
        ps.setString(54, PreparedStatementUtils.getStringVal("email", datum));
        ps.setString(55, PreparedStatementUtils.getStringVal("contact_person", datum));
        ps.setString(56, PreparedStatementUtils.getStringVal("contact_tel", datum));
        ps.setString(57, PreparedStatementUtils.getStringVal("zip", datum));
        ps.setString(58, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(59, PreparedStatementUtils.getStringVal("ent", datum));
        ps.setString(60, PreparedStatementUtils.getStringVal("note", datum));
        ps.setString(61, PreparedStatementUtils.getStringVal("gsks", datum));
        ps.setString(62, PreparedStatementUtils.getStringVal("gsjs", datum));
        ps.setString(63, PreparedStatementUtils.getStringVal("entity_type_before", datum));
        ps.setString(64, PreparedStatementUtils.getStringVal("entity_type_after", datum));
        PreparedStatementUtils.setLong(65, datum.get("created_time"),ps);
        ps.setTimestamp(66, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(67, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
