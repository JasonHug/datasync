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
public class TrademarkInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_trademark_info_tmp` (`zz_id`,`id`,`_id`,`reg_number`,`trademark_name`,`type_num`,`apply_date`,`apply_year`,`eid`,`zz_eid`,`applicant`,`applicant_en`,`address_cn`,`address_en`,`image_url`,`org_image_url`,`first_pubno`,`first_pubdate`,`reg_pubno`,`reg_pubdate`,`is_shared`,`applicant_share`,`trademark_type`,`category_flag`,`period`,`start_date`,`end_date`,`trademark_form`,`global_date`,`appoint_date`,`priority_date`,`agent`,`trademark_status`,`now_status`,`is_history`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("reg_number", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("trademark_name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("type_num", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("apply_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("apply_year", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("applicant", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("applicant_en", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("address_cn", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("address_en", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("image_url", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("org_image_url", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("first_pubno", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("first_pubdate", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("reg_pubno", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("reg_pubdate", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("is_shared", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("applicant_share", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("trademark_type", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("category_flag", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("period", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("end_date", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("trademark_form", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("global_date", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("appoint_date", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("priority_date", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("agent", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("trademark_status", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("now_status", datum));
        PreparedStatementUtils.setInt(35, datum.get("is_history"),ps);
        ps.setTimestamp(36, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(37, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(38, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
