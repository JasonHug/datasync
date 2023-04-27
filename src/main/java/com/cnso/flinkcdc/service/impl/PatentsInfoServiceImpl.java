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
public class PatentsInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_patents_info_tmp` (`zz_id`,`id`,`outhor_num`,`outhor_date`,`authorize_num`,`authorize_date`,`patent_name`,`request_num`,`request_num_standard`,`request_date`,`outhor_change_pub_date`,`outhor_change_pub_no`,`authorize_change_pub_date`,`authorize_change_pub_no`,`decrypt_pub_date`,`type`,`type_name`,`authorize_tag`,`category_num_ipc`,`brief`,`patent_person_ori`,`patent_person`,`designer`,`agent_people`,`agent`,`agent_info_eid`,`zz_agent_info_eid`,`pct_date`,`qrcode`,`address`,`zipcode`,`last_status`,`compare_files`,`patent_img`,`annex`,`related_companies`,`pct_req_num`,`pct_req_date`,`pct_pub_num`,`pct_pub_date`,`pct_apply_country`,`is_history`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`,`table_suffix`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("outhor_num", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("outhor_date", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("authorize_num", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("authorize_date", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("patent_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("request_num", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("request_num_standard", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("request_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("outhor_change_pub_date", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("outhor_change_pub_no", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("authorize_change_pub_date", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("authorize_change_pub_no", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("decrypt_pub_date", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("authorize_tag", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("category_num_ipc", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("patent_person_ori", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("patent_person", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("designer", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("agent_people", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("agent", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("agent_info_eid", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("zz_agent_info_eid", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("pct_date", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("qrcode", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("address", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("zipcode", datum));
        ps.setString(32, PreparedStatementUtils.getStringVal("last_status", datum));
        ps.setString(33, PreparedStatementUtils.getStringVal("compare_files", datum));
        ps.setString(34, PreparedStatementUtils.getStringVal("patent_img", datum));
        ps.setString(35, PreparedStatementUtils.getStringVal("annex", datum));
        ps.setString(36, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(37, PreparedStatementUtils.getStringVal("pct_req_num", datum));
        ps.setString(38, PreparedStatementUtils.getStringVal("pct_req_date", datum));
        ps.setString(39, PreparedStatementUtils.getStringVal("pct_pub_num", datum));
        ps.setString(40, PreparedStatementUtils.getStringVal("pct_pub_date", datum));
        ps.setString(41, PreparedStatementUtils.getStringVal("pct_apply_country", datum));
        PreparedStatementUtils.setLong(42, datum.get("is_history"),ps);
        ps.setTimestamp(43, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(44, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(45, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
        ps.setString(46, PreparedStatementUtils.getStringVal("table_suffix", datum));
    }

}
