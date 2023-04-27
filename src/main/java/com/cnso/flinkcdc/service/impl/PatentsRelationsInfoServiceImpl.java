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
public class PatentsRelationsInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_patents_relations_info_tmp` (`zz_id`,`id`,`_id`,`eid`,`zz_eid`,`ename`,`role`,`history_role_tag`,`type`,`type_name`,`patent_name`,`authorize_tag`,`outhor_num`,`outhor_date`,`authorize_num`,`authorize_date`,`outhor_change_pub_date`,`outhor_change_pub_no`,`authorize_change_pub_date`,`authorize_change_pub_no`,`request_num_standard`,`brief`,`patent_person_ori`,`patent_person`,`agent`,`agent_info_eid`,`zz_agent_info_eid`,`category_num_ipc`,`request_date`,`last_status`,`related_companies`,`is_history`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`,`table_suffix`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("_id", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("role", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("history_role_tag", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("patent_name", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("authorize_tag", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("outhor_num", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("outhor_date", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("authorize_num", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("authorize_date", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("outhor_change_pub_date", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("outhor_change_pub_no", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("authorize_change_pub_date", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("authorize_change_pub_no", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("request_num_standard", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("patent_person_ori", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("patent_person", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("agent", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("agent_info_eid", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("zz_agent_info_eid", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("category_num_ipc", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("request_date", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("last_status", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("related_companies", datum));
        PreparedStatementUtils.setLong(32, datum.get("is_history"),ps);
        ps.setTimestamp(33, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(34, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(35, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
        ps.setString(36, PreparedStatementUtils.getStringVal("table_suffix", datum));
    }

}
