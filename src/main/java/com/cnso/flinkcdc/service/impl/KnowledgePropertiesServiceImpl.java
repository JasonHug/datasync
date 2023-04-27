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
public class KnowledgePropertiesServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_knowledge_properties_tmp` (`id`,`eid`,`zz_eid`,`seq_no`,`number`,`name`,`type`,`pledgor`,`pawnee`,`period`,`status`,`public_date`,`row_update_time`,`pledgor_eid`,`zz_pledgor_eid`,`pawnee_eid`,`zz_pawnee_eid`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        PreparedStatementUtils.setLong(4, datum.get("seq_no"),ps);
        ps.setString(5, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("pledgor", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("pawnee", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("period", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("public_date", datum));
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(14, PreparedStatementUtils.getStringVal("pledgor_eid", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("zz_pledgor_eid", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("pawnee_eid", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("zz_pawnee_eid", datum));
        PreparedStatementUtils.setInt(18, datum.get("is_history"),ps);
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
