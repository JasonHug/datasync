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
public class CopyrightsRelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_copyrights_relations_tmp` (`id`,`eid`,`zz_eid`,`obj_id`,`ename`,`type`,`type_name`,`number`,`type_num`,`short_name`,`company`,`version`,`success_date`,`first_date`,`approval_date`,`year`,`name`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("number", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("type_num", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("short_name", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("company", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("version", datum));
        PreparedStatementUtils.setDate(13, datum.get("success_date"), ps);
        PreparedStatementUtils.setDate(14, datum.get("first_date"), ps);
        PreparedStatementUtils.setDate(15, datum.get("approval_date"), ps);
        ps.setString(16, PreparedStatementUtils.getStringVal("year", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("name", datum));
        PreparedStatementUtils.setLong(18, datum.get("created_time"),ps);
        ps.setTimestamp(19, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(20, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
