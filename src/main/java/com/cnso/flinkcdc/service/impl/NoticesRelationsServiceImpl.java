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
public class NoticesRelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_notices_relations_tmp` (`id`,`eid`,`zz_eid`,`obj_id`,`ename`,`type`,`type_code`,`type_name`,`people`,`people_ori`,`role`,`court`,`url`,`date`,`related_companies`,`related_companies_ori`,`content`,`content_ori`,`year_date`,`created_time`,`order_date`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("type", datum));
        PreparedStatementUtils.setInt(7, datum.get("type_code"),ps);
        ps.setString(8, PreparedStatementUtils.getStringVal("type_name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("people", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("people_ori", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("role", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("url", datum));
        PreparedStatementUtils.setDate(14, datum.get("date"), ps);
        ps.setString(15, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("related_companies_ori", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("content", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("content_ori", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("year_date", datum));
        PreparedStatementUtils.setLong(20, datum.get("created_time"),ps);
        PreparedStatementUtils.setDate(21, datum.get("order_date"), ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
