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
public class AuctionsRelationsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_auctions_relations_tmp` (`id`,`eid`,`zz_eid`,`obj_id`,`ename`,`role`,`type`,`full_name`,`name`,`owner`,`court`,`date`,`related_companies`,`start_price`,`est_price`,`_restrict`,`certificate`,`basis`,`description`,`order_date`,`created_time`,`row_update_time`,`transaction_date`,`transaction_price`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("obj_id", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("role", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("full_name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("owner", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("court", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("related_companies", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("start_price", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("est_price", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("_restrict", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("certificate", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("basis", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("description", datum));
        PreparedStatementUtils.setDate(20, datum.get("order_date"), ps);
        PreparedStatementUtils.setLong(21, datum.get("created_time"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(23, PreparedStatementUtils.getStringVal("transaction_date", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("transaction_price", datum));
        ps.setTimestamp(25, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
