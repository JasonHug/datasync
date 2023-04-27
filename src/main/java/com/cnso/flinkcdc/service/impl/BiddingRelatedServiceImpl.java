package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class BiddingRelatedServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_bidding_related_tmp` (`id`,`u_id`,`zz_eid`,`eid`,`role`,`title`,`publish_time`,`area_code`,`notice_type_main`,`notice_type_sub`,`industry_code`,`project_number`,`project_bid_money`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        PreparedStatementUtils.setLong(1, datum.get("id").toString(),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("u_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        PreparedStatementUtils.setLong(5, datum.get("role").toString(),ps);
        ps.setString(6, PreparedStatementUtils.getStringVal("title", datum));
        ps.setTimestamp(7, new Timestamp(TableUtils.getLong(datum.get("publish_time").toString())));
        ps.setString(8, PreparedStatementUtils.getStringVal("area_code", datum));
        PreparedStatementUtils.setLong(9, datum.get("notice_type_main").toString(),ps);
        PreparedStatementUtils.setLong(10, datum.get("notice_type_sub").toString(),ps);
        PreparedStatementUtils.setLong(11, datum.get("industry_code").toString(),ps);
        ps.setString(12, PreparedStatementUtils.getStringVal("project_number", datum));
        ps.setBigDecimal(13, new BigDecimal(datum.get("project_bid_money").toString()));
        PreparedStatementUtils.setLong(14, datum.get("create_time").toString(),ps);
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(16, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
