package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class BiddingInfoServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_bidding_info_tmp` (`zz_id`,`id`,`title`,`publish_time`,`area_code`,`notice_type_main`,`notice_type_sub`,`industry_code`,`project_name`,`project_number`,`project_budget_money`,`project_time_limit`,`entry_start_time`,`entry_end_time`,`bid_open_time`,`proprietor_company`,`agency_company`,`winner_company`,`winner_candidate`,`tenderer`,`related_construction`,`project_fund_source`,`attachment_oss`,`url`,`crawl_time`,`s_id`,`u_id`,`u_tags`,`qds`,`date_year`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("title", datum));
        ps.setTimestamp(4, new Timestamp(TableUtils.getLong(datum.get("publish_time").toString())));
        ps.setString(5, PreparedStatementUtils.getStringVal("area_code", datum));
        PreparedStatementUtils.setLong(6, datum.get("notice_type_main"),ps);
        PreparedStatementUtils.setLong(7, datum.get("notice_type_sub"),ps);
        PreparedStatementUtils.setLong(8, datum.get("industry_code"),ps);
        ps.setString(9, PreparedStatementUtils.getStringVal("project_name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("project_number", datum));
        PreparedStatementUtils.setBigDecimal(11, datum.get("project_budget_money"), ps);
        ps.setString(12, PreparedStatementUtils.getStringVal("project_time_limit", datum));
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("entry_start_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.getLong(datum.get("entry_end_time").toString())));
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("bid_open_time").toString())));
        ps.setString(16, PreparedStatementUtils.getStringVal("proprietor_company", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("agency_company", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("winner_company", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("winner_candidate", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("tenderer", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("related_construction", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("project_fund_source", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("attachment_oss", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("url", datum));
        ps.setTimestamp(25, new Timestamp(TableUtils.getLong(datum.get("crawl_time").toString())));
        ps.setString(26, PreparedStatementUtils.getStringVal("s_id", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("u_id", datum));
        PreparedStatementUtils.setInt(28, datum.get("u_tags"),ps);
        ps.setString(29, PreparedStatementUtils.getStringVal("qds", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("date_year", datum));
        PreparedStatementUtils.setLong(31, datum.get("create_time"),ps);
        ps.setTimestamp(32, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(33, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
