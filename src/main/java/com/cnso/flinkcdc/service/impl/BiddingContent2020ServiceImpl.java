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
public class BiddingContent2020ServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_bidding_content_2020_tmp` (`id`,`content_text`,`content_html`,`content_swf`,`u_id`,`u_tags`,`qds`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("content_text", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("content_html", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("content_swf", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("u_id", datum));
        PreparedStatementUtils.setInt(6, datum.get("u_tags"),ps);
        ps.setString(7, PreparedStatementUtils.getStringVal("qds", datum));
        PreparedStatementUtils.setLong(8, datum.get("create_time"),ps);
        ps.setTimestamp(9, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(10, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
