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
public class ArticleMiningServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_article_mining_tmp` (`zz_id`,`id`,`oid`,`title`,`pub_time`,`brief`,`source`,`rank`,`url`,`class_type`,`keywords`,`keywords_desc`,`names`,`companies`,`neg_index`,`sentiment`,`company_nlps`,`theme_keywords`,`topic`,`eid_info`,`u_tags`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("oid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("title", datum));
        ps.setTimestamp(5, new Timestamp(TableUtils.getLong(datum.get("pub_time").toString())));
        ps.setString(6, PreparedStatementUtils.getStringVal("brief", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("source", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("rank", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("class_type", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("keywords", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("keywords_desc", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("names", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("companies", datum));
        PreparedStatementUtils.setBigDecimal(15, datum.get("neg_index"), ps);
        ps.setString(16, PreparedStatementUtils.getStringVal("sentiment", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("company_nlps", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("theme_keywords", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("topic", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("eid_info", datum));
        PreparedStatementUtils.setInt(21, datum.get("u_tags"),ps);
        ps.setTimestamp(22, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(23, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(24, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
