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
public class ArticleEnterpriseServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_article_enterprise_tmp` (`zz_id`,`id`,`eid`,`zz_eid`,`oid`,`title`,`source`,`url`,`class_type`,`pub_time`,`neg_index`,`sentiment`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("oid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("source", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("url", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("class_type", datum));
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("pub_time").toString())));
        PreparedStatementUtils.setBigDecimal(11, datum.get("neg_index"), ps);
        ps.setString(12, PreparedStatementUtils.getStringVal("sentiment", datum));
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(14, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(15, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
