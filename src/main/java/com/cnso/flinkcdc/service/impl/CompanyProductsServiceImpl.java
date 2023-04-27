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
public class CompanyProductsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_company_products_tmp` (`id`,`proj_id`,`eid`,`zz_eid`,`ename`,`pro_name`,`kind`,`description`,`domain`,`links`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("proj_id", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("pro_name", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("kind", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("description", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("domain", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("links", datum));
        ps.setTimestamp(11, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
