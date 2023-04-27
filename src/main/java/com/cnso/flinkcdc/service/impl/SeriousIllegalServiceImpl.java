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
public class SeriousIllegalServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_serious_illegal_tmp` (`id`,`eid`,`zz_eid`,`execution`,`executed_person`,`major_tax_violatio`,`in_date`,`in_department`,`ill_type`,`in_reason`,`out_date`,`out_reason`,`out_department`,`is_history`,`is_cancel`,`organization_code`,`create_time`,`row_update_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("execution", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("executed_person", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("major_tax_violatio", datum));
        PreparedStatementUtils.setDate(7, datum.get("in_date"), ps);
        ps.setString(8, PreparedStatementUtils.getStringVal("in_department", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("ill_type", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("in_reason", datum));
        PreparedStatementUtils.setDate(11, datum.get("out_date"), ps);
        ps.setString(12, PreparedStatementUtils.getStringVal("out_reason", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("out_department", datum));
        PreparedStatementUtils.setInt(14, datum.get("is_history"),ps);
        PreparedStatementUtils.setInt(15, datum.get("is_cancel"),ps);
        ps.setString(16, PreparedStatementUtils.getStringVal("organization_code", datum));
        ps.setTimestamp(17, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(18, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(19, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(20, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
