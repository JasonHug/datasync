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
public class AbnormalServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_abnormal_tmp` (`id`,`eid`,`zz_eid`,`name`,`reg_no`,`province`,`in_reason`,`in_date`,`out_reason`,`out_date`,`department`,`last_update_time`,`row_update_time`,`out_department`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("in_reason", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("in_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("out_reason", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("out_date", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("department", datum));
        PreparedStatementUtils.setLong(12, datum.get("last_update_time"),ps);
        ps.setTimestamp(13, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(14, PreparedStatementUtils.getStringVal("out_department", datum));
        ps.setTimestamp(15, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
