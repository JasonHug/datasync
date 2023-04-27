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
public class PrivatefundEmprecordServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_privatefund_emprecord_tmp` (`id`,`fund_no`,`eid`,`zz_eid`,`name`,`entry_date`,`leave_date`,`department`,`title`,`enterprise`,`hiseid`,`qds`,`u_tags`,`create_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("fund_no", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("entry_date", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("leave_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("department", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("title", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("enterprise", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("hiseid", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("qds", datum));
        PreparedStatementUtils.setInt(13, datum.get("u_tags"),ps);
        ps.setTimestamp(14, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(15, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(16, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
