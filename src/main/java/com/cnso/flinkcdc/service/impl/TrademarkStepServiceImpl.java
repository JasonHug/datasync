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
public class TrademarkStepServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_trademark_step_tmp` (`zz_id`,`id`,`reg_number`,`type_num`,`eid`,`zz_eid`,`business_name`,`link_name`,`conclusion`,`process_date`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id"),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("reg_number", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("type_num", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("business_name", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("link_name", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("conclusion", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("process_date", datum));
        ps.setTimestamp(11, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(12, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(13, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
