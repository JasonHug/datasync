package com.cnso.flinkcdc.service.impl;

import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;
import com.cnso.flinkcdc.util.TiDBUtil;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create by zhengtianhao 2023-04-23 0023 20:42:01
 */
public class CreditNoIndexFullServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf){
        sqlBuf.append("INSERT INTO `t_credit_no_index_full_tmp` (`zz_id`, `id`, `credit_no`, `eid`, `zz_eid`, ");
        sqlBuf.append("`province`, `reg_no`, `org_no`, `name`, `status`, `oper_name`, `start_date`, `history_names`, ");
        sqlBuf.append("`his_eids`, `data`, `number`, `update_time`, `row_update_time`, `local_row_update_time`) ");
        sqlBuf.append(" VALUES ");
        sqlBuf.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
        ps.setString(1, PreparedStatementUtils.getStringVal("zz_id", datum));
        PreparedStatementUtils.setLong(2, datum.get("id").toString(),ps);
        ps.setString(3, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("province", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("reg_no", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("org_no", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("status", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("oper_name", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("start_date", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("history_names", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("his_eids", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("data", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("number", datum));
        PreparedStatementUtils.setLong(17, datum.get("update_time").toString(), ps);
        ps.setTimestamp(18, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(19, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }
}
