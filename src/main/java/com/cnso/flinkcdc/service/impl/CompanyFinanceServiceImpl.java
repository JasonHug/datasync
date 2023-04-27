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
public class CompanyFinanceServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_company_finance_tmp` (`id`,`eid`,`zz_eid`,`ename`,`round_date`,`round`,`round_type`,`amount`,`estimated_amount`,`pre_money`,`post_money`,`currency`,`precise`,`investors`,`investors_json`,`publish_date`,`newstitle`,`newslink`,`create_time`,`local_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("round_date", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("round", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("round_type", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("amount", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("estimated_amount", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("pre_money", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("post_money", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("currency", datum));
        PreparedStatementUtils.setInt(13, datum.get("precise"),ps);
        ps.setString(14, PreparedStatementUtils.getStringVal("investors", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("investors_json", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("publish_date", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("newstitle", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("newslink", datum));
        ps.setTimestamp(19, new Timestamp(TableUtils.getLong(datum.get("create_time").toString())));
        ps.setTimestamp(20, new Timestamp(TableUtils.getLong(datum.get("local_update_time").toString())));
        ps.setTimestamp(21, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
