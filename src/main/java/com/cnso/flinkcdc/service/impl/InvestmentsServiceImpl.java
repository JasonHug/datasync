package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.PreparedStatementUtils;
import com.cnso.flinkcdc.util.TableUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class InvestmentsServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_investments_tmp` (`id`,`eid`,`zz_eid`,`name`,`invest_eid`,`zz_invest_eid`,`ctrate_time`,`last_update_time`,`u_tags`,`row_update_time`,`stock_percentage`,`should_capi`,`invest_name`,`invest_credit_no`,`invest_reg_no`,`invest_status`,`invest_oper_name`,`invest_regist_capi`,`invest_start_date`,`belong_org`,`belong_org_code`,`invest_status_code`,`currency_code`,`should_capi_conv`,`stock_num`,`invest_quote_status`,`real_capi`,`should_con_date`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("invest_eid", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("zz_invest_eid", datum));
        PreparedStatementUtils.setLong(7, datum.get("ctrate_time"),ps);
        PreparedStatementUtils.setLong(8, datum.get("last_update_time"),ps);
        PreparedStatementUtils.setLong(9, datum.get("u_tags"),ps);
        ps.setTimestamp(10, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(11, PreparedStatementUtils.getStringVal("stock_percentage", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("should_capi", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("invest_name", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("invest_credit_no", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("invest_reg_no", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("invest_status", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("invest_oper_name", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("invest_regist_capi", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("invest_start_date", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("belong_org", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("belong_org_code", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("invest_status_code", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("currency_code", datum));
        PreparedStatementUtils.setBigDecimal(24, datum.get("should_capi_conv"), ps);
        PreparedStatementUtils.setLong(25, datum.get("stock_num"),ps);
        PreparedStatementUtils.setInt(26, datum.get("invest_quote_status"),ps);
        PreparedStatementUtils.setBigDecimal(27, datum.get("real_capi"), ps);
        ps.setString(28, PreparedStatementUtils.getStringVal("should_con_date", datum));
        PreparedStatementUtils.setInt(29, datum.get("is_history"),ps);
        ps.setTimestamp(30, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
