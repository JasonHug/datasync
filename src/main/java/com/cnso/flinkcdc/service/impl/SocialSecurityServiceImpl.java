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
public class SocialSecurityServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_social_security_tmp` (`id`,`eid`,`zz_eid`,`name`,`report_year`,`report_name`,`report_date`,`bq_shengyubx_je`,`dw_je_display`,`dw_yanglaobx_je`,`bq_shiyebx_je`,`dw_yiliaobx_je`,`shiyebx_num`,`dw_yanglaobx_js`,`dw_gongshangbx_js`,`yiliaobx_num`,`dw_shengyubx_je`,`dw_js_display`,`dw_shengyubx_js`,`bq_yiliaobx_je`,`bq_gongshangbx_je`,`dw_gongshangbx_je`,`shengyubx_num`,`dw_shiyebx_js`,`dw_shiyebx_je`,`bq_je_display`,`gongshangbx_num`,`yanglaobx_num`,`bq_yanglaobx_je`,`dw_yiliaobx_js`,`currency`,`created_time`,`row_update_time`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("name", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("report_year", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("report_name", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("report_date", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("bq_shengyubx_je", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("dw_je_display", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("dw_yanglaobx_je", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("bq_shiyebx_je", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("dw_yiliaobx_je", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("shiyebx_num", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("dw_yanglaobx_js", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("dw_gongshangbx_js", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("yiliaobx_num", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("dw_shengyubx_je", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("dw_js_display", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("dw_shengyubx_js", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("bq_yiliaobx_je", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("bq_gongshangbx_je", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("dw_gongshangbx_je", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("shengyubx_num", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("dw_shiyebx_js", datum));
        ps.setString(25, PreparedStatementUtils.getStringVal("dw_shiyebx_je", datum));
        ps.setString(26, PreparedStatementUtils.getStringVal("bq_je_display", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("gongshangbx_num", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("yanglaobx_num", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("bq_yanglaobx_je", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("dw_yiliaobx_js", datum));
        ps.setString(31, PreparedStatementUtils.getStringVal("currency", datum));
        PreparedStatementUtils.setLong(32, datum.get("created_time"),ps);
        ps.setTimestamp(33, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setTimestamp(34, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
