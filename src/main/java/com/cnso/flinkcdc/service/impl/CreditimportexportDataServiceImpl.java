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
public class CreditimportexportDataServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("INSERT INTO `t_creditimportexport_data_tmp` (`id`,`ename`,`credit_no`,`customs_num`,`customs_reg`,`business_category`,`org_code`,`reg_date`,`reg_area`,`administrative_divisions`,`economic_regions`,`special_area`,`industry_type`,`vail_date`,`elect_type`,`cancel_flag`,`annual_report`,`credit_levels`,`punishments`,`eid`,`zz_eid`,`last_updated_time`,`created_time`,`md5_id`,`row_update_time`,`hg_level`,`credit_levels_new`,`nameen`,`add_cn`,`add_en`,`is_history`,`local_row_update_time`,`update_time`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        PreparedStatementUtils.setLong(1, datum.get("id"),ps);
        ps.setString(2, PreparedStatementUtils.getStringVal("ename", datum));
        ps.setString(3, PreparedStatementUtils.getStringVal("credit_no", datum));
        ps.setString(4, PreparedStatementUtils.getStringVal("customs_num", datum));
        ps.setString(5, PreparedStatementUtils.getStringVal("customs_reg", datum));
        ps.setString(6, PreparedStatementUtils.getStringVal("business_category", datum));
        ps.setString(7, PreparedStatementUtils.getStringVal("org_code", datum));
        ps.setString(8, PreparedStatementUtils.getStringVal("reg_date", datum));
        ps.setString(9, PreparedStatementUtils.getStringVal("reg_area", datum));
        ps.setString(10, PreparedStatementUtils.getStringVal("administrative_divisions", datum));
        ps.setString(11, PreparedStatementUtils.getStringVal("economic_regions", datum));
        ps.setString(12, PreparedStatementUtils.getStringVal("special_area", datum));
        ps.setString(13, PreparedStatementUtils.getStringVal("industry_type", datum));
        ps.setString(14, PreparedStatementUtils.getStringVal("vail_date", datum));
        ps.setString(15, PreparedStatementUtils.getStringVal("elect_type", datum));
        ps.setString(16, PreparedStatementUtils.getStringVal("cancel_flag", datum));
        ps.setString(17, PreparedStatementUtils.getStringVal("annual_report", datum));
        ps.setString(18, PreparedStatementUtils.getStringVal("credit_levels", datum));
        ps.setString(19, PreparedStatementUtils.getStringVal("punishments", datum));
        ps.setString(20, PreparedStatementUtils.getStringVal("eid", datum));
        ps.setString(21, PreparedStatementUtils.getStringVal("zz_eid", datum));
        ps.setString(22, PreparedStatementUtils.getStringVal("last_updated_time", datum));
        ps.setString(23, PreparedStatementUtils.getStringVal("created_time", datum));
        ps.setString(24, PreparedStatementUtils.getStringVal("md5_id", datum));
        ps.setTimestamp(25, new Timestamp(TableUtils.getLong(datum.get("row_update_time").toString())));
        ps.setString(26, PreparedStatementUtils.getStringVal("hg_level", datum));
        ps.setString(27, PreparedStatementUtils.getStringVal("credit_levels_new", datum));
        ps.setString(28, PreparedStatementUtils.getStringVal("nameen", datum));
        ps.setString(29, PreparedStatementUtils.getStringVal("add_cn", datum));
        ps.setString(30, PreparedStatementUtils.getStringVal("add_en", datum));
        PreparedStatementUtils.setInt(31, datum.get("is_history"),ps);
        ps.setTimestamp(32, new Timestamp(TableUtils.parsNanoTime(datum.get("local_row_update_time").toString())));
    }

}
