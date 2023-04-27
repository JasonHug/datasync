package com.cnso.flinkcdc.service.impl;

import com.cnso.flinkcdc.service.BaseService;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-25 0025 10:59:52
 */
public class BiddingRelatedServiceImpl extends BaseService {

    @Override
    public void initInsertTmpSql(StringBuffer sqlBuf) {
        sqlBuf.append("");
    }

    @Override
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws SQLException {
    }

}
