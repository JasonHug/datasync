package com.cnso.flinkcdc.model;

import com.cnso.flinkcdc.service.BaseService;

import java.util.List;

/**
 * Create by zhengtianhao 2023-04-23 0023 16:43:38
 */
public class ProcessResult {

    List<BinlogData> delList;

    List<BinlogData> insertList;

    List<BinlogData> updateList;

    String tableName;

    BaseService service;

    ETableRelation tableRelation;

    public List<BinlogData> getDelList() {
        return delList;
    }

    public void setDelList(List<BinlogData> delList) {
        this.delList = delList;
    }

    public List<BinlogData> getInsertList() {
        return insertList;
    }

    public void setInsertList(List<BinlogData> insertList) {
        this.insertList = insertList;
    }

    public List<BinlogData> getUpdateList() {
        return updateList;
    }

    public void setUpdateList(List<BinlogData> updateList) {
        this.updateList = updateList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public BaseService getService() {
        return service;
    }

    public void setService(BaseService service) {
        this.service = service;
    }

    public ETableRelation getTableRelation() {
        return tableRelation;
    }

    public void setTableRelation(ETableRelation tableRelation) {
        this.tableRelation = tableRelation;
    }
}
