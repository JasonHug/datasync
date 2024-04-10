package com.cnso.flinkcdc.model;

import java.util.Date;

/**
 * Create by Zyy 2023-04-23 0023 17:44:34
 */
public class ETableRelation {
    /**
     * 主键
     */
    private Long id;

    /**
     * 数据库名称
     */
    private String databaseName;

    /**
     * 企信宝表名称/前缀（如果有分表，则为表名称的前缀）
     */
    private String tableName;

    /**
     * 新表名（合并分库、分表的表名称）
     */
    private String newTableName;

    /**
     * 是否有EID列, 1 有， 0 无
     */
    private Integer hasEid;

    /**
     * json内容中的eid的,可以设置多个 如：列名1:属性1,属性2|列名2:属性1,属性2
     */
    private String jsonContEids;

    /**
     * 多线程锁keyword,多个以 ',' 隔开
     */
    private String lockKeywords;

    /**
     * 列名不是eid，值为eid内容的列名，以 ',' 隔开
     */
    private String otherEids;

    /**
     * 1 单表，2 分表， 3 分库分表
     */
    private Integer tableType;

    /**
     * 描述
     */
    private String remark;

    /**
     * 创建临时表SQL语句
     */
    private String memoryTableSql;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public void setNewTableName(String newTableName) {
        this.newTableName = newTableName;
    }

    public Integer getHasEid() {
        return hasEid;
    }

    public void setHasEid(Integer hasEid) {
        this.hasEid = hasEid;
    }

    public String getJsonContEids() {
        return jsonContEids;
    }

    public void setJsonContEids(String jsonContEids) {
        this.jsonContEids = jsonContEids;
    }

    public String getLockKeywords() {
        return lockKeywords;
    }

    public void setLockKeywords(String lockKeywords) {
        this.lockKeywords = lockKeywords;
    }

    public String getOtherEids() {
        return otherEids;
    }

    public void setOtherEids(String otherEids) {
        this.otherEids = otherEids;
    }

    public Integer getTableType() {
        return tableType;
    }

    public void setTableType(Integer tableType) {
        this.tableType = tableType;
    }

    public String getRemark() {
        return remark;
    }

    public String getMemoryTableSql() {
        return memoryTableSql;
    }

    public void setMemoryTableSql(String memoryTableSql) {
        this.memoryTableSql = memoryTableSql;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
