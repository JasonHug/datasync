package com.cnso.flinkcdc.model;

import java.util.List;
import java.util.Map;

/**
 * @author jia.xd
 * @date 2023/2/13
 */
public class BinlogData {

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    String topic;

    Long offset;

    /**
     * 修改数据
     */
    List<Map<String,Object>> data;

    /**
     * 数据库名
     */
    String database;

    Long es;

    /**
     * 主键Id
     */
    Long id;

    /**
     * 是否是ddl
     */
    Boolean isDdl;

    /**
     * mysql对应类型
     */
    Map<String, String> mysqlType;

    /**
     * 原数据
     */
    List<Map<String, String>> old;

    /**
     * 主键列名
     */
    List<String> pkNames;

    /**
     * sql语句
     */
    String sql;

    /**
     * sql
     */
    Map<String, String> sqlType;

    /**
     * 数据库表名
     */
    String table;

    Long ts;

    /**
     * 主键执行类型
     */
    String type;

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, String> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, String> sqlType) {
        this.sqlType = sqlType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
