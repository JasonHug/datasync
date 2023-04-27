package com.cnso.datasync.domain;

/**
 * Create by zhengtianhao 2023-04-25 0025 11:27:30
 */
public class TableInfo {

    String tableName;

    String columnName;

    String dataType;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
