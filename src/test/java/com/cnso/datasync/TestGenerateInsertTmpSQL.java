package com.cnso.datasync;

import com.cnso.datasync.domain.TableInfo;
import com.cnso.flinkcdc.util.TiDBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Create by zhengtianhao 2023-04-25 0025 09:26:13
 */
public class TestGenerateInsertTmpSQL {

    public static void main(String[] args) throws SQLException {

        String tableName = "t_bidding_content_other";


        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://10.172.21.195:3306/test_enterprise?characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true&useServerPrepStmts=true&allowMultiQueries=true";
        String user = "imaster";
        String pwd = "vjfrALHac*Rp2i^x";
        Connection conn = null;
        try {
            TiDBUtil.init(driver, url, user, pwd);
            conn = TiDBUtil.getConn();
            createCode(tableName, conn);
        }finally {
            conn.close();
        }
    }

    private static void createCode(String tableName, Connection conn) throws SQLException {
        System.out.println("---table:"+tableName+"---");
        PreparedStatement ps = null;
        try {
            StringBuffer sqlBuff = new StringBuffer();
            sqlBuff.append("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE ");
            sqlBuff.append("from information_schema.`COLUMNS` WHERE TABLE_SCHEMA = 'test_enterprise' AND TABLE_NAME = '");
            sqlBuff.append(tableName).append("' ORDER BY ORDINAL_POSITION");
            ps = conn.prepareStatement(sqlBuff.toString());
            ResultSet resultSet = ps.executeQuery();
            List<TableInfo> tableInfos = new ArrayList<>();
            while (resultSet.next()){
                TableInfo tableInfo = new TableInfo();
                tableInfo.setTableName(resultSet.getString("TABLE_NAME"));
                tableInfo.setColumnName(resultSet.getString("COLUMN_NAME"));
                tableInfo.setDataType(resultSet.getString("DATA_TYPE"));
                tableInfos.add(tableInfo);
            }
            StringBuffer insertSql = new StringBuffer();
            insertSql.append("INSERT INTO `").append(tableName).append("_tmp` ");
            insertSql.append("(").append(tableInfos.stream().map(item -> {return "`"+item.getColumnName()+"`";}).collect(Collectors.joining(","))).append(")");
            insertSql.append(" VALUES ");
            insertSql.append("(").append(tableInfos.stream().map(item -> {
                if (item.getColumnName().equals("update_time") && item.getDataType().equals("datetime")){
                    return "NOW()";
                }else {
                    return "?";
                }
            }).collect(Collectors.joining(","))).append(")");

            System.out.println("--插入语句--");
            System.out.println(insertSql.toString());
            System.out.println("--------ps---------");
            int position = 0;
            for (int i = 0; i < tableInfos.size(); i++) {
                TableInfo tableInfo = tableInfos.get(i);
                String column = "\""+tableInfo.getColumnName()+"\"";
                if (tableInfo.getColumnName().equals("update_time") && tableInfo.getDataType().equals("datetime")){
                }else {
                    position++;
                }
                switch (tableInfo.getDataType().toLowerCase(Locale.ROOT)){
                    case "varchar":
                    case "char":
                    case "text":
                    case "mediumtext":
                    case "longtext":
                        System.out.println("ps.setString("+position+", PreparedStatementUtils.getStringVal("+column+", datum));");
                        break;
                    case "bigint":
                    case "int":
                        System.out.println("PreparedStatementUtils.setLong("+position+", datum.get("+column+"),ps);");
                        break;
                    case "tinyint":
                        System.out.println("PreparedStatementUtils.setInt("+position+", datum.get("+column+"),ps);");
                        break;
                    case "double":
                        System.out.println("PreparedStatementUtils.setDouble("+position+", datum.get("+column+"), ps);");
                        break;
                    case "decimal":
                        System.out.println("PreparedStatementUtils.setBigDecimal("+position+", datum.get("+column+"), ps);");
                        break;
                    case "date":
                        System.out.println("PreparedStatementUtils.setDate("+position+", datum.get("+column+"), ps);");
                        break;
                    case "datetime":
                    case "timestamp":
                        if (tableInfo.getColumnName().equals("update_time") && tableInfo.getDataType().equals("datetime")){
                            break;
                        }
                        if (tableInfo.getColumnName().equals("local_row_update_time")){
                            System.out.println("ps.setTimestamp("+position+", new Timestamp(TableUtils.parsNanoTime(datum.get("+column+").toString())));");
                        }else {
                            System.out.println("ps.setTimestamp("+position+", new Timestamp(TableUtils.getLong(datum.get("+column+").toString())));");
                        }
                        break;
                    default:
                        System.err.println("not match column:"+tableInfo.getColumnName()+" type:"+tableInfo.getDataType());
                }
            }
            System.out.println();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            if(null != ps){
                System.out.println("close ps");
                ps.close();
            }
            if (null != conn){
                System.out.println("close conn");
                conn.close();
            }
        }
    }
}
