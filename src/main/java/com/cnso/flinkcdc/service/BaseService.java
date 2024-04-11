package com.cnso.flinkcdc.service;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ETableRelation;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create by zhengtianhao 2023-04-23 0023 20:41:25
 */
public abstract class BaseService {

    /**
     * 批量删除
     * @param dataList
     */
    public void batchDel(ETableRelation tableRelation, List<BinlogData> dataList, Connection conn) throws Exception  {

        //封装需要删除id集合
        if (tableRelation.getTableType() == 1 && !tableRelation.getTableType().equals("t_report_details")){
            singleTableDel(tableRelation, dataList, conn);
        }else {
            splitTableDel(tableRelation, dataList, conn);
        }
    };

    /**
     * 单表删除
     * @param tableRelation
     * @param dataList
     * @param conn
     * @return
     */
    private int singleTableDel(ETableRelation tableRelation, List<BinlogData> dataList, Connection conn) throws Exception {
        List<Long> ids = new ArrayList<>();
        String pkName = "id";
        for (BinlogData binlogData : dataList) {
            List<Map<String, Object>> data = binlogData.getData();
            pkName = binlogData.getPkId();
            for (Map<String, Object> datum : data) {
                ids.add(new Long(datum.get("id").toString()));
            }
        }

        List<List<Long>> partition = Lists.partition(ids, 1000);
        StringBuffer sqlBuffer = new StringBuffer();
        sqlBuffer.append("DELETE FROM ").append(tableRelation.getNewTableName()).append(" ");
        sqlBuffer.append("WHERE `").append(pkName).append("` IN ");
        String sql = null;
        PreparedStatement ps = null;
        try{
            for (List<Long> tmpList : partition) {
                StringBuffer tmpBuffer = new StringBuffer();
                String idsStr = tmpList.stream().map(item -> {return "'"+item.toString()+"'";}).collect(Collectors.joining(","));
                tmpBuffer.append("(").append(idsStr).append(")");
                sql = sqlBuffer.toString() + tmpBuffer.toString();
                System.out.println("[delete sql] sql="+sql);
                ps = conn.prepareStatement(sql);
                int num = ps.executeUpdate(sql);
            }
        }finally {
            if(ps != null){
                ps.close();
            }
        }
        return ids.size();
    }

    /**
     * 分库、分表
     * @param tableRelation
     * @param dataList
     * @param conn
     * @return
     */
    private int splitTableDel(ETableRelation tableRelation, List<BinlogData> dataList, Connection conn) throws Exception {
        List<String> zzIds = new ArrayList<>();
        for (BinlogData binlogData : dataList) {
            List<Map<String, Object>> data = binlogData.getData();
            for (Map<String, Object> datum : data) {
                zzIds.add(datum.get("zz_id").toString());
            }
        }

        List<List<String>> partition = Lists.partition(zzIds, 1000);
        StringBuffer sqlBuffer = new StringBuffer();
        sqlBuffer.append("DELETE FROM ").append(tableRelation.getNewTableName()).append(" ");
        sqlBuffer.append("WHERE `zz_id` IN ");
        String sql = null;
        PreparedStatement ps = null;
        try{
            for (List<String> tmpList : partition) {
                StringBuffer tmpBuffer = new StringBuffer();
                String idsStr = tmpList.stream().map(item -> {return "'"+item+"'";}).collect(Collectors.joining(","));
                tmpBuffer.append("(").append(idsStr).append(")");
                sql = sqlBuffer.toString() + tmpBuffer.toString();
                System.out.println("[delete sql] sql="+sql);
                ps = conn.prepareStatement(sql);
                int num = ps.executeUpdate(sql);
                System.out.println("[delete size] size = "+num);
            }
        }finally {
            if(ps != null){
                ps.close();
            }
        }
        return zzIds.size();
    }

    /**
     * 使用内存表更新
     * @param tableRelation
     * @param list
     * @param conn
     */
    public int batchInsertOrUpdate(ETableRelation tableRelation, List<BinlogData> list, Connection conn) throws Exception{
        PreparedStatement insertPs = null;
        Statement updateStatement = null;
        try {
            StringBuffer sqlBuf = new StringBuffer();
            String memTable = tableRelation.getNewTableName() + "_memory";
            initInsertTmpSql(sqlBuf, memTable);
            insertPs = conn.prepareStatement(sqlBuf.toString());
            for (int i = 0; i < list.size(); i++){
                BinlogData binlogData = list.get(i);
                List<Map<String, Object>> data = binlogData.getData();
                for (Map<String, Object> datum: data) {
                    System.out.println("[update sql] sql=" + datum);
                    initPs(insertPs, datum);
                    insertPs.addBatch();
                }
            }
            insertPs.executeBatch();

            // 内存表更新数据
            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(" replace into ").append(tableRelation.getNewTableName()).append(" ");
            sqlBuffer.append("select tmp.* from").append(memTable).append(" tmp ");
            updateStatement = conn.createStatement();
            return updateStatement.executeUpdate(sqlBuffer.toString());
        }finally {
            if(insertPs!=null)
                insertPs.close();
            if(updateStatement!=null)
                updateStatement.close();
        }
    }

    /**
     * 初始化插入临时表语句
     * @param sqlBuf
     * @param memTable
     */
    private void initInsertTmpSql(StringBuffer sqlBuf, String memTable) {
        throw new NotImplementedException();
    }

    /**
     * 批量插入到临时表里
     * @param list
     */
    public int batchInsertTmp(List<BinlogData> list,Connection conn) throws Exception  {
        System.out.println("[start insert table tmp]");
        PreparedStatement ps = null;
        StringBuffer sqlBuf = new StringBuffer();
        initInsertTmpSql(sqlBuf);
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(sqlBuf.toString());
            for (int i = 0; i < list.size(); i++) {
                BinlogData binlogData = list.get(i);
                List<Map<String, Object>> data = binlogData.getData();
                for (Map<String, Object> datum : data) {
                    initPs(ps, datum);
                    ps.addBatch();
                }
                if (i % 2000 == 0 || (i+1) == list.size()){
                    ps.executeBatch();
                    conn.commit();
                    ps.clearBatch();
                }
            }
        }catch (Exception e){
            throw e;
        }finally {
            if(ps != null){
                ps.close();
            }
            conn.setAutoCommit(true);
        }
        System.out.println("[insert table finish] size="+list.size());
        return list.size();
    }

    /**
     * 初始化插入临时表语句
     * @param sqlBuf
     */
    public void initInsertTmpSql(StringBuffer sqlBuf){
        throw new NotImplementedException();
    };

    /**
     * 初始化
     * @param ps
     * @param datum
     * @throws SQLException
     */
    public void initPs(PreparedStatement ps, Map<String, Object> datum) throws Exception {
        throw new NotImplementedException();
    };

    /**
     * 根据临时表里数据做数据更新
     * @return
     */
    public int batchUpdateFromTmp(String tableName,Connection conn) throws Exception  {
        StringBuffer sqlBuffer = new StringBuffer();
        sqlBuffer.append(" replace into ").append(tableName).append(" ");
        sqlBuffer.append("select ");;
        sqlBuffer.append("    tmp.* ");
        sqlBuffer.append("from ").append(tableName).append("_tmp tmp ");
        System.out.println("[replace into sql] sql:"+sqlBuffer.toString());
        Statement ps = null;
        try {
            ps =  conn.createStatement();
            return ps.executeUpdate(sqlBuffer.toString());
        }finally {
            if(ps != null ){
                ps.close();
            }
        }
    }

    /**
     * 清空临时表
     */
    public int clearInsertTmp(String tableName,Connection conn) throws Exception {
        PreparedStatement ps = null;
        try{
            StringBuffer sqlBuf = new StringBuffer();
            sqlBuf.append("delete from ").append(tableName).append("_tmp");
            ps = conn.prepareStatement(sqlBuf.toString());
            return ps.executeUpdate();
        }finally {
            if(ps != null ){
                ps.close();
            }
        }

    }

}
