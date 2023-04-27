package com.cnso.flinkcdc.service;

import com.alibaba.fastjson2.JSON;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.process.ZzResultProcess;
import com.cnso.flinkcdc.util.TableUtils;
import com.cnso.flinkcdc.util.TiDBUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Create by zhengtianhao 2023-04-23 0023 19:09:06
 */
public class ETableRelationService {

    private final static Logger logger = LoggerFactory.getLogger(ETableRelationService.class);

    private static Map<String, ETableRelation> relationMap = new ConcurrentHashMap<>();

    private static List<ETableRelation> relationList = new ArrayList<>();

    private static void initRelation(){
        Connection conn = null;
        try {
            conn = TiDBUtil.getConn();
            logger.info("[search relation] conn={}", conn);
            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append("select database_name, `table_name`, new_table_name, has_eid, json_cont_eids, lock_keywords, ");
            sqlBuffer.append("other_eids, table_type from e_table_relation ");
            PreparedStatement ps = conn.prepareStatement(sqlBuffer.toString());
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()){
                ETableRelation tableRelation = new ETableRelation();
                tableRelation.setDatabaseName(resultSet.getString("database_name"));
                tableRelation.setTableName(resultSet.getString("table_name"));
                tableRelation.setNewTableName(resultSet.getString("new_table_name"));
                tableRelation.setHasEid(resultSet.getInt("has_eid"));
                tableRelation.setJsonContEids(resultSet.getString("json_cont_eids"));
                tableRelation.setLockKeywords(resultSet.getString("lock_keywords"));
                tableRelation.setOtherEids(resultSet.getString("other_eids"));
                tableRelation.setTableType(resultSet.getInt("table_type"));
                relationList.add(tableRelation);
            }
        }catch (Exception e){
            logger.error("[query table relations] exception!", e);
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                }
            }
        }
    }

    /**
     *
     * @param key
     * @return
     */
    public static ETableRelation getCurrRelation(String key){
        ETableRelation eTableRelation = relationMap.get(key);
        if (null != eTableRelation){
            return eTableRelation;
        }
        if (relationList == null || relationList.size() == 0){
            initRelation();
            logger.info("[init table relation] finish");
        }
        //库名&表名
        String[] split = key.split("\\&");
        if (split.length != 2){
            logger.error("[explain data KEY] exception-key:{}", key);
            throw new RuntimeException("[explain KEY] exception-key:"+key);
        }
        String dbName = split[0];
        String tableName = split[1];
        logger.info("[get table relations] dbName={}, tableName={}", dbName, tableName);
        if (CollectionUtils.isNotEmpty(relationList)){
            //单表
            List<ETableRelation> collect = relationList.stream().filter(item ->
                    item.getTableType() == 1
                            && tableName.equals(item.getTableName())
                            && dbName.equals(item.getDatabaseName())).collect(Collectors.toList());
            //分表
            if (CollectionUtils.isEmpty(collect)){
                collect = relationList.stream().filter(item ->
                        item.getTableType() == 2
                                && tableName.startsWith(item.getTableName())
                                && dbName.equals(item.getDatabaseName())).collect(Collectors.toList());
            }

            //分库分表
            if (CollectionUtils.isEmpty(collect)){
                collect = relationList.stream().filter(item ->
                        item.getTableType() == 3
                                && tableName.startsWith(item.getTableName())
                                && dbName.startsWith(item.getDatabaseName())).collect(Collectors.toList());

            }
            if (CollectionUtils.isNotEmpty(collect)){
                if (collect.size() == 1){
                    eTableRelation = collect.get(0);
                }else {
                    double si = 0.0;
                    for (ETableRelation tableRelation : collect) {
                        double name = TableUtils.findNameSimilarity(tableRelation.getTableName(), tableName);
                        double table = TableUtils.findNameSimilarity(tableRelation.getDatabaseName(), dbName);
                        if (si < (name+table)){
                            eTableRelation = tableRelation;
                        }
                    }
                }
            }
            if (null != eTableRelation){
                relationMap.put(key, eTableRelation);
            }
        }
        logger.info("[table relations] eTableRelation={}", eTableRelation);
        return eTableRelation;
    }

}
