package com.cnso.flinkcdc.service;

import com.alibaba.fastjson2.JSON;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.util.ETableRelationUtils;
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

/**
 * Create by Zyy 2023-04-23 0023 19:09:06
 *
 * CREATE TABLE `e_table_relation` (
 * id bigint(0) AUTO_INCREMENT COMMENT '主键',
 * database_name varchar(100) NOT NULL COMMENT '数据库名称',
 * table_name varchar(100) NOT NULL COMMENT 'qxb表名称/前缀 如果有分表 则为表名称前缀',
 * new_table_name varchar(100) NOT NULL COMMENT '新表名:合并分库分表后的表名称',
 * has_eid tinyint(0) NOT NULL COMMENT '是否有Eid列1有0无',
 * json_cont_eid varchar(100) COMMENT 'json中的eid,可以设置多个 列名1:属性a,属性b|列名2:属性aa,属性bb',
 * lock_keywords varchar(200) COMMENT '多线程锁keywords,多个以逗号隔开',
 * other_eids varchar(200) COMMENT '列名不是eid 值为eid内容的列名 逗号隔开',
 * table_type tinyint(0) COMMENT '1单表2分表3分库分表',
 * remark varchar(200) COMMENT '描述',
 * memory_table_sql text COMMENT '创建mem表的sql语句',
 * create text COMMENT '创建mem表的sql语句',
 * memory_table_sql text COMMENT '创建mem表的sql语句',
 * create_time datetime,
 * update_time datetime,
 * unique key `unique_idx` (database_name, table_name)
 * )
 */
public class ETableRelationService {

    private final static Logger logger = LoggerFactory.getLogger(ETableRelationService.class);

    private synchronized static void initRelation() {
        Connection conn = null;
        try {
            if (CollectionUtils.isNotEmpty(ETableRelationUtils.RELATION_LIST)) return;

            // ETableRelationUtils的数据关系为空，需要初始化
            conn = TiDBUtil.getConn();
            logger.info("[search relation] conn={}", conn);
            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append("select database_name, `table_name`, new_table_name, has_eid, json_cont_eids, lock_keywords, ");
            sqlBuffer.append("other_eids, table_type from e_table_relation ");
            PreparedStatement ps = conn.prepareStatement(sqlBuffer.toString());
            ResultSet resultSet = ps.executeQuery();

            // 加载配置表
            ArrayList<ETableRelation> relations = new ArrayList<>();
            while (resultSet.next()) {
                ETableRelation tableRelation = new ETableRelation();
                tableRelation.setDatabaseName(resultSet.getString("database_name"));
                tableRelation.setTableName(resultSet.getString("table_name"));
                tableRelation.setNewTableName(resultSet.getString("new_table_name"));
                tableRelation.setHasEid(resultSet.getInt("has_eid"));
                tableRelation.setJsonContEids(resultSet.getString("json_cont_eids"));
                tableRelation.setLockKeywords(resultSet.getString("lock_keywords"));
                tableRelation.setOtherEids(resultSet.getString("other_eids"));
                tableRelation.setTableType(resultSet.getInt("table_type"));
                relations.add(tableRelation);
            }

            ETableRelationUtils.initList(relations);
        } catch (Exception e) {
            logger.error("[query table relations] exception!", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                }
            }
            logger.info("[init relation table] Finish Relation_list");
        }
    }

    /**
     * 库名&表名 库名&表名_1 库名_1&表名_1 作为主键
     * @param key
     * @return
     */
    public static ETableRelation getCurrRelation(String key) {
        // Map初始为空
        ETableRelation eTableRelation = ETableRelationUtils.RELATION_MAP.get(key);

        // 加载service类的时候已经初始化
        List<ETableRelation> DBTablesConfig = ETableRelationUtils.RELATION_LIST;
        if (null != eTableRelation) {
            return eTableRelation;
        }

        if (CollectionUtils.isEmpty(DBTablesConfig)) {
            initRelation();
        }

        //库名&表名
        String[] split = key.split("\\&");
        if (split.length != 2) {
            logger.error("[explain data KEY] exception-key:{}", key);
            throw new RuntimeException("[explain KEY] exception-key:" + key);
        }

        String dbName = split[0];
        String tableName = split[1];
        logger.info("[get table relations] dbName={}, tableName={}", dbName, tableName);

        // 将本次窗口涉及到的 库&表 都放进来
        ArrayList<ETableRelation> collect = new ArrayList<>();
        // 单表
        for (int i = 0; i < DBTablesConfig.size(); i++) {
            ETableRelation item = DBTablesConfig.get(i);
            if (null == item)
                logger.info("[Error], {}, {}", JSON.toJSONString(DBTablesConfig), DBTablesConfig.size());

            if (item.getTableType() == 1 && tableName.equals(item.getTableName()) && dbName.equals(item.getDatabaseName()))
                collect.add(item);
        }

        // 分表
        if (CollectionUtils.isEmpty(collect)) {
            for (int i = 0; i < DBTablesConfig.size(); i++) {
                ETableRelation item = DBTablesConfig.get(i);
                if (null == item)
                    logger.info("[Error], {}", JSON.toJSONString(DBTablesConfig));

                if (item.getTableType() == 2 && tableName.equals(item.getTableName()) && dbName.equals(item.getDatabaseName()))
                    collect.add(item);
            }
        }

        // 分库分表
        if (CollectionUtils.isEmpty(collect)) {
            for (int i = 0; i < DBTablesConfig.size(); i++) {
                ETableRelation item = DBTablesConfig.get(i);
                if (null == item)
                    logger.info("[Error], {}", JSON.toJSONString(DBTablesConfig));

                if (item.getTableType() == 3 && tableName.equals(item.getTableName()) && dbName.equals(item.getDatabaseName()))
                    collect.add(item);
            }
        }

        if (CollectionUtils.isNotEmpty(collect)) {
            if (collect.size() == 1) {
                eTableRelation = collect.get(0);
            } else {
                // 相似算法判断分库分表
                double si = 0.0;
                for (ETableRelation tableRelation :
                        collect) {
                    double db = TableUtils.findNameSimilarity(tableRelation.getDatabaseName(), dbName);
                    double table = TableUtils.findNameSimilarity(tableRelation.getTableName(), tableName);
                    if (si < (table + db))
                        eTableRelation = tableRelation;
                }
            }
        }

        if (null != eTableRelation)
            ETableRelationUtils.RELATION_MAP.put(key, eTableRelation);

        logger.info("[table relations] eTableRelation={}", eTableRelation);
        return eTableRelation;
    }

}
