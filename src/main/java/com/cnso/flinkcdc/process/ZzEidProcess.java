package com.cnso.flinkcdc.process;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.util.EIdUtils;
import com.cnso.flinkcdc.util.TableUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-23 0023 17:28:33
 */
public class ZzEidProcess {

    //处理EID
    public void processEid(ETableRelation tableRelation, Map<String, List<BinlogData>> dataMap){
        ZzIdProcess zzIdProcess = new ZzIdProcess();
        for (Map.Entry<String, List<BinlogData>> entry : dataMap.entrySet()) {
            for (BinlogData binlogData : entry.getValue()) {
                execEid(tableRelation, binlogData);
                //处理主键ID
                zzIdProcess.processZzId(binlogData, tableRelation);
            }
        }
    }

    private void execEid(ETableRelation tableRelation, BinlogData binlogData){

        List<Map<String, Object>> dataList = binlogData.getData();
        if (CollectionUtils.isNotEmpty(dataList)){
            //无eid表不处理zz_eid
            generatorEid(dataList, tableRelation);
            //解析其它eid
            processOthersEid(dataList, tableRelation);
            //解析JSON中eid
            processJsonEid(dataList, tableRelation);
        }

        //generatorTableInfo(binlogData);

    }

    /**
     * 生成企业对应eid
     * @param data
     */
    private void generatorZzId(List<Map<String, Object>> data, ETableRelation tableRelation) {
        //解析EID
        for (Map<String, Object> map : data) {
            //解析其它eid
            String eid = ObjectUtils.isEmpty(map.get("eid")) ? null : map.get("eid").toString();
            map.put("zz_eid", EIdUtils.generateEid(eid));
        }
    }

    /**
     * 生成企业对应eid
     * @param data
     */
    private void generatorEid(List<Map<String, Object>> data, ETableRelation tableRelation) {
        //解析EID
        if (tableRelation.getHasEid() == 1){
            for (Map<String, Object> map : data) {
                //解析其它eid
                String eid = ObjectUtils.isEmpty(map.get("eid")) ? null : map.get("eid").toString();
                map.put("zz_eid", EIdUtils.generateEid(eid));
            }
        }
    }

    private void generatorTableInfo(BinlogData binlogData) {
        List<Map<String, Object>> dataList = binlogData.getData();
        if (CollectionUtils.isNotEmpty(dataList)){
            for (Map<String, Object> map : dataList) {
                //增加表后缀、库名称
                String suffix = TableUtils.getSuffix(binlogData.getTable());
                if (StringUtils.isNotEmpty(suffix)){
                    map.put("table_suffix", suffix);
                    map.put("date_year", suffix);
                }
                if (StringUtils.isNotEmpty(binlogData.getDatabase())){
                    map.put("db_name", binlogData.getDatabase());
                }
            }
        }
    }

    /**
     * 处理数据列为eid数据，列名不是eid
     * @param data
     * @param tableRelation
     */
    private void processOthersEid(List<Map<String, Object>> data, ETableRelation tableRelation){

        //判断表是否存在不是eid的，其它列名称
        if (ObjectUtils.isNotEmpty(tableRelation) && StringUtils.isNotEmpty(tableRelation.getOtherEids())){
            String[] columns = tableRelation.getOtherEids().split(",");
            //处理列名不是eid的列
            for (String column : columns) {
                if (StringUtils.isNotEmpty(column)){
                    if (CollectionUtils.isNotEmpty(data)){
                        for (Map<String, Object> datum : data) {
                            String eid = ObjectUtils.isNotEmpty(datum.get(column)) ? datum.get(column).toString() : null;
                            if (StringUtils.isEmpty(eid)){
                                datum.put("zz_"+column, null == eid ? "" : EIdUtils.generateEid(eid));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析JSON中的eid
     * @param data
     * @param tableRelation
     */
    private void processJsonEid(List<Map<String, Object>> data, ETableRelation tableRelation){
        if (StringUtils.isNotEmpty(tableRelation.getJsonContEids())){
            String[] columns = tableRelation.getJsonContEids().split("\\|");
            for (String column : columns) {
                String[] cs = column.split(":");
                if (cs.length < 1) continue;
                //列名称
                String cName = cs[0];
                //JSON需要转换列名称
                String[] keys = cs[1].split(",");
                for (String key : keys) {
                    if (CollectionUtils.isNotEmpty(data)){
                        for (Map<String, Object> datum : data) {
                            if (ObjectUtils.isNotEmpty(datum.get(cName)) && StringUtils.isNotEmpty(datum.get(cName).toString())){
                                //解析eid
                                datum.put(cName, EIdUtils.generateEid(datum.get(cName).toString()));
                            }
                        }
                    }
                }
            }
        }
    }
}
