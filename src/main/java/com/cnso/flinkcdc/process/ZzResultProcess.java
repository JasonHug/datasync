package com.cnso.flinkcdc.process;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.factory.ServiceFactory;
import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.model.ProcessResult;
import com.cnso.flinkcdc.service.ETableRelationService;
import com.cnso.flinkcdc.util.BinlogDataUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create by Zyy 2023-04-23 0023 16:14:29
 */
public class ZzResultProcess extends ProcessWindowFunction<String, List<ProcessResult>, String, TimeWindow> {

    Logger logger = LoggerFactory.getLogger(ZzResultProcess.class);

    @Override
    public void process(String key, Context context, Iterable<String> iterable, Collector<List<ProcessResult>> collector) throws Exception{
        logger.info("[start] tableInfo={}",key);
        List<BinlogData> dataList = new ArrayList<>();
        List<BinlogData> filterList = new ArrayList<>();

        //获取表关系
        ETableRelation currRelation = ETableRelationService.getCurrRelation(key);
        if (null == currRelation) throw new RuntimeException("[Not Found Relation] key: " + key);
        logger.info("[table relation] Relation={}", JSON.toJSONString(currRelation));

        //解析binlog数据
        for (String tmp : iterable) {
            BinlogDataUtils.rebuild(tmp, dataList);
        }
        logger.info("[data] binlog data size = {}", dataList.size());
        //数据数据库+表+主键分组
        Map<String, List<BinlogData>> fiMap = dataList.stream()
                .collect(Collectors
                        .groupingBy(item -> {
                                    return item.getDatabase() + item.getTable() + item.getPkId();
                                },
                                Collectors.collectingAndThen(Collectors.toList(), value -> value)));
        //解析EID
        ZzEidProcess zzEidProcess = new ZzEidProcess();
        zzEidProcess.processEid(currRelation, fiMap);

        for (Map.Entry<String, List<BinlogData>> entry : fiMap.entrySet()) {
            BinlogDataFilterProcess filterProcess = new BinlogDataFilterProcess();
            filterList.add(filterProcess.process(entry.getValue()));
        }

        logger.info("[data] binlog data filter size size ={}", filterList.size());
        ArrayList<ProcessResult> processResList = new ArrayList<>();
        ProcessResult processResult = splitTask(filterList);

        processResult.setService(ServiceFactory.createService(currRelation));
        processResult.setTableName(currRelation.getNewTableName());
        processResult.setTableRelation(currRelation);
        processResList.add(processResult);

        // 复制条目数
        if(currRelation.getNewTableName().equals("t_enterprise")) {
            ProcessResult pr = new ProcessResult();
            ETableRelation er = new ETableRelation();
            BeanUtil.copyProperties(processResult, pr, false);
            BeanUtil.copyProperties(currRelation, er, false);

            er.setNewTableName("m_enterprise");
            pr.setTableName(er.getNewTableName());
            pr.setService(ServiceFactory.createService(er));
            pr.setTableRelation(er);
            processResList.add(pr);
        }

        logger.info("[res] {}", processResult);
        collector.collect(processResList);
    }

    private ProcessResult splitTask(List<BinlogData> dataList){
        ProcessResult batchData = new ProcessResult();

        List<BinlogData> insertList = new ArrayList<>();

        List<BinlogData> updateList = new ArrayList<>();

        List<BinlogData> delList = new ArrayList<>();

        for (BinlogData binlogData : dataList) {
            switch (binlogData.getType().toLowerCase(Locale.ROOT)){
                case "insert":
                    insertList.add(binlogData);
                    break;
                case "update":
                    updateList.add(binlogData);
                    break;
                case "delete":
                    delList.add(binlogData);
                    break;
                default:
                    logger.info("[DML TYPE ERROR], not found DML type,data={}", JSON.toJSONString(binlogData));
            }
        }
        batchData.setInsertList(insertList);
        batchData.setDelList(delList);
        batchData.setUpdateList(updateList);
        logger.info("[process data] insertSize={}, updateSize={}, delSize={}", insertList.size(), updateList.size(), delList.size());
        return batchData;
    }

}
