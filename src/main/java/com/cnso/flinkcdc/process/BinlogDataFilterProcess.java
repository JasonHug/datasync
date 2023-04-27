package com.cnso.flinkcdc.process;

import com.cnso.flinkcdc.model.BinlogData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Create by zhengtianhao 2023-04-23 0023 20:12:26
 */
public class BinlogDataFilterProcess {

    Logger log = LoggerFactory.getLogger(BinlogDataFilterProcess.class);

    public BinlogData process(List<BinlogData> dataList){

        if (CollectionUtils.isEmpty(dataList)) return null;

        //根据ts排序
        dataList.sort(Comparator.comparing(BinlogData::getTs));

        BinlogData taskData = dataList.get(0);
        String firstType = taskData.getType().toUpperCase(); // 初始任务动作
        String realType = firstType; // 要执行的动作

        if(dataList.size() > 1){
            boolean exists = false;
            for (int i = dataList.size() - 1; i > 0; i--) {
                exists = true;
                BinlogData lastData = dataList.get(i);
                String thisType = lastData.getType().toUpperCase();
                switch (thisType){
                    case "DELETE":
                        taskData = lastData; // 数据存在不存在，都执行一次删除
                        realType = "DELETE";
                        break;
                    case "UPDATE":
                    case "INSERT":
                        switch (firstType){
                            case "INSERT":
                                taskData = lastData; // 新数据插入动作
                                realType = "INSERT";
                                break;
                            case "DELETE":
                            case "UPDATE":
                                taskData = lastData; // 此时数据存在，更新数据
                                realType = "UPDATE";
                                break;
                            default:
                                taskData = lastData;
                                realType = taskData.getType();
                                log.error("不支持的数据DML类型，{}, 表：{}, id: {}",firstType,taskData.getTable(),taskData.getPkId());
                                break;
                        }
                        break;
                    default:
                        log.error("不支持的数据DML类型，{}, 表：{}, id: {}",thisType,lastData.getTable(),lastData.getPkId());
                        exists = false;
                        break;
                }
                if(exists){
                    break;
                }

            }
        }
        taskData.setType(realType);
        return taskData;
    }

}
