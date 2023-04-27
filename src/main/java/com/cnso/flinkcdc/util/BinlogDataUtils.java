package com.cnso.flinkcdc.util;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;

import java.util.List;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-24 0024 17:52:02
 */
public class BinlogDataUtils {

    public static void rebuild(String str, List<BinlogData> dataList){
        BinlogData binlogData = JSON.parseObject(str, BinlogData.class);
        List<Map<String, Object>> data = binlogData.getData();
        for (Map<String, Object> datum : data) {
            BinlogData tmp = new BinlogData();
            BeanUtil.copyProperties(binlogData, tmp);
            tmp.getData().clear();
            tmp.getData().add(datum);
            tmp.setPkId(datum.get(binlogData.getPkNames().get(0)).toString());
            dataList.add(tmp);
        }
    }

}
