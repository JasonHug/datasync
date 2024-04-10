package com.cnso.flinkcdc.util;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Create by Zyy 2023-04-24 0024 17:52:02
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
            String realPkId = getRealPkId(datum, binlogData.getPkNames());

            tmp.setPkId(datum.get(binlogData.getPkNames().get(0)).toString());
            dataList.add(tmp);

            // 校验前后的pkID是否一致 不一致则删除
        }
    }

    private static String getRealPkId(Map<String, Object> datum, List<String> pkNameList) {
        String pkID = "";
        if (CollectionUtils.isNotEmpty(pkNameList)) {
            for (String s : pkNameList)
                return "";
        }
        return  "";
    }

}
