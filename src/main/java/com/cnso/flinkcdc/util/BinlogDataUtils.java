package com.cnso.flinkcdc.util;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;
import org.apache.avro.reflect.MapEntry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
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

            tmp.setPkId(realPkId);
            dataList.add(tmp);

            // 校验前后的pkID是否一致 不一致则删除
            List<Map<String, Object>> old = tmp.getOld();
            if (null != old && old.size() > 0) {
                for (Map<String, Object> map: old) {
                    HashMap<String, Object> oldMap = new HashMap<>();
                    if (null != map) {
                        BeanUtil.copyProperties(datum, oldMap);
                        for (Map.Entry<String, Object> entry :
                                map.entrySet()) {
                            oldMap.put(entry.getKey(), entry.getValue());

                        }
                    }

                    String oldPkID = getRealPkId(oldMap, binlogData.getPkNames());
                    if (null != binlogData.getPkNames()
                    && StringUtils.isNotEmpty(oldPkID) && !realPkId.equals(oldPkID)){
                        BinlogData del = new BinlogData();
                        BeanUtil.copyProperties(binlogData, del);
                        del.getData().clear();
                        del.getData().add(oldMap);
                        del.setType("DELETE");
                        del.setPkId(oldPkID);
                        dataList.add(del);
                    }
                }
            }
        }
    }

    private static String getRealPkId(Map<String, Object> datum, List<String> pkNameList) {
        String pkID = "";
        if (CollectionUtils.isNotEmpty(pkNameList)) {
            for (String s : pkNameList)
                pkID = pkID + datum.get(s).toString();
        }
        return pkID;
    }

}
