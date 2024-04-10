package com.cnso.flinkcdc.util;

import com.cnso.flinkcdc.model.ETableRelation;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ETableRelationUtils {

    public static Map<String, ETableRelation> RELATION_MAP = new ConcurrentHashMap<>();
    public static List<ETableRelation> RELATION_LIST = Collections.synchronizedList(new ArrayList<>());
    private static volatile boolean inited = false;

    public synchronized static void initList(List<ETableRelation> relations) {
        if(inited) return;
        inited = true;

        if (CollectionUtils.isEmpty(RELATION_LIST))
            RELATION_LIST.addAll(relations);
    }
}
