package com.cnso.flinkcdc.util;

import com.cnso.flinkcdc.model.BinlogData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @author jia.xd
 * @date 2023/2/14
 */
public class TableUtils {

    public static final int BATCH_SIZE = 20000;

    //只处理以下DML类型数据
    private final static String dmlTypeStr = "insert,update,delete";

    public static String getTableYear(String tableName){
        String[] split = tableName.split("_");
        String dateYear = null;
        if (split.length > 0){
            dateYear = split[split.length -1];
        }
        return dateYear;
    }

    public static String getSuffix(String tableName){
        String[] split = tableName.split("_");
        String suffix = null;
        if (split.length > 0){
            suffix = split[split.length -1];
        }
        return suffix;
    }

    private static int getLevenshteinDistance(String X, String Y)
    {
        int m = X.length();
        int n = Y.length();

        int[][] T = new int[m + 1][n + 1];
        for (int i = 1; i <= m; i++) {
            T[i][0] = i;
        }
        for (int j = 1; j <= n; j++) {
            T[0][j] = j;
        }

        int cost;
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                cost = X.charAt(i - 1) == Y.charAt(j - 1) ? 0: 1;
                T[i][j] = Integer.min(Integer.min(T[i - 1][j] + 1, T[i][j - 1] + 1),
                        T[i - 1][j - 1] + cost);
            }
        }

        return T[m][n];
    }

    public static double findNameSimilarity(String x, String y) {
        if (x == null || y == null) {
            throw new IllegalArgumentException("Strings must not be null");
        }

        double maxLength = Double.max(x.length(), y.length());
        if (maxLength > 0) {
            // 如果需要，可以选择忽略大小写
            return (maxLength - getLevenshteinDistance(x, y)) / maxLength;
        }
        return 1.0;
    }

    /**
     * 转换纳秒时间
     * @param dateStr
     * @return
     */
    public static Long parsNanoTime(String dateStr){
        Long l = 0l;
        if (StringUtils.isNotEmpty(dateStr)){
            String[] split = dateStr.split("\\.");
            if (split.length ==2){
                try {
                    Date date = DateUtils.parseDate(split[0], "yyyy-MM-dd HH:mm:ss");
                    l = date.getTime()/1000;
                    String non = split[1].length() > 3 ? split[1].substring(0, 3) : split[1];
                    double pow = Math.pow(10, non.length());
                    l = l * new Double(pow).longValue();
                    l = l + new Long(non).longValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        return l;
    }

    /**
     * 获取毫秒
     * @param dateStr
     * @return
     */
    public static Long getLong(String dateStr){
        Long l = 0l;
        if (StringUtils.isNotEmpty(dateStr)){
            try {
                Date date = DateUtils.parseDate(dateStr, "yyyy-MM-dd HH:mm:ss");
                l = date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return l;
    }

    public static Long getDateLong(String dateStr){
        Long l = 0l;
        if (StringUtils.isNotEmpty(dateStr)){
            try {
                Date date = DateUtils.parseDate(dateStr, "yyyy-MM-dd");
                l = date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return l;
    }

    /**
     * 校验DML类型
     * @param binlogData
     * @return
     */
    public static boolean checkDmlType(BinlogData binlogData){
        boolean isContinue = false;
        if (ObjectUtils.isNotEmpty(binlogData) && StringUtils.isNotEmpty(binlogData.getType())){
            int i = dmlTypeStr.indexOf(binlogData.getType().toLowerCase(Locale.ROOT));
            isContinue = i > -1;
        }
        return isContinue;
    }

    /**
     * 处理时间
     * @param binlogData
     */
    public static void processTime(BinlogData binlogData){
        List<Map<String, Object>> data = binlogData.getData();
        if (CollectionUtils.isNotEmpty(data)){
            for (Map<String, Object> itemMap : data) {
                for (Map.Entry<String, Object> entry : itemMap.entrySet()) {
                    if (entry.getKey().equals("local_row_update_time")){
                        String s = entry.getValue().toString();
                        if (StringUtils.isNotEmpty(s)){
                            entry.setValue(s.split("\\.").length > 0 ? s.split("\\.")[0] : s);
                        }
                    }
                }
            }
        }
    }
}
