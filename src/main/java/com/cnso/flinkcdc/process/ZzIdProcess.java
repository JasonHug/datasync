package com.cnso.flinkcdc.process;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.util.TableUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Create by zhengtianhao 2023-04-26 0026 10:05:26
 */
public class ZzIdProcess {

    public void processZzId(BinlogData binlogData, ETableRelation tableRelation){

        String tableSuffix = TableUtils.getSuffix(binlogData.getTable());
        String dbSuffix = TableUtils.getSuffix(binlogData.getDatabase());
        List<Map<String, Object>> dataList = binlogData.getData();
        String zzId = "";
        switch (tableRelation.getTableType()){
            //单表
            case 1:
                if (tableRelation.getNewTableName().equals(new String("t_report_details"))){
                    for (Map<String, Object> map : dataList) {
                        tableSuffix = StringUtils.leftPad(map.get("report_year").toString(), 4, "0");
                        zzId = tableSuffix +"_"+ StringUtils.leftPad(map.get("zz_eid").toString(), 32, "0");
                        putId(map,"zz_id", zzId);
                    }
                }
                break;
            case 2:
                tableSuffix = tableSuffix.startsWith("other") ? "9999" : tableSuffix;
                tableSuffix = StringUtils.leftPad(tableSuffix, 5, "0");
                for (Map<String, Object> map : dataList) {
                    zzId = tableSuffix +"_"+ StringUtils.leftPad(map.get(binlogData.getPkNames().get(0)).toString(), 20, "0");
                    putId(map,"zz_id", zzId);
                }
                break;
            case 3:
                dbSuffix = StringUtils.leftPad(dbSuffix, 2, "0");
                tableSuffix = StringUtils.leftPad(tableSuffix, 2, "0");
                //szse_eid_pname_pid/szse_pid_relation: 09_09_0000000d562640c9d1a8f5a5d4675e64, 库2位_表2位_32位id
                if (tableRelation.getNewTableName().equals(new String("szse_eid_pname_pid")) ||
                        tableRelation.getNewTableName().equals(new String("szse_pid_relation"))){
                    for (Map<String, Object> map : dataList) {
                        zzId = dbSuffix+"_"+tableSuffix+"_"+ StringUtils.leftPad(map.get("zz_eid").toString(), 32, "0");
                        putId(map,"zz_id", zzId);
                    }
                } else if (tableRelation.getNewTableName().equals(new String("t_article_enterprise")) ||
                        tableRelation.getNewTableName().equals(new String("t_article_enterprise_tags")) ||
                        tableRelation.getNewTableName().equals(new String("t_article_content")) ||
                        tableRelation.getNewTableName().equals(new String("t_article_mining"))) {
                    //t_article_enterprise/t_article_enterprise_tags/t_article_content/t_article_mining:2022_2022_00000000000000010100, 库4位_表4位_20位id
                    dbSuffix = StringUtils.leftPad(dbSuffix, 4, "0");
                    tableSuffix = StringUtils.leftPad(tableSuffix, 4, "0");
                    for (Map<String, Object> map : dataList) {
                        zzId = dbSuffix+"_"+tableSuffix+"_"+ StringUtils.leftPad(map.get(binlogData.getPkNames().get(0)).toString(), 20, "0");
                        putId(map,"zz_id", zzId);
                    }
                } else {
                    for (Map<String, Object> map : dataList) {
                        zzId = dbSuffix+"_"+tableSuffix+"_"+ StringUtils.leftPad(map.get(binlogData.getPkNames().get(0)).toString(), 20, "0");
                        putId(map,"zz_id", zzId);
                    }
                }
                break;
            default:
                throw new RuntimeException("[table type error] type:"+tableRelation.getTableType());
        }
    }

    private void putId(Map<String, Object> map, String key, String val){
        map.put(key, val);
    }

    public static void main(String[] args) {
        String data = "dfafe";
        System.out.println(StringUtils.leftPad(data, 2, "0"));
    }
}
