package com.cnso.flinkcdc.sink;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ProcessResult;
import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.TiDBUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Create by zhengtianhao 2023-04-23 0023 16:49:08
 */
public class SyncDataToTidbSink extends RichSinkFunction<List<ProcessResult>> {
    Logger log = LoggerFactory.getLogger(SyncDataToTidbSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("[open sink]");
    }

    @Override
    public void invoke(List<ProcessResult> results, Context context) throws Exception {
        Connection conn = TiDBUtil.getConn();
        try {
            if (CollectionUtils.isNotEmpty(results)) {
                for (ProcessResult result :
                        results) {
                    //清空临时表
                    BaseService service = result.getService();

                    //清空临时表
                    log.info("[clear tmp table] tmp_table={}_tmp", result.getTableName());
                    //service.clearInsertTmp(result.getTableName(), conn);

                    log.info("[start data in database] {}", service);
                    //数据删除
                    if (CollectionUtils.isNotEmpty(result.getDelList())) {
                        service.batchDel(result.getTableRelation(), result.getDelList(), conn);
                        log.info("[delete] finish");
                    }

                    updateByMemoryTable(result, service, conn);
                }
            }
        }finally {
            if(conn != null){
                conn.close();
            }
        }
    }

    /**
     * 使用内存表更新
     * @param result
     * @param service
     * @param conn
     */
    private void updateByMemoryTable(ProcessResult result, BaseService service, Connection conn) throws Exception{
        ArrayList<BinlogData> insertTmpList = new ArrayList<>();
        insertTmpList.addAll(result.getInsertList());
        insertTmpList.addAll(result.getUpdateList());
        List<List<BinlogData>> partition = Lists.partition(insertTmpList, 500);

        for (List<BinlogData> binlogData: partition){
            conn.setAutoCommit(false);
            service.batchInsertOrUpdate(result.getTableRelation(), binlogData, conn);

            conn.commit();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info("[close sink]");
    }
}
