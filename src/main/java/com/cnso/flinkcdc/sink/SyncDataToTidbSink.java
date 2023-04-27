package com.cnso.flinkcdc.sink;

import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ProcessResult;
import com.cnso.flinkcdc.service.BaseService;
import com.cnso.flinkcdc.util.TiDBUtil;
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
public class SyncDataToTidbSink extends RichSinkFunction<ProcessResult> {
    Logger log = LoggerFactory.getLogger(SyncDataToTidbSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("[open sink]");
    }

    @Override
    public void invoke(ProcessResult result, Context context) throws Exception {
        Connection conn = TiDBUtil.getConn();
        try {
            //清空临时表
            BaseService service = result.getService();
            //清空临时表
            log.info("[clear tmp table] tmp_table={}_tmp", result.getTableName());
            service.clearInsertTmp(result.getTableName(), conn);

            log.info("[start data in database]");
            //数据删除
            if (CollectionUtils.isNotEmpty(result.getDelList())) {
                service.batchDel(result.getTableRelation(), result.getDelList(), conn);
                log.info("[delete] finish");
            }
            //插入临时表
            List<BinlogData> insertTmpList = new ArrayList<>();
            insertTmpList.addAll(result.getInsertList());
            insertTmpList.addAll(result.getUpdateList());
            log.info("[insert tmp table] size={}", insertTmpList.size());
            log.info("[service] {}", service);
            if (CollectionUtils.isNotEmpty(insertTmpList)) {
                log.info("[insert data] start");
                service.batchInsertTmp(insertTmpList,conn);
                log.info("[insert data to tmp table] finish");
            }
            log.info("[data in database finish]");
            //更新临时表
            service.batchUpdateFromTmp(result.getTableName(),conn);
            log.info("[replace into table] finish");
        }finally {
            if(conn != null){
                conn.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info("[close sink]");
    }
}
