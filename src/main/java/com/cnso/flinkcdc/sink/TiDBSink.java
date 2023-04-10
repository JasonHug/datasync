package com.cnso.flinkcdc.sink;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.util.TiDBUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.IS_SEND_OFFSET;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SEND_OFFSET_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DATABASE;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DRIVER;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_USERNAME;

public class TiDBSink extends RichSinkFunction<Iterable<String>> {

    private final static Logger logger = LoggerFactory.getLogger(TiDBSink.class);

    private final static String DateParsePattern = "yyyy-MM-dd HH:mm:ss";

    // private final static ArrayList<String> dbPrefix = Lists.newArrayList("db_df_enterprise_", "db_business_reproduce_", "db_code_", "db_df_bidding_", "db_df_credit_", "db_df_danger_", "db_df_enterprise_reports_", "db_df_gspublic_", "db_df_institution_", "db_df_invest_", "db_df_ip_", "db_df_justice_", "db_df_management_", "db_df_miningdata_", "db_df_tax_", "db_df_trademark_", "db_df_trademark_other_", "db_df_yq_article_", "db_enterprise_index_", "db_qualification_certificate_", "db_df_lawsuits_", "db_szse_");

    private ParameterTool params;
    private String db;
    private Boolean isSend;
    private String sendOffsetUrl;

    public TiDBSink(ParameterTool params) {
        this.params = params;
        this.db = params.get(SINK_TIDB_DATABASE);
        this.isSend = params.getBoolean(IS_SEND_OFFSET);
        this.sendOffsetUrl = params.get(SEND_OFFSET_URL);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String driver = params.get(SINK_TIDB_DRIVER, "com.mysql.cj.jdbc.Driver");
        String url = params.get(SINK_TIDB_URL);
        String user = params.get(SINK_TIDB_USERNAME);
        String pwd = params.get(SINK_TIDB_PASSWORD);

        TiDBUtil.init(driver, url, user, pwd);
        logger.info("Connection pool init succeed!!!");
    }

    @Override
    public void close() throws Exception {
        TiDBUtil.close();
        super.close();
    }

    @Override
    public void invoke(Iterable<String> value, Context context) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        Map<String, Long> offsetMap = null;
        try {
            conn = TiDBUtil.getConn();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();

            int flag = 0;
            offsetMap = new HashMap<>(171);
            for (String line : value) {
                BinlogData binlogData = JSON.parseObject(line, BinlogData.class);
                List<Map<String, Object>> dataList = binlogData.getData();
                String topic = binlogData.getTopic();
                String tableName = getTableName(topic);
                if (isSend) {
                    Long offset = binlogData.getOffset();
                    offsetMap.compute(tableName, (k, v) -> v == null ? offset : Math.max(v, offset));
                }

                for (Map<String, Object> temp : dataList) {
                    BinlogData tempD = new BinlogData();
                    BeanUtil.copyProperties(binlogData, tempD);
                    List<Map<String, Object>> dt = new ArrayList<>();
                    dt.add(temp);
                    tempD.setData(dt);
                    String pk = tempD.getPkNames()
                            .stream()
                            .map(col -> String.valueOf(temp.get(col)))
                            .collect(Collectors.joining("|"));
                    String database = tempD.getDatabase();
                    String table = tempD.getTable();
                    String updateTime = temp.get("local_row_update_time").toString();
                    Long upTimeStamp = parsNanoTime(updateTime);
                    Long ts = tempD.getTs();
                    Map<String, Object> params = new HashMap<>(7);
                    byte[] encode = Base64.getEncoder().encode(JSON.toJSONString(tempD).getBytes(StandardCharsets.UTF_8));
                    params.put("msg_content", new String(encode));
                    params.put("exec_time", upTimeStamp);
                    params.put("pk_id", pk);
                    params.put("db_name", database);
                    params.put("table_name", table);
                    params.put("ts", ts);
                    String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    params.put("create_time", now);
                    params.put("update_time", now);

                    String execSql = buildExecSql(this.db, tableName, params);
                    stmt.addBatch(execSql);
                    flag += 1;
                }

                if (flag % 100 == 0) {
                    stmt.executeBatch();
                    conn.commit();
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            stmt.executeBatch();
            conn.commit();

            if (stmt != null) {
                stmt.close();
            }

            if (conn != null) {
                conn.close();
            }

            if (isSend) {
                sendOffset(offsetMap, sendOffsetUrl);
            }
        }
    }

    private String buildExecSql(String db, String table, Map<String, Object> params) {
        Set<String> cols = params.keySet();

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(db)
                .append(".")
                .append(table)
                .append(" (");
        cols.forEach(col -> sqlBuilder.append(col).append(", "));
        sqlBuilder.delete(sqlBuilder.length() -2, sqlBuilder.length());
        sqlBuilder.append(") VALUES(");
        cols.forEach(col -> {
            if (("exec_time").equals(col) || ("ts").equals(col)) {
                sqlBuilder.append(params.get(col)).append(", ");
            } else {
                sqlBuilder.append("'").append(params.get(col)).append("', ");
            }
        });
        sqlBuilder.delete(sqlBuilder.length() -2, sqlBuilder.length());
        sqlBuilder.append(")");


        return sqlBuilder.toString();
    }

    private Long parsNanoTime(String dateStr){
        Long l = 0l;
        if (StringUtils.isNotEmpty(dateStr)){
            String[] split = dateStr.split("\\.");
            if (split.length ==2){
                try {
                    Date date = DateUtils.parseDate(split[0], DateParsePattern);
                    l = date.getTime()/1000;
                    double pow = Math.pow(10, split[1].length());
                    l = l * new Double(pow).longValue();
                    l = l + new Long(split[1]).longValue();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        return l;
    }

    private String getTableName(String topic) {

        String tableName = topic;

        String[] tmp = topic.split("_t_");
        if (tmp.length == 2) {
            tableName = "t_" + tmp[1];
        } else {
            tableName = topic.split("db_szse_")[1];
        }

        return tableName;
    }

    private void sendOffset(Map<String, Long> offsetMap, String url) {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("offsetMap", offsetMap);
                    HttpUtil.post(url, paramMap);
                } catch (Exception e) {
                    logger.warn("send offset failed", e);
                }
            }
        };

        executor.execute(run);
    }
}
