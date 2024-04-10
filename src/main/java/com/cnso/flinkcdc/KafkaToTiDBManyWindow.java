package com.cnso.flinkcdc;

import com.alibaba.fastjson.JSON;
import com.cnso.flinkcdc.common.constant.ParameterConstant;
import com.cnso.flinkcdc.deserialization.KafkaDeserialization;
import com.cnso.flinkcdc.model.BinlogData;
import com.cnso.flinkcdc.model.ETableRelation;
import com.cnso.flinkcdc.process.ZzResultProcess;
import com.cnso.flinkcdc.service.ETableRelationService;
import com.cnso.flinkcdc.sink.SyncDataToTidbSink;
import com.cnso.flinkcdc.util.TiDBUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.FLINK_CHECKPOINT_DIR;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_DRIVER;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_URL;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SINK_TIDB_USERNAME;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_BOOTSTRAP_SERVERS;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_GROUP_ID;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_TOPICS;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_USERNAME;

public class KafkaToTiDBManyWindow {

    private final static Logger logger = LoggerFactory.getLogger(KafkaToTiDBManyWindow.class);

    private static final String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool config = ParameterTool.fromArgs(args);
        String cpDir = config.get(FLINK_CHECKPOINT_DIR);

        // env.setParallelism(3);


        //窗口的时间大小
        String windowTimeSeconds = config.get(ParameterConstant.WINDOW_TIME_SECONDS);
        //默认为3秒
        Long times = new Long(3);
        if (StringUtils.isNoneEmpty(windowTimeSeconds)){
            times = new Long(windowTimeSeconds);
        }

        env.enableCheckpointing(120000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.disableOperatorChaining();

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(600000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(120000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(9999);
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(cpDir));

        // kafka的配置
        String bootstrapServers = config.get(SOURCE_KAFKA_BOOTSTRAP_SERVERS);
        String topics = config.get(SOURCE_KAFKA_TOPICS);
        List<String> topicList = Arrays.stream(topics.split(",")).collect(Collectors.toList());
        String groupId = config.get(SOURCE_KAFKA_GROUP_ID);
        String user = config.get(SOURCE_KAFKA_USERNAME);
        String pwd = config.get(SOURCE_KAFKA_PASSWORD);
        String jaasConfig = String.format(saslJaasConfig, user, pwd);
        String filterTopics = config.get(ParameterConstant.FILTER_KAFKA_TOPICS);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topicList)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // .setStartingOffsets(OffsetsInitializer.timestamp(1677240000000L))
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserialization()))
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "SCRAM-SHA-256")
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
                .build();

        //初始化tidb连接
        final String tidbDriver = config.get(SINK_TIDB_DRIVER, "com.mysql.cj.jdbc.Driver").toString();
        final String tidbUrl = config.get(SINK_TIDB_URL);
        final String tidbUser = config.get(SINK_TIDB_USERNAME);
        final String tidbPwd = config.get(SINK_TIDB_PASSWORD);

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-tidb");

        kafkaSource.map(new RichMapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return s;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                logger.info("[connection tidb] tidbDriver={}, tidbUrl={}, tidbUser={}", tidbDriver, tidbUrl, tidbUser);
                TiDBUtil.init(tidbDriver, tidbUrl, tidbUser, tidbPwd);
            }

            @Override
            public void close() throws Exception {
                logger.info("[close tidb]");
            }
        }).filter(new RichFilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                String topic = JSON.parseObject(s, BinlogData.class).getTopic();
                if(StringUtils.isNotEmpty(filterTopics)){
                    List<String> collect = Arrays.stream(filterTopics.split(",")).collect(Collectors.toList());
                    if (null != collect)
                        return !collect.contains(topic);
                }
                return true;
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) {
                String groupByKey = null;
                try{
                    BinlogData binlogData = JSON.parseObject(s, BinlogData.class);
                    // 库名&表名 库名&表名_1 库名_1&表名_1 作为主键
                    String key = binlogData.getDatabase() + "&" + binlogData.getTable();
                    ETableRelation currRelation = ETableRelationService.getCurrRelation(key);
                    if (null != currRelation) {
                        groupByKey = currRelation.getDatabaseName()+"&"+currRelation.getNewTableName();
                    }
                } catch (Exception e) {
                    logger.error("[Group By Key Error] msg:{}", s, e);
                }
                return groupByKey;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(times)))
                .process(new ZzResultProcess())
                .addSink(new SyncDataToTidbSink());
        env.execute();
    }

}
