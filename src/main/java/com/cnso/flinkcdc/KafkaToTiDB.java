package com.cnso.flinkcdc;

import com.cnso.flinkcdc.deserialization.KafkaDeserialization;
import com.cnso.flinkcdc.sink.TiDBSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.FLINK_CHECKPOINT_DIR;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_BOOTSTRAP_SERVERS;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_GROUP_ID;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_PASSWORD;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_TOPICS;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_KAFKA_USERNAME;

public class KafkaToTiDB {

    private static final String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(3);

        ParameterTool config = ParameterTool.fromArgs(args);
        String cpDir = config.get(FLINK_CHECKPOINT_DIR);

        env.enableCheckpointing(120000L);
        env.setStateBackend(new HashMapStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(600000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(120000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(9999);
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(cpDir));

        String bootstrapServers = config.get(SOURCE_KAFKA_BOOTSTRAP_SERVERS);
        String topics = config.get(SOURCE_KAFKA_TOPICS);
        List<String> topicList = Arrays.stream(topics.split(",")).collect(Collectors.toList());
        String groupId = config.get(SOURCE_KAFKA_GROUP_ID);
        String user = config.get(SOURCE_KAFKA_USERNAME);
        String pwd = config.get(SOURCE_KAFKA_PASSWORD);
        String jaasConfig = String.format(saslJaasConfig, user, pwd);
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


        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-tidb");

        // kafkaSource.print();

        SingleOutputStreamOperator<Iterable<String>> toSinkStream = kafkaSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new AllWindowFunction<String, Iterable<String>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<Iterable<String>> collector) throws Exception {
                        collector.collect(iterable);
                    }
                });

        toSinkStream.addSink(new TiDBSink(config));

        env.execute();
    }

}
