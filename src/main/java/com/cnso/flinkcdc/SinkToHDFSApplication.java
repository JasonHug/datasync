package com.cnso.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.cnso.flinkcdc.util.Utils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

import static com.cnso.flinkcdc.common.constant.ParameterConstant.FLINK_CHECKPOINT_DIR;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.OUTPUT_PATH;
import static com.cnso.flinkcdc.common.constant.ParameterConstant.SOURCE_MYSQL_HOST;

public class SinkToHDFSApplication {

    public static void main(String[] args) throws Exception {
        ParameterTool config = ParameterTool.fromArgs(args);
        String cpDir = config.get(FLINK_CHECKPOINT_DIR);
        String outputPath = config.get(OUTPUT_PATH);
        String mysqlHost = config.get(SOURCE_MYSQL_HOST);
        String subPath = StringUtils.substringAfterLast(mysqlHost, ".");
        String outPath = String.join("/", outputPath, subPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint 相关配置
        env.enableCheckpointing(600000L);
        env.setStateBackend(new HashMapStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(600000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(9999);
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(cpDir));

        MySqlSource<JSONObject> sourceFunction = Utils.getMysqlSourceBuilder(config);

        DataStreamSource<JSONObject> mysqlSource = env.fromSource(sourceFunction, WatermarkStrategy.forMonotonousTimestamps(), "MysqlSource");

        final StreamingFileSink<JSONObject> sink = StreamingFileSink
                .forRowFormat(new Path(outPath), new SimpleStringEncoder<JSONObject>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.HOURS.toMillis(1))  // 至少包含1小时的数据
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(30)) // 最近30分钟没有收到新的记录
                                .withMaxPartSize(1024 * 1024 * 1024) // 文件大小达到1G（写入最后一条记录后）
                                .build())
                .build();

        mysqlSource.addSink(sink);

        env.execute();
    }

}
